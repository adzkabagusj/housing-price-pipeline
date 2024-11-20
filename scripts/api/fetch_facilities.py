import requests
import json
import time
import logging
import pandas as pd
from hdfs import InsecureClient
from typing import Optional, Dict, Set
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# Configure logging properly
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('osm_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OSMFacilitiesFetcher:
    def __init__(self):
        self.hdfs_url = "http://namenode:9870"
        self.hdfs_client = InsecureClient(self.hdfs_url, user="hadoop")
        self.overpass_url = "http://overpass-api.de/api/interpreter"
        
        # Define facility queries with more specific tags
        self.facility_queries = {
            "jumlah_fasilitas_pendidikan": [
                'amenity~"school|university|college|kindergarten"'
            ],
            "jumlah_fasilitas_kesehatan": [
                'amenity~"hospital|clinic|doctors|dentist|pharmacy"'
            ],
            "jumlah_fasilitas_perbelanjaan": [
                'shop~"supermarket|mall|department_store|convenience"',
                'amenity~"marketplace|shopping_mall"'
            ],
            "jumlah_fasilitas_transportasi": [
                'amenity~"bus_station|taxi|ferry_terminal"',
                'aeroway~"aerodrome|terminal"',
                'railway~"station|halt"'
            ],
            "jumlah_fasilitas_rekreasi": [
                'leisure~"park|sports_centre|fitness_centre|swimming_pool"',
                'amenity~"park|theatre|cinema"'
            ]
        }

        # Setup session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def build_query(self, kecamatan: str, facility_filter: str) -> str:
        """
        Build Overpass API query for a specific kecamatan and facility type.
        Uses area-based search within administrative boundaries.
        """
        # Note: We need to search within Indonesia to avoid wrong matches
        query = f"""
        [out:json][timeout:60];
        area["name"="Indonesia"]->.country;
        area["admin_level"="6"]["name"="{kecamatan}"](area.country)->.searchArea;
        (
          nwr[{facility_filter}](area.searchArea);
        );
        out count;
        """
        return query.strip()

    def get_facilities_count(self, kecamatan: str, max_retries: int = 3) -> Optional[Dict]:
        """
        Fetch facility counts for a given kecamatan with improved error handling
        and rate limiting.
        """
        results = {}
        retry_count = 0
        
        for category, facility_filters in self.facility_queries.items():
            total_count = 0
            
            for facility_filter in facility_filters:
                query = self.build_query(kecamatan, facility_filter)
                
                while retry_count < max_retries:
                    try:
                        # Add delay to respect rate limits
                        time.sleep(2)
                        
                        response = self.session.post(
                            self.overpass_url,
                            data=query,
                            timeout=60,
                            headers={'Content-Type': 'application/x-www-form-urlencoded'}
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            if 'elements' in data:
                                count = len(data['elements'])
                                total_count += count
                            break
                        elif response.status_code == 429:  # Too Many Requests
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited. Waiting {wait_time} seconds")
                            time.sleep(wait_time)
                            retry_count += 1
                        else:
                            logger.error(f"API error {response.status_code}: {response.text}")
                            retry_count += 1
                            time.sleep(5)
                    
                    except Exception as e:
                        logger.error(f"Exception during API call for {kecamatan}: {e}")
                        retry_count += 1
                        time.sleep(5)
                        
                if retry_count >= max_retries:
                    logger.error(f"Max retries reached for {kecamatan}")
                    return None
                    
            results[category] = total_count
            
        # Add metadata
        results.update({
            "kecamatan": kecamatan,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        })
        
        return results

    def get_current_kecamatans(self, progress: Dict, raw_data_path: str) -> Set[str]:
        """
        Extract unique kecamatan names from the current pagination page.
        """
        current_page = progress["current_page"]
        kecamatans = set()

        for province, page in progress["provinces"].items():
            if page != current_page:
                continue
                
            try:
                province_path = os.path.join(raw_data_path, province)
                if not self.hdfs_client.status(province_path, strict=False):
                    continue
                    
                files = self.hdfs_client.list(province_path)
                target_file = f"page_{current_page}_{province}.csv"
                
                if target_file in files:
                    with self.hdfs_client.read(os.path.join(province_path, target_file)) as reader:
                        df = pd.read_csv(reader)
                        valid_kecamatans = df["kecamatan"].dropna().unique()
                        kecamatans.update(valid_kecamatans)
                        
            except Exception as e:
                logger.error(f"Error reading kecamatans for {province}: {e}")

        return kecamatans

    def save_facilities_to_hdfs(self, facilities: Dict, hdfs_path: str) -> bool:
        """
        Save facilities data to HDFS with error handling.
        """
        try:
            kecamatan = facilities["kecamatan"]
            df = pd.DataFrame([facilities])
            
            # Ensure directory exists
            if not self.hdfs_client.status(hdfs_path, strict=False):
                self.hdfs_client.makedirs(hdfs_path)
            
            csv_path = os.path.join(hdfs_path, f"{kecamatan}.csv")
            with self.hdfs_client.write(csv_path, overwrite=True) as writer:
                df.to_csv(writer, index=False)
            
            logger.info(f"Successfully saved facilities data for {kecamatan}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save facilities data: {e}")
            return False

    def run(self, progress_file: str, raw_data_path: str, facilities_path: str):
        """
        Main execution flow with improved error handling and logging.
        """
        try:
            with self.hdfs_client.read(progress_file) as reader:
                progress = json.load(reader)
        except Exception as e:
            logger.error(f"Failed to load progress file: {e}")
            return

        kecamatans = self.get_current_kecamatans(progress, raw_data_path)
        logger.info(f"Found {len(kecamatans)} unique kecamatans on page {progress['current_page']}")

        for kecamatan in kecamatans:
            # Skip if facilities already exist
            if self.hdfs_client.status(os.path.join(facilities_path, f"{kecamatan}.csv"), strict=False):
                logger.info(f"Facilities for {kecamatan} already exist, skipping")
                continue

            logger.info(f"Fetching facilities for {kecamatan}")
            facilities = self.get_facilities_count(kecamatan)
            
            if facilities:
                self.save_facilities_to_hdfs(facilities, facilities_path)
            else:
                logger.error(f"Failed to fetch facilities for {kecamatan}")

if __name__ == "__main__":
    fetcher = OSMFacilitiesFetcher()
    fetcher.run(
        progress_file="/progress.json",
        raw_data_path="/data/raw/rumah123",
        facilities_path="/data/raw/osm_facilities"
    )