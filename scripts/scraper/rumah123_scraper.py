import requests
from bs4 import BeautifulSoup
import pandas as pd
from hdfs import InsecureClient
from datetime import datetime
import json
import re
import os
from typing import Dict, List, Set, Optional
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Rumah123Scraper:
    def __init__(self):
        self.provinces = [
            "dki-jakarta", "jawa-barat", "banten", "jawa-timur", "jawa-tengah",
            "bali", "daerah-istimewa-yogyakarta", "sumatera-utara", "kepulauan-riau",
            "sulawesi-selatan", "kalimantan-timur", "riau", "lampung", "sumatera-selatan",
            "kalimantan-barat", "sulawesi-utara", "nusa-tenggara-barat", "nusa-tenggara-timur",
            "sumatera-barat", "kalimantan-selatan", "jambi", "kepulauan-bangka-belitung",
            "kalimantan-tengah", "papua", "aceh", "bengkulu", "papua-barat", "sulawesi-tengah",
            "sulawesi-tenggara", "gorontalo", "kalimantan-utara", "maluku-utara",
            "sulawesi-barat", "maluku"
        ]
        
        # HDFS configuration
        self.hdfs_url = "http://namenode:9870"
        self.hdfs_raw_path = "/data/raw/rumah123/"
        self.hdfs_client = InsecureClient(self.hdfs_url, user="hadoop")
        self.progress_file = "/progress.json"
        
        # Setup requests session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def clean_price(self, price_text: Optional[str]) -> Optional[int]:
        """Convert price text to integer, handling edge cases."""
        if not price_text:
            return None
        try:
            # Remove non-numeric characters and convert to integer
            clean_text = re.sub(r"[^\d]", "", price_text)
            return int(clean_text) if clean_text else None
        except ValueError:
            logger.warning(f"Could not parse price: {price_text}")
            return None

    def fetch_existing_titles(self) -> Set[str]:
        """Fetch existing titles from HDFS with error handling."""
        try:
            titles = set()
            for province in self.provinces:
                province_path = os.path.join(self.hdfs_raw_path, province)
                if not self.hdfs_client.status(province_path, strict=False):
                    continue
                
                file_list = self.hdfs_client.list(province_path)
                for file in file_list:
                    try:
                        with self.hdfs_client.read(os.path.join(province_path, file)) as reader:
                            df = pd.read_csv(reader)
                            titles.update(df["judul_iklan"].dropna().tolist())
                    except Exception as e:
                        logger.error(f"Error reading file {file}: {e}")
            return titles
        except Exception as e:
            logger.error(f"Error fetching existing titles: {e}")
            return set()

    def scrape_search_page(self, url: str, existing_titles: Set[str]) -> List[Dict]:
        """Scrape house listings from search page with improved error handling."""
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            house_cards = soup.select("div.ui-organism-intersection__element")
            
            if not house_cards:
                logger.warning(f"No house cards found on {url}")
                return []

            houses_to_scrape = []
            for card in house_cards:
                try:
                    title_elem = card.select_one("a[href^='/properti/'] h2")
                    link_elem = card.select_one("a[href^='/properti/']")
                    
                    if not title_elem or not link_elem:
                        continue
                        
                    title = title_elem.text.strip()
                    link = link_elem["href"]
                    
                    if title and title not in existing_titles:
                        houses_to_scrape.append({
                            "judul_iklan": title,
                            "link": f"https://www.rumah123.com{link}"
                        })
                except Exception as e:
                    logger.error(f"Error parsing house card: {e}")
                    continue
                    
            return houses_to_scrape
            
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return []

    def scrape_house_details(self, url: str, province: str) -> Dict:
        """Scrape detailed information about a house with improved parsing."""
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            def get_value(selector: str, index: int = 0, attr: Optional[str] = None) -> Optional[str]:
                try:
                    elements = soup.select(selector)
                    if not elements or index >= len(elements):
                        return None
                    element = elements[index]
                    return element[attr] if attr else element.text.strip()
                except (IndexError, TypeError, AttributeError) as e:
                    logger.debug(f"Error getting value for selector {selector}: {e}")
                    return None

            # Extract location details
            location_text = get_value("p.text-xs.text-gray-500.mb-2")
            location_parts = location_text.split(",") if location_text else []
            kecamatan = location_parts[0].strip() if location_parts else None
            kabupaten = location_parts[-1].strip() if len(location_parts) > 1 else None

            # Extract agent and update date
            update_info = get_value("p.text-3xs.text-gray-400.mb-4")
            if update_info:
                update_parts = update_info.split("oleh")
                update_date = update_parts[0].split(" ")[1] if len(update_parts) > 0 else None
                agent = update_parts[1].strip() if len(update_parts) > 1 else None
            else:
                update_date = None
                agent = None

            return {
                "judul_iklan": get_value("h1"),
                "harga": self.clean_price(get_value("span.text-primary.font-bold")),
                "kecamatan": kecamatan,
                "kabupaten_kota": kabupaten,
                "provinsi": province,
                "terakhir_diperbarui": update_date,
                "agen": agent,
                "link_rumah123": url,
                "kamar_tidur": get_value("p:contains('Kamar Tidur') + p"),
                "kamar_mandi": get_value("p:contains('Kamar Mandi') + p"),
                "luas_tanah": get_value("p:contains('Luas Tanah') + p"),
                "luas_bangunan": get_value("p:contains('Luas Bangunan') + p"),
                "carport": get_value("p:contains('Carport') + p"),
                "sertifikat": get_value("p:contains('Sertifikat') + p"),
                "daya_listrik": get_value("p:contains('Daya Listrik') + p"),
                "kamar_tidur_pembantu": get_value("p:contains('Kamar Tidur Pembantu') + p"),
                "kamar_mandi_pembantu": get_value("p:contains('Kamar Mandi Pembantu') + p"),
                "dapur": get_value("p:contains('Dapur') + p"),
                "ruang_makan": get_value("p:contains('Ruang Makan') + p"),
                "ruang_tamu": get_value("p:contains('Ruang Tamu') + p"),
                "kondisi_perabotan": get_value("p:contains('Kondisi Perabotan') + p"),
                "material_bangunan": get_value("p:contains('Material Bangunan') + p"),
                "material_lantai": get_value("p:contains('Material Lantai') + p"),
                "garasi": get_value("p:contains('Garasi') + p"),
                "jumlah_lantai": get_value("p:contains('Jumlah Lantai') + p"),
                "konsep_dan_gaya_rumah": get_value("p:contains('Konsep dan Gaya Rumah') + p"),
                "pemandangan": get_value("p:contains('Pemandangan') + p"),
                "terjangkau_internet": get_value("p:contains('Terjangkau Internet') + p"),
                "lebar_jalan": get_value("p:contains('Lebar Jalan') + p"),
                "tahun_dibangun": get_value("p:contains('Tahun Dibangun') + p"),
                "tahun_direnovasi": get_value("p:contains('Tahun Direnovasi') + p"),
                "sumber_air": get_value("p:contains('Sumber Air') + p"),
                "hook": get_value("p:contains('Hook') + p"),
                "kondisi_properti": get_value("p:contains('Kondisi Properti') + p"),
                "waktu_scraping": datetime.now().isoformat()
            }
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return {}

    def store_to_hdfs(self, data: List[Dict], province: str, page: int) -> bool:
        """Store scraped house data to HDFS with proper error handling."""
        if not data:
            logger.warning(f"No data to store for {province} page {page}")
            return False

        try:
            # Convert data to DataFrame
            df = pd.DataFrame(data)
            
            # Create province directory if it doesn't exist
            province_path = os.path.join(self.hdfs_raw_path, province)
            if not self.hdfs_client.status(province_path, strict=False):
                self.hdfs_client.makedirs(province_path)

            # Store the data
            filename = f"page_{page}_{province}.csv"
            hdfs_path = os.path.join(province_path, filename)
            
            with self.hdfs_client.write(hdfs_path, overwrite=True) as writer:
                df.to_csv(writer, index=False)
            
            logger.info(f"Successfully stored {len(data)} records for {province} page {page}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store {province} page {page} data: {e}")
            return False

    def load_progress(self) -> Dict:
        """Load scraping progress with initialization handling."""
        try:
            with self.hdfs_client.read(self.progress_file) as reader:
                return json.load(reader)
        except Exception as e:
            logger.error(f"Failed to load progress file: {e}")
            raise

    def run(self):
        """Main scraping process with improved flow control."""
        progress = self.load_progress()
        existing_titles = self.fetch_existing_titles()
        current_page = progress["current_page"]

        for province, page in progress["provinces"].items():
            if page != current_page:
                continue
                
            url = f"https://www.rumah123.com/jual/{province}/rumah/?page={page}"
            logger.info(f"Processing {province}, page {page}")
            
            # Scrape search page
            houses = self.scrape_search_page(url, existing_titles)
            if not houses:
                logger.info(f"No new houses found for {province} page {page}, moving to next province")
                continue

            # Scrape details for each house
            house_details = []
            for house in houses:
                details = self.scrape_house_details(house["link"], province)
                if details:
                    house_details.append(details)

            # Store the data if we have any
            if house_details:
                if self.store_to_hdfs(house_details, province, page):
                    logger.info(f"Successfully processed {province} page {page}")

if __name__ == "__main__":
    scraper = Rumah123Scraper()
    scraper.run()