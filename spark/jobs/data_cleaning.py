from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import IntegerType, FloatType, StringType
from datetime import datetime
import json
from hdfs import InsecureClient
import logging
from typing import Optional, Dict
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Data Cleaning Pipeline") \
            .getOrCreate()
            
        # HDFS configuration
        self.hdfs_url = "http://namenode:9870"
        self.hdfs_client = InsecureClient(self.hdfs_url, user="hadoop")
        
        # HDFS paths
        self.raw_rumah123_path = "/data/raw/rumah123"
        self.raw_osm_path = "/data/raw/osm_facilities"
        self.cleaned_rumah123_path = "/data/cleaned/rumah123"
        self.cleaned_osm_path = "/data/cleaned/osm_facilities"
        self.progress_file = "/progress.json"

    def load_progress(self) -> Dict:
        """Load the current progress state."""
        try:
            with self.hdfs_client.read(self.progress_file) as reader:
                return json.load(reader)
        except Exception as e:
            logger.error(f"Failed to load progress: {e}")
            raise

    def save_progress(self, progress: Dict) -> None:
        """Save updated progress state."""
        try:
            with self.hdfs_client.write(self.progress_file, overwrite=True) as writer:
                json.dump(progress, writer)
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
            raise

    def clean_rumah123_data(self, df):
        """Clean and transform rumah123 raw data."""
        try:
            required_columns = [
                                    "judul_iklan", 
                                    "harga", 
                                    "kecamatan", 
                                    "kabupaten_kota",
                                    "provinsi",
                                    "terakhir_diperbarui",
                                    "agen",
                                    "link_rumah123",
                                    "kamar_tidur",
                                    "kamar_mandi",
                                    "luas_tanah",
                                    "luas_bangunan",
                                    "carport",
                                    "sertifikat",
                                    "daya_listrik",
                                    "kamar_tidur_pembantu",
                                    "kamar_mandi_pembantu",
                                    "dapur",
                                    "ruang_makan",
                                    "ruang_tamu",
                                    "kondisi_perabotan",
                                    "material_bangunan",
                                    "material_lantai",
                                    "garasi",
                                    "jumlah_lantai",
                                    "konsep_dan_gaya_rumah",
                                    "pemandangan",
                                    "terjangkau_internet",
                                    "lebar_jalan",
                                    "tahun_dibangun",
                                    "tahun_direnovasi",
                                    "sumber_air",
                                    "hook",
                                    "kondisi_properti",
                                ]  
            
            for col_name in required_columns:
                if col_name not in df.columns:
                    raise ValueError(f"Required column {col_name} is missing from data.")

            # Your existing column transformations
            df = df.withColumn(
                "harga",
                when(col("harga").rlike(r"Rp \d+,\d+ Miliar"), 
                     (col("harga").substr(5, 10).cast(FloatType()) * 1000000).cast(IntegerType()))
                .when(col("harga").rlike(r"Rp \d+,\d+ Juta"), 
                      (col("harga").substr(5, 10).cast(FloatType()) * 1000000).cast(IntegerType()))
                .otherwise(None)
            )
            
            df = df.withColumn("kecamatan", col("kecamatan").cast(StringType())) \
                .withColumn("kabupaten_kota", col("kabupaten_kota").cast(StringType())) \
                .withColumn("provinsi", col("provinsi").cast(StringType())) \
                .withColumn("terakhir_diperbarui", col("terakhir_diperbarui").cast(StringType())) \
                .withColumn("agen", col("agen").cast(StringType())) \
                .withColumn("link_rumah123", col("link_rumah123").cast(StringType())) \
                .withColumn("kamar_tidur", col("kamar_tidur").cast(IntegerType())) \
                .withColumn("kamar_mandi", col("kamar_mandi").cast(IntegerType())) \
                .withColumn("luas_tanah", col("luas_tanah").cast(FloatType())) \
                .withColumn("luas_bangunan", col("luas_bangunan").cast(FloatType())) \
                .withColumn("carport", col("carport").cast(IntegerType())) \
                .withColumn("sertifikat", col("sertifikat").cast(StringType())) \
                .withColumn("daya_listrik", col("daya_listrik").cast(IntegerType())) \
                .withColumn("kamar_tidur_pembantu", col("kamar_tidur_pembantu").cast(IntegerType())) \
                .withColumn("kamar_mandi_pembantu", col("kamar_mandi_pembantu").cast(IntegerType())) \
                .withColumn("dapur", col("dapur").cast(IntegerType())) \
                .withColumn("ruang_makan", col("ruang_makan").cast(StringType())) \
                .withColumn("ruang_tamu", col("ruang_tamu").cast(StringType())) \
                .withColumn("kondisi_perabotan", col("kondisi_perabotan").cast(StringType())) \
                .withColumn("material_bangunan", col("material_bangunan").cast(StringType())) \
                .withColumn("material_lantai", col("material_lantai").cast(StringType())) \
                .withColumn("garasi", col("garasi").cast(IntegerType())) \
                .withColumn("jumlah_lantai", col("jumlah_lantai").cast(IntegerType())) \
                .withColumn("konsep_dan_gaya_rumah", col("konsep_dan_gaya_rumah").cast(StringType())) \
                .withColumn("pemandangan", col("pemandangan").cast(StringType())) \
                .withColumn("terjangkau_internet", col("terjangkau_internet").cast(StringType())) \
                .withColumn("lebar_jalan", col("lebar_jalan").cast(StringType())) \
                .withColumn("tahun_dibangun", col("tahun_dibangun").cast(IntegerType())) \
                .withColumn("tahun_direnovasi", col("tahun_direnovasi").cast(IntegerType())) \
                .withColumn("sumber_air", col("sumber_air").cast(StringType())) \
                .withColumn("hook", col("hook").cast(StringType())) \
                .withColumn("kondisi_properti", col("kondisi_properti").cast(StringType()))
            
            return df
        except Exception as e:
            logger.error(f"Error cleaning rumah123 data: {str(e)}")
            raise

    def clean_osm_facilities_data(self, df):
        """Clean and transform OpenStreetMap facilities data."""
        try:
            required_columns = [
                "kecamatan",
                "jumlah_fasilitas_pendidikan", 
                "jumlah_fasilitas_kesehatan", 
                "jumlah_fasilitas_perbelanjaan", 
                "jumlah_fasilitas_transportasi",
                "jumlah_fasilitas_rekreasi"
            ]
            
            for col_name in required_columns:
                if col_name not in df.columns:
                    raise ValueError(f"Required column {col_name} is missing from data.")

            df = df.withColumn("jumlah_fasilitas_pendidikan", col("jumlah_fasilitas_pendidikan").cast(IntegerType())) \
                   .withColumn("jumlah_fasilitas_kesehatan", col("jumlah_fasilitas_kesehatan").cast(IntegerType())) \
                   .withColumn("jumlah_fasilitas_perbelanjaan", col("jumlah_fasilitas_perbelanjaan").cast(IntegerType())) \
                   .withColumn("jumlah_fasilitas_transportasi", col("jumlah_fasilitas_transportasi").cast(IntegerType())) \
                   .withColumn("jumlah_fasilitas_rekreasi", col("jumlah_fasilitas_rekreasi").cast(IntegerType()))
            
            return df
        except Exception as e:
            logger.error(f"Error cleaning OSM facilities data: {str(e)}")
            raise

    def process_current_page(self, progress: Dict) -> bool:
        """Process all data for the current pagination page."""
        current_page = progress["current_page"]
        logger.info(f"Processing data for page {current_page}")
        
        try:
            # 1. Process Rumah123 data
            rumah123_dfs = []
            for province, page in progress["provinces"].items():
                if page != current_page:
                    continue
                    
                file_path = f"{self.raw_rumah123_path}/{province}/page_{current_page}_{province}.csv"
                if self.hdfs_client.status(file_path, strict=False):
                    df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                    cleaned_df = self.clean_rumah123_data(df)
                    rumah123_dfs.append(cleaned_df)
            
            if rumah123_dfs:
                combined_rumah123_df = rumah123_dfs[0]
                for df in rumah123_dfs[1:]:
                    combined_rumah123_df = combined_rumah123_df.union(df)
                
                # Save cleaned Rumah123 data
                save_path = f"{self.cleaned_rumah123_path}/page_{current_page}"
                combined_rumah123_df.write.mode("overwrite").csv(save_path, header=True)
            
            # 2. Process OSM Facilities data
            # Get unique kecamatans from the current page's Rumah123 data
            if rumah123_dfs:
                kecamatans = combined_rumah123_df.select("kecamatan").distinct().collect()
                osm_dfs = []
                
                for row in kecamatans:
                    kecamatan = row["kecamatan"]
                    file_path = f"{self.raw_osm_path}/{kecamatan}.csv"
                    
                    if self.hdfs_client.status(file_path, strict=False):
                        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                        cleaned_df = self.clean_osm_facilities_data(df)
                        osm_dfs.append(cleaned_df)
                
                if osm_dfs:
                    combined_osm_df = osm_dfs[0]
                    for df in osm_dfs[1:]:
                        combined_osm_df = combined_osm_df.union(df)
                    
                    # Save cleaned OSM data
                    save_path = f"{self.cleaned_osm_path}/page_{current_page}"
                    combined_osm_df.write.mode("overwrite").csv(save_path, header=True)
            
            # 3. Update progress
            # Only update if we've successfully processed everything
            for province in progress["provinces"]:
                if progress["provinces"][province] == current_page:
                    progress["provinces"][province] = current_page + 1
            progress["current_page"] = current_page + 1
            self.save_progress(progress)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing page {current_page}: {e}")
            return False

    def run(self):
        """Main execution flow."""
        try:
            # Load current progress
            progress = self.load_progress()
            
            # Process current page
            success = self.process_current_page(progress)
            
            if not success:
                logger.error("Failed to process current page")
                return
            
            logger.info("Successfully completed data cleaning pipeline")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()