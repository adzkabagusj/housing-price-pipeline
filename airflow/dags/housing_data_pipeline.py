from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'housing_data_pipeline',
    default_args=default_args,
    description='A pipeline to scrape and process housing data',
    schedule_interval=timedelta(minutes=30),  # Runs every 30 minutes
    start_date=days_ago(1),
    catchup=False
)

# Task 1: Scrape Data from Rumah123
def scrape_rumah123():
    """
    This task will scrape data from rumah123.com and store it in HDFS.
    """
    import subprocess
    subprocess.run(["python", "/scripts/scraper/rumah123_scraper.py"])

scrape_task = PythonOperator(
    task_id='scrape_rumah123_data',
    python_callable=scrape_rumah123,
    dag=dag
)

# Task 2: Scrape OpenStreetMap API Data
def scrape_osm_data():
    """
    This task will call the OpenStreetMap API based on 'kecamatan'.
    """
    import subprocess
    subprocess.run(["python", "/scripts/api/fetch_facilities.py"])

osm_task = PythonOperator(
    task_id='scrape_osm_data',
    python_callable=scrape_osm_data,
    dag=dag
)

# Task 3: Submit Spark Job for Cleaning All Data
def submit_spark_cleaning():
    """
    This task will run a Spark job to clean all data (Rumah123 and OSM).
    """
    return SparkSubmitOperator(
        task_id='clean_all_data',
        conn_id='spark_default',
        application='/spark/jobs/data_cleaning.py',
        name='clean_all_data',
        conf={'spark.executor.memory': '2g', 'spark.driver.memory': '2g'},
        dag=dag
    )

spark_clean_all_task = submit_spark_cleaning()

# Task 4: Upload Socio-Economic CSV to HDFS
# upload_socio_economic_task = BashOperator(
#     task_id='upload_socio_economic_csv_to_hdfs',
#     bash_command='hdfs dfs -put /scripts/csv/socio-economic.csv /data/raw/socio-economic/',
#     dag=dag
# )

# Task Dependencies
scrape_task >> osm_task >> spark_clean_all_task 
# scrape_task >> osm_task >> spark_clean_all_task >> upload_socio_economic_task
