from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure the ingestion code is in the path
sys.path.append('/opt/airflow/ingestion')

try:
    from extractors.fetch_prices import main as run_fetch
except Exception as e:
    error_msg = str(e)
    print(f"Error importing ingestion script: {error_msg}")
    def run_fetch(symbols):
        raise ImportError(f"FAILED TO IMPORT SCRIPT: {error_msg}")

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dezoomcampproject-493714")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "de-zoomcamp-market-data-493714")
LOCAL_DATA_DIR = "/app/data/raw/market_data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_ingestion_gcs_pipeline',
    default_args=default_args,
    description='End-to-end pipeline: Fetch stock data -> Parquet -> GCS -> BigQuery -> dbt',
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'gcs', 'parquet', 'bigquery', 'dbt'],
) as dag:

    # Task 1: Fetch data and save as LOCAL Parquet files
    def fetch_with_config(**kwargs):
        # Pull symbols from dag_run.conf (passed via API) or use default list
        conf = kwargs.get('dag_run').conf or {}
        symbols = conf.get('symbols', ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'AMZN'])
        print(f"Triggering ingestion for symbols: {symbols}")
        return run_fetch(symbols)

    fetch_task = PythonOperator(
        task_id='fetch_stock_prices_to_parquet',
        python_callable=fetch_with_config,
        provide_context=True,
    )

    # Task 2: Upload all newly created Parquet files to GCS
    def upload_to_gcs_func(bucket_name, local_path):
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        hook = GCSHook()
        
        for root, dirs, files in os.walk(local_path):
            for file in files:
                if file.endswith(".parquet"):
                    local_file = os.path.join(root, file)
                    # Create a relative path for GCS: e.g. AAPL/year_2024/month_4/data.parquet
                    remote_path = os.path.relpath(local_file, local_path)
                    print(f"Uploading {local_file} to gs://{bucket_name}/{remote_path}")
                    hook.upload(bucket_name, remote_path, local_file)

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs_func,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'local_path': LOCAL_DATA_DIR
        }
    )

    # Task 3: Load Parquet files from GCS to BigQuery Bronze
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    
    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bq_bronze',
        bucket=BUCKET_NAME,
        source_objects=['*.parquet'], 
        destination_project_dataset_table=f"{PROJECT_ID}.market_data_bronze.stock_prices",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # Task 4: Run dbt to build Silver and Gold layers
    from airflow.operators.bash import BashOperator
    
    dbt_run = BashOperator(
        task_id='dbt_run_medallion',
        bash_command='cd /opt/airflow/transformation && dbt run --profiles-dir .',
    )

    fetch_task >> upload_task >> load_to_bronze >> dbt_run