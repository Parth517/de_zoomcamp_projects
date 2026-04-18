from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure the ingestion code is in the path
# In the Airflow container, we mounted ./ingestion to /opt/airflow/ingestion
sys.path.append('/opt/airflow/ingestion')

# Import the main function from your script
try:
    from extractors.fetch_prices import main as run_fetch
except ImportError as e:
    print(f"Error importing ingestion script: {e}")
    # Define a dummy function for Airflow parsing if import fails
    def run_fetch(symbols):
        print(f"FAILED TO IMPORT SCRIPT: {e}")

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
    "stock_ingestion_docker",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_task = DockerOperator(
        task_id="run_ingestion",
        image="ingestion-image:latest",
        command="AAPL,MSFT,TSLA",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "ALPHA_VANTAGE_API_KEY": "{{ var.value.ALPHA_VANTAGE_API_KEY }}"
        },
        auto_remove=True,
    )