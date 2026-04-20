from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from requests.auth import HTTPBasicAuth
import os

app = FastAPI()

# Airflow Configuration
AIRFLOW_URL = "http://airflow-webserver:8080/api/v1"
DAG_ID = "stock_ingestion_gcs_pipeline"
AIRFLOW_AUTH = HTTPBasicAuth("airflow", "airflow")

class PipelineRequest(BaseModel):
    symbols: list[str]


@app.post("/run-pipeline")
def run_pipeline(request: PipelineRequest):
    """
    Triggers the Airflow DAG via the REST API.
    """
    trigger_url = f"{AIRFLOW_URL}/dags/{DAG_ID}/dagRuns"
    
    # Airflow Expects 'conf' for passing dynamic parameters
    payload = {
        "conf": {
            "symbols": request.symbols
        }
    }

    try:
        response = requests.post(
            trigger_url,
            json=payload,
            auth=AIRFLOW_AUTH
        )

        if response.status_code == 200:
            return {"status": "Pipeline triggered", "dag_run_id": response.json().get("dag_run_id")}
        else:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Airflow API Error: {response.text}"
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    return {"status": "healthy"}