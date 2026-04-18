from fastapi import FastAPI
from pydantic import BaseModel
import subprocess
import sys

app = FastAPI()


class PipelineRequest(BaseModel):
    symbols: list[str]


@app.post("/run-pipeline")
def run_pipeline(request: PipelineRequest):
    symbols_str = ",".join(request.symbols)

    subprocess.run(
        [sys.executable, "../ingestion/extractors/fetch_prices.py", symbols_str]
    )

    return {"status": "Pipeline triggered"}