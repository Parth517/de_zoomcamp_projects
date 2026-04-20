# 📈 Market Momentum: Orchestrated Stock Analytics Pipeline

This project is part of the **Data Engineering Zoomcamp**. It implements an end-to-end data pipeline to automate the collection, processing, and analysis of stock market data using the Medallion Architecture.

## 📖 Problem Description
**The Problem**: Individual investors and analysts often struggle with manual data collection from various financial APIs. Parsing raw JSON, handling API rate limits, deduplicating overlapping historical data, and calculating trend metrics (like moving averages) is time-consuming and error-prone.

**The Solution**: This platform provides a fully automated, orchestrated pipeline that:
1.  **Ingests** high-fidelity stock data via the Alpha Vantage API.
2.  **Orchestrates** the flow from extraction to warehouse using **Apache Airflow**.
3.  **Refines** the data using **dbt** (Medallion Architecture) to ensure data quality and provide ready-to-use analytical insights.
4.  **Visualizes** key trends and performance metrics through an interactive **Streamlit** dashboard.

## 🏗️ Architecture
- **Infratructure**: Terraform (IaC) on Google Cloud Platform.
- **Data Lake**: Google Cloud Storage (Bucket).
- **Data Warehouse**: Google BigQuery (Bronze, Silver, Gold layers).
- **Orchestration**: Apache Airflow in Docker.
- **Transformation**: dbt (Data Build Tool).
- **Dashboard**: Streamlit.

## 🚀 Reproducibility: How to Run

### 1. Prerequisites
- Docker & Docker Compose
- Google Cloud Platform Account
- Alpha Vantage API Key ([Get one here](https://www.alphavantage.co/support/#api-key))

### 2. Setup Infrastructure
1. Place your Google Service Account key at `./google_credentials.json`.
2. Configure your `.env` file:
   ```env
   ALPHA_VANTAGE_API_KEY=your_key_here
   GCP_PROJECT_ID=your_project_id
   ```
3. Run Terraform to provision BigQuery and GCS:
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

### 3. Launch Services
```bash
docker-compose up -d --build
```
Access the systems at:
- **Dashboard**: `http://localhost:8501`
- **Airflow**: `http://localhost:8080` (User/Pass: `airflow`/`airflow`)

## 📊 Data Warehouse Optimization
To maximize query performance and minimize costs, the **Silver** and **Gold** layers are:
- **Partitioned** by `price_date`: Allows BigQuery to prune partitions when querying specific time ranges.
- **Clustered** by `symbol`: Segments data by stock ticker for fast lookups in the dashboard.

---
*Created by Parth as part of the DE Zoomcamp final project.*
