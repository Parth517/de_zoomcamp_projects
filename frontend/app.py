import streamlit as st
import requests
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import os

# Configuration
BACKEND_URL = "http://backend:8000/run-pipeline"
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dezoomcampproject-493714")
DATASET_ID = "market_data_gold"
TABLE_ID = "fct_stock_metrics"

st.set_page_config(page_title="Market Insights Platform", layout="wide")

st.title("📈 Market Insights & Orchestration")

# --- SIDEBAR: Controls ---
with st.sidebar:
    st.header("Orchestration")
    available_stocks = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
    symbols = st.multiselect("Select Stocks to Ingest", available_stocks, default=["AAPL", "TSLA"])
    
    if st.button("🚀 Trigger Airflow Pipeline"):
        if not symbols:
            st.warning("Please select at least one stock")
        else:
            try:
                response = requests.post(BACKEND_URL, json={"symbols": symbols})
                if response.status_code == 200:
                    st.success("Pipeline triggered!")
                else:
                    st.error(f"Failed: {response.text}")
            except Exception as e:
                st.error(f"Error: {e}")

# --- MAIN: Dashboard ---
st.subheader("Analytical Dashboard (Medallion Gold Layer)")

def get_data():
    client = bigquery.Client()
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` ORDER BY price_date ASC"
    return client.query(query).to_dataframe()

try:
    df = get_data()
    
    if df.empty:
        st.info("No data found in BigQuery. Please run the pipeline first.")
    else:
        selected_symbol = st.selectbox("Filter by Symbol", df['symbol'].unique())
        filtered_df = df[df['symbol'] == selected_symbol]

        # --- TILE 1: Price vs Moving Average ---
        st.markdown("### 📊 Tile 1: Trend Analysis")
        fig = px.line(filtered_df, x="price_date", y=["close_price", "moving_avg_7d"], 
                      title=f"{selected_symbol} Close Price vs 7-Day Moving Average",
                      labels={"value": "Price (USD)", "price_date": "Date"},
                      template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

        # --- TILE 2: Market Performance ---
        st.markdown("### ⚡ Tile 2: Performance Metrics")
        latest_data = filtered_df.iloc[-1]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Latest Price", f"${latest_data['close_price']:.2f}", 
                    delta=f"{latest_data['daily_pct_change']:.2f}%")
        col2.metric("7D Moving Avg", f"${latest_data['moving_avg_7d']:.2f}")
        col3.metric("Volatility Index", "Low" if abs(latest_data['daily_pct_change']) < 2 else "High")

except Exception as e:
    st.warning(f"Waiting for Data Warehouse initialization... (Error: {str(e)[:50]}...)")

st.divider()
st.caption("Data source: Alpha Vantage API via Airflow Ingestion Pipeline")
