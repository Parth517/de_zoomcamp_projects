import requests
import time
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import sys

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

# We'll use /app/data as the root for consistency across Docker containers
OUTPUT_DIR = os.getenv("DATA_DIR", "/app/data/raw/market_data")


def fetch_daily_data(symbol):
    """Fetches daily stock data from Alpha Vantage."""
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact", # Last 100 days
        "apikey": API_KEY
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        raise Exception(f"API error for {symbol}: {response.status_code}")

    data = response.json()

    # Handle API limit message
    if "Note" in data:
        print("Rate limit hit. Sleeping for 60s...")
        time.sleep(60)
        return fetch_daily_data(symbol)
    
    if "Error Message" in data:
         raise Exception(f"Alpha Vantage Error: {data['Error Message']}")

    return data


def format_and_clean_data(symbol, raw_data):
    """Refines the raw JSON into a flat, typed Pandas DataFrame."""
    if "Time Series (Daily)" not in raw_data:
        raise ValueError(f"No time series data found for {symbol}")

    # Convert the 'Time Series (Daily)' dict to a DataFrame
    df = pd.DataFrame.from_dict(raw_data["Time Series (Daily)"], orient='index')
    
    # Cleaning column names (e.g., '1. open' -> 'open')
    df.columns = [col.split(". ")[1] for col in df.columns]
    
    # Convert index to a proper date column
    df.index.name = 'date'
    df.reset_index(inplace=True)
    
    # Ensure numeric types
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col])
    
    # Add metadata
    df['symbol'] = symbol
    df['ingested_at'] = datetime.utcnow()
    
    # Reorder columns for better readability
    cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'ingested_at']
    return df[cols]


def save_as_parquet(df, symbol):
    """Saves the DataFrame as a Parquet file partitioned by date."""
    now = datetime.utcnow()
    
    # Flatten the structure for BigQuery compatibility (one wildcard limit)
    path = f"{OUTPUT_DIR}"
    os.makedirs(path, exist_ok=True)

    filename = f"{path}/{symbol}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    
    df.to_parquet(filename, index=False)
    print(f"Saved {len(df)} rows for {symbol} → {filename}")


def main(symbols):
    if not API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set.")

    for symbol in symbols:
        try:
            print(f"Fetching data for {symbol}...")
            raw_data = fetch_daily_data(symbol)
            
            print(f"Processing and flattening data...")
            df = format_and_clean_data(symbol, raw_data)
            
            save_as_parquet(df, symbol)
            
            # Respect API limits
            time.sleep(15)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")


if __name__ == "__main__":
    # Handle both comma-separated string or list from Airflow
    if len(sys.argv) > 1:
        symbols_input = sys.argv[1]
        symbols = symbols_input.split(",") if "," in symbols_input else [symbols_input]
        main(symbols)
    else:
        print("Usage: python fetch_prices.py SYMBOL1,SYMBOL2...")
