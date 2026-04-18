import requests
import time
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import sys

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

OUTPUT_DIR = "/app/data/raw/market_data"


def fetch_daily_data(symbol):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        raise Exception(f"API error for {symbol}: {response.status_code}")

    data = response.json()

    # Handle API limit message
    if "Note" in data:
        print("Rate limit hit. Sleeping...")
        time.sleep(60)
        return fetch_daily_data(symbol)

    return data


def save_to_file(symbol, data):
    now = datetime.utcnow()
    path = f"{OUTPUT_DIR}/{symbol}/year_{now.year}/month_{now.month}/day_{now.day}"
    os.makedirs(path, exist_ok=True)

    filename = f"{path}/data_{now.strftime('%H%M%S')}.json"

    with open(filename, "w") as f:
        json.dump(data, f)

    print(f"Saved data for {symbol} → {filename}")


def main(symbols):
    if not API_KEY:
        raise ValueError("API key not set.")

    for symbol in symbols:
        try:
            data = fetch_daily_data(symbol)
            save_to_file(symbol, data)
            time.sleep(15)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")


if __name__ == "__main__":
    symbols = sys.argv[1].split(",")  # "AAPL,MSFT"
    main(symbols)
