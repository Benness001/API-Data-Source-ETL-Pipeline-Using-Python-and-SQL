# Configure ETL Pipeline

import requests
import json
import pandas as pd
from pathlib import Path

API_URL = "https://jsonplaceholder.typicode.com/users"
RAW_DATA_PATH = "data/raw_data.json"
PROCESSED_DATA_PATH = "data/clean_data.csv"
Path("data").mkdir(exist_ok=True)

print("Environment ready ✅")
print(f"API URL: {API_URL}")
print(f"RAW DATA PATH: {RAW_DATA_PATH}")

#Extract Raw API DATA

def extract_data():
    print("Extracting data from API...")

    response = requests.get(API_URL)
    response.raise_for_status()

    data = response.json()

    with open(RAW_DATA_PATH, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Saved {len(data)} records to {RAW_DATA_PATH}")

    return data

