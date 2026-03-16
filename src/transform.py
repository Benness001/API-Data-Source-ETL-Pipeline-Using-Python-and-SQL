#Transform Raw API DATA

import pandas as pd
import json
import os
from pathlib import Path

API_URL = "https://jsonplaceholder.typicode.com/users"
RAW_DATA_PATH = "api_data.json"
PROCESSED_DATA_PATH = "data/clean_data.csv"
Path("data").mkdir(exist_ok=True)

# Create data folder
os.makedirs("data", exist_ok=True)

print("Folder created successfully ✅")


def transform_data(data):
    print("Transforming data...")

    # ---------------------------
    # LOAD RAW DATA
    # ---------------------------

    df = pd.json_normalize(data)

    # ---------------------------
    # CLEAN COLUMN NAMES
    # ---------------------------
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(".", "_", regex=False)
        .str.strip()
    )

    # ---------------------------
    # SELECT IMPORTANT COLUMNS
    # ---------------------------
    df = df[
        [
            "id",
            "name",
            "username",
            "email",
            "address_city",
            "company_name"
        ]
    ]

    # ---------------------------
    # DATA TYPE ENFORCEMENT
    # ---------------------------
    df["id"] = df["id"].astype(int)
    df["name"] = df["name"].astype(str)
    df["email"] = df["email"].astype(str)

    # ---------------------------
    # TEXT STANDARDIZATION
    # ---------------------------
    df["name"] = df["name"].str.title()
    df["username"] = df["username"].str.lower()
    df["email"] = df["email"].str.lower()
    df["address_city"] = df["address_city"].str.title()
    df["company_name"] = df["company_name"].str.title()

    # ---------------------------
    # HANDLE MISSING VALUES
    # ---------------------------
    df.fillna(
        {
            "address_city": "Unknown",
            "company_name": "Unknown"
        },
        inplace=True
    )

    # ---------------------------
    # REMOVE DUPLICATES
    # ---------------------------
    df.drop_duplicates(subset=["id"], inplace=True)

    # ---------------------------
    # FEATURE ENGINEERING
    # ---------------------------

    # Extract email domain
    df["email_domain"] = df["email"].str.split("@").str[-1]

    # Name length feature
    df["name_length"] = df["name"].str.len()

    # Username length
    df["username_length"] = df["username"].str.len()

    # City + Company combined field
    df["location_company"] = (
        df["address_city"] + " - " + df["company_name"]
    )

    # ---------------------------
    # DATA QUALITY CHECKS
    # ---------------------------

    # Remove invalid emails
    df = df[df["email"].str.contains("@", na=False)]

    # Remove empty names
    df = df[df["name"].str.strip() != ""]

    # ---------------------------
    # SORT DATASET
    # ---------------------------
    df.sort_values(by="id", inplace=True)

    # Reset index
    df.reset_index(drop=True, inplace=True)

    # ---------------------------
    # SAVE CLEAN DATA
    # ---------------------------
    df.to_csv(PROCESSED_DATA_PATH, index=False)

    print("Transformation completed successfully ✅")

    #print(df)

    return df

#transform_data(extract_data())