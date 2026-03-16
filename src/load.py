# Load Transformed Data to an SQL database (ETL_DB)

import pandas as pd
from sqlalchemy import create_engine 
import urllib

def load_to_sql(df):
    print("Loading data into SQL Server...")

    # 1. Setup paths and connection string
    PROCESSED_DATA_PATH = "data/clean_data.csv"
    
    # Use double backslashes for the server name instance
    # Update the conn_str in your load script with your local server instance name: "SERVER=YOUR_SERVER_NAME\\SQLEXPRESS;"
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=YOUR_SERVER_NAME\\SQLEXPRESS;"
        "DATABASE=ETL_DB;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    
    # 2. Create SQLAlchemy Engine (Crucial: requires URL encoding for the string)
    quoted_conn = urllib.parse.quote_plus(conn_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_conn}")

    try:
        # 3. Load the cleaned CSV and Upload to SQL
        # 'append' adds data to existing table; 'replace' recreates it

        df.to_sql(
            name="Users", 
            con=engine, 
            if_exists="replace", 
            index=False,
            chunksize=1000
        )
        
        print("Load completed successfully.")
        print(df)

    except Exception as e:
        print(f"An error occurred: {e}")