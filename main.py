# main.py
import sys
from src import extract_data, transform_data, load_to_sql

def run_pipeline():
    """
    Orchestrates the Project Atlas ETL Pipeline:
    Stage 1: Data Acquisition & Ingestion
    Stage 2: Data Processing & Transformation (ETL Layer)
    """
    print("--- 🚀 Initializing Project Atlas ETL Pipeline ---")

    try:
        # STEP 1: EXTRACT (Data Acquisition)
        # Fetches raw JSON from API and saves locally
        print("\n[1/3] Starting Stage 1: Data Acquisition...")
        raw_data = extract_data()
        
        if not raw_data:
            print("❌ Extraction failed: No data received. Exiting.")
            return

        # STEP 2: TRANSFORM (Data Processing)
        # Flattens JSON, cleans text, and performs Feature Engineering
        print("\n[2/3] Starting Stage 2: Data Processing & Transformation...")
        transformed_df = transform_data(raw_data)

        # STEP 3: LOAD (Data Storage Layer)
        # Pushes cleaned DataFrame to SQL Server
        print("\n[3/3] Finalizing Stage 2: Loading to Data Storage...")
        load_to_sql(transformed_df)

        print("\n--- ✅ ETL Pipeline Completed Successfully! ---")
        print(f"Total Records Processed: {len(transformed_df)}")

    except ConnectionError as ce:
        print(f"\n❌ Stage 1 Failed: Network/API connection error: {ce}")
    except Exception as e:
        print(f"\n❌ Pipeline failed unexpectedly: {e}")
        # Exit with error code for automation tools (GitHub Actions/Jenkins)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()