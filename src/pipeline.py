def run_pipeline():
    print("Starting ETL pipeline...")

    try:
        # Step 1: Extract
        print("Extracting data...")
        raw_data = extract_data()

        # Step 2: Transform
        print("Transforming data...")
        transformed_df = transform_data(raw_data)

        # Step 3: Load
        print("Loading data into SQL Server...")
        load_to_sql(transformed_df)

        print("ETL pipeline completed successfully.")

    except Exception as e:
        print(f"Pipeline failed: {e}")


if __name__ == "__main__":
    run_pipeline()