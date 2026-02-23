from google.cloud import bigquery
from loguru import logger
import os

logger.add("logs/bigquery.log", rotation="1 MB")

PROJECT_ID = "financial-data-pipeline-488207"
DATASET_ID = "financial_pipeline"
PROCESSED_DATA_PATH = "data/processed"

def create_bigquery_client():
    """Initialize BigQuery client using ADC"""
    client = bigquery.Client(project=PROJECT_ID)
    logger.info("BigQuery client created")
    return client

def create_dataset(client):
    """Create dataset if it doesn't exist"""
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    client.create_dataset(dataset, exists_ok = True)

    logger.success(f"Dataset {DATASET_ID} ready")

def load_parquet_to_bigquery(client, table_name):
    """Load parquet files into BigQuery table"""
    import pandas as pd
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    all_dfs = []
    for ticker_folder in os.listdir(PROCESSED_DATA_PATH):
        folder_path = os.path.join(PROCESSED_DATA_PATH, ticker_folder)
        if os.path.isdir(folder_path) and ticker_folder.startswith("Ticker="):
            ticker = ticker_folder.split("=")[1]
            for file in os.listdir(folder_path):
                if file.endswith(".parquet"):
                    df = pd.read_parquet(os.path.join(folder_path, file))
                    df["Ticker"] = ticker
                    all_dfs.append(df)
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Loaded {len(combined_df)} rows across {len(all_dfs)} files")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    
    job = client.load_table_from_dataframe(combined_df, table_ref, job_config=job_config)
    job.result()
    
    table = client.get_table(table_ref)
    logger.success(f"✓ Loaded {table.num_rows} rows into {table_ref}")


def create_fact_table(client):
    """Create cleaned fact table from staging"""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}"
    query = f"""
    CREATE OR REPLACE TABLE `{table_ref}.fct_stock_prices` AS
    SELECT
        Date,
        Ticker,
        Open,
        High,
        Low,
        Close,
        Volume,
        ROUND(daily_return, 4) AS daily_return,
        ROUND(price_range, 4) AS price_range,
        ingestion_timestamp
    FROM `{table_ref}.raw_stock_prices`
    WHERE Date IS NOT NULL
        AND Close IS NOT NULL
        AND Volume > 0
    ORDER BY Ticker, Date
    """

    job = client.query(query) 
    job.result()
    table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.fct_stock_prices")
    logger.success(f"✓ Created fct_stock_prices with {table.num_rows} rows")


def create_aggregate_table(client):
    """Create aggregate table from fact table"""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}"
    query = f"""
    CREATE OR REPLACE TABLE `{table_ref}.agg_daily_summary` AS
    SELECT
        Ticker,
        count(*) as total_trading_days,
        ROUND(AVG(daily_return), 4) AS avg_daily_return,
        ROUND(MAX(daily_return), 4) AS max_daily_return,
        ROUND(MIN(daily_return), 4) AS min_daily_return,
        ROUND(AVG(Volume), 0) AS avg_volume,
        ROUND(MAX(Close), 2) AS all_time_high,
        ROUND(MIN(Close), 2) AS all_time_low
    FROM `{table_ref}.fct_stock_prices`
    GROUP BY Ticker
    ORDER BY Ticker
    """

    job = client.query(query) 
    job.result()
    table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.agg_daily_summary")
    logger.success(f"Created agg_daily_summary with {table.num_rows} rows")


if __name__ == "__main__":
    client = create_bigquery_client()
    create_dataset(client)
    load_parquet_to_bigquery(client, "raw_stock_prices")
    create_fact_table(client)
    create_aggregate_table(client)

