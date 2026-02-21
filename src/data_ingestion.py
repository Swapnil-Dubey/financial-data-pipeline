import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import os
from loguru import logger

TICKERS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]
START_DATE = "2023-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")
RAW_DATA_PATH = "data/raw"

def log_ingestion_metadata(successful, failed):
    metadata = {
        "run_timestamp": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
        "successful_tickers": ",".join(successful),
        "failed_tickers": ",".join(failed),
        "total_fetched": len(successful),
        "total_failed": len(failed)
    }
    
    metadata_df = pd.DataFrame([metadata])
    metadata_path = "logs/ingestion_metadata.csv"
    
    # If file exists, append. If not, create it with headers.
    if os.path.exists(metadata_path):
        metadata_df.to_csv(metadata_path, mode="a", header=False, index=False)
    else:
        metadata_df.to_csv(metadata_path, mode="w", header=True, index=False)
    
    logger.success(f"Metadata logged to {metadata_path}")

def fetch_stock_data(tickers, start_date, end_date):
    successful = []
    failed = []
    for ticker in tickers:
        logger.info(f"Getting data for {ticker}...")

        try:
            data = yf.download(ticker, start_date, end_date)
            data.columns = data.columns.get_level_values(0)  # Flatten multi-level headers
            data["Ticker"] = ticker


            if data.empty:
                logger.warning("API call returned no data")
                failed.append(ticker)
                continue

            today = datetime.today().strftime("%Y-%m-%d")
            filepath = f"{RAW_DATA_PATH}/{ticker}_{today}.csv"
            data.to_csv(filepath)

            logger.success(f"ticker {ticker} returned {len(data)} rows of data saved to {filepath}")
            successful.append(ticker)

            #rate limiting
            time.sleep(1)
        

        except Exception as e:
            logger.error(f"Exception occurred at {ticker}: {e}")
            failed.append(ticker)

    return successful, failed


if __name__ == "__main__": # means only run this block if running this file directly, dont run when importing from other files
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    successful, failed = fetch_stock_data(TICKERS, START_DATE, END_DATE)
    log_ingestion_metadata(successful, failed)


    print(f"INGESTION SUMMARY")
    print(f"{'='*40}")
    print(f"Successful: {successful}")
    print(f"Failed:     {failed}")
    print(f"Total: {len(successful)}/{len(TICKERS)} tickers fetched")



