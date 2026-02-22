from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime
from loguru import logger
import os



# Configure logging
logger.add("logs/processing.log", rotation="1 MB")


def create_spark_session():
    """Initialize Spark session"""
    spark = SparkSession.builder \
            .appName("FinancialDataProcessing")\
            .config("spark.driver.memory","2g")\
            .getOrCreate()
    
    logger.info("Spark session created")
    return spark


def read_raw_data(spark, raw_path):
    """Read all CSV files from raw data directory"""
    logger.info(f"Reading data from {raw_path}")
    
    df = spark.read.csv(
        f"{raw_path}/*.csv",
        header = True,
        inferSchema = True
    )
    logger.info(f"Loaded {df.count()} rows")
    return df


def cast_data_type(df):
    """Cast columns to proper data types"""

    logger.info("Casting data types...")

    df = df.withColumn("Date",to_date(col("Date")))\
            .withColumn("Open",col("Open").cast(DoubleType()))\
            .withColumn("Close",col("Close").cast(DoubleType()))\
            .withColumn("High",col("High").cast(DoubleType()))\
            .withColumn("Low",col("Low").cast(DoubleType()))\
            .withColumn("Volume",col("Volume").cast(DoubleType()))
    
    logger.info("Data types cast successfully")
    return df

def add_calculated_columns(df):
    logger.info("Adding calculated columns")

    df = df.withColumn("daily_return", (col("Close")-col("Open"))/col("Open")*100)\
            .withColumn("price_range",col("High") - col("Low")) \
            .withColumn("ingestion_timestamp", current_timestamp())
    return df


def data_quality_checks(df):
    '''Run basic data quality checks'''
    logger.info("Running data quality checks'")

    total_rows = df.count()
    null_counts = df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c) 
        for c in ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]
    ]).collect()[0]

    negative_prices = df.filter((col("Open")<0)|(col("Close")<0)|(col("High")<0)|(col("Low")<0)).count()

    high_lt_low = df.filter(col("High")<col("Low")).count()

    logger.info(f"Total rows: {total_rows}")
    logger.info(f"Null counts: {null_counts.asDict()}")
    logger.info(f"Negative prices: {negative_prices}")
    logger.info(f"High < Low violations: {high_lt_low}")

    if negative_prices > 0 or high_lt_low > 0:
        raise ValueError("Data quality check failed — bad records found")
    
    logger.success("All quality checks passed")
    return df

def save_to_parquet(df, output_path):
    """Save processed data to parquet format"""

    logger.info(f"Saving to parquet at {output_path}")

    df.write.mode("overwrite").partitionBy("Ticker").parquet(output_path) # we partition by the thing
        #                                                                   that we're gonna do queries on the filtered by this colum ex- where ticker = aapl(for query performance optimization)

    logger.success(f"✓ Data saved to {output_path}")


if __name__=="__main__":
    RAW_DATA_PATH = "data/raw"
    PROCESSED_DATA_PATH = "data/processed"  

    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

    spark = create_spark_session()
    df = read_raw_data(spark, RAW_DATA_PATH)
    df = cast_data_type(df)
    df = add_calculated_columns(df)
    df = data_quality_checks(df)
    save_to_parquet(df, PROCESSED_DATA_PATH)

    spark.stop()
    
