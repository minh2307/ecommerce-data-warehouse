"""
Phase 2: Data Cleaning (Silver Layer)
=====================================
Reads Bronze layer data, applies data type casting, null handling,
string normalization, deduplication, and writes clean data to Silver layer.

Input:  data/bronze/events_raw.parquet
Output: data/silver/events_clean.parquet (partitioned by event_date)

Architecture Decision:
    The Silver layer is the "single source of truth" for cleaned data.
    All downstream models (dimensions, facts) read from Silver.
    Cleaning operations are idempotent — re-running produces identical results.
"""

import logging
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.spark_config import create_spark_session
from src.common.data_quality import (
    validate_row_count,
    validate_schema,
    validate_nulls,
    generate_profiling_report,
    save_quality_report,
)
from src.common.etl_metadata import ETLMetadataTracker

load_dotenv()

logger = logging.getLogger(__name__)

BRONZE_DATA_PATH = os.getenv("BRONZE_DATA_PATH", "data/bronze")
BRONZE_DATA_PATH = os.getenv("BRONZE_DATA_PATH", "data/bronze")
SILVER_DATA_PATH = os.getenv("SILVER_DATA_PATH", "data/silver")


def cast_data_types(df: DataFrame) -> DataFrame:
    """
    Cast columns from string to their proper data types.

    Args:
        df: Bronze DataFrame with string columns.

    Returns:
        DataFrame with correct data types.

    Design Decision:
        event_time → TimestampType for time-based joins and windowing.
        price → DoubleType for arithmetic operations.
        product_id, user_id, category_id → LongType/StringType as appropriate.
    """
    logger.info("Casting data types...")

    df = (
        df
        .withColumn("event_time", F.col("event_time").cast(TimestampType()))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("category_id", F.col("category_id").cast("long"))
        .withColumn("user_id", F.col("user_id").cast("long"))
    )

    logger.info("Data type casting completed")
    return df


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Handle null values in critical columns.

    Args:
        df: DataFrame with potential nulls.

    Returns:
        DataFrame with nulls replaced by default values.

    Design Decision:
        brand and category_code are filled with "unknown" rather than dropped
        because these rows still contain valuable event data (user_id, event_type).
        Dropping them would lose ~30% of events in typical e-commerce datasets.
    """
    logger.info("Handling null values...")

    df = (
        df
        .withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
        .withColumn("category_code", F.coalesce(F.col("category_code"), F.lit("unknown")))
    )

    # Drop rows where critical fields are null (these rows are unusable)
    critical_cols = ["event_time", "user_id", "product_id"]
    df = df.dropna(subset=critical_cols)
    logger.info("Null handling completed")
    return df


def normalize_strings(df: DataFrame) -> DataFrame:
    """
    Normalize string columns: trim whitespace and convert to lowercase.

    Args:
        df: DataFrame with string columns.

    Returns:
        DataFrame with normalized strings.

    Design Decision:
        Normalization ensures consistent grouping and joining.
        "Apple" and " apple " should map to the same brand.
    """
    logger.info("Normalizing strings...")

    string_columns = ["event_type", "brand", "category_code", "user_session"]
    for col_name in string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.lower(F.col(col_name))))

    logger.info("String normalization completed for: %s", string_columns)
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    Args:
        df: DataFrame with potential duplicates.

    Returns:
        Deduplicated DataFrame.

    Design Decision:
        Full row deduplication (all columns) is used because the source
        CSV may contain exact duplicate rows from export artifacts.
        Key-based deduplication would be too aggressive for events
        where the same user can trigger the same event type multiple times.
    """
    logger.info("Removing duplicates...")

    df = df.dropDuplicates()

    logger.info("Duplicate removal transformation queued")
    return df


def create_event_date(df: DataFrame) -> DataFrame:
    """
    Create event_date column from event_time for partitioning.

    Args:
        df: DataFrame with event_time column.

    Returns:
        DataFrame with event_date column added.

    Design Decision:
        event_date (DateType) is extracted from event_time for partition key.
        Date-based partitioning enables efficient partition pruning for
        time-range queries, which is the most common access pattern
        for web analytics data.
    """
    logger.info("Creating event_date column...")

    df = df.withColumn("event_date", F.to_date(F.col("event_time")))

    logger.info("event_date column created")
    return df


def write_silver(df: DataFrame, output_path: str) -> tuple[DataFrame, int]:
    """
    Write cleaned DataFrame to Silver layer as partitioned Parquet.

    Args:
        df: Cleaned DataFrame.
        output_path: Silver layer output directory.

    Returns:
        Tuple of (df_written, row_count).

    Design Decision:
        Partitioned by event_date for optimal query performance.
        ZSTD compression is applied via SparkSession config (spark_config.py).
    """
    silver_path = os.path.join(output_path, "events_clean.parquet")
    logger.info("Writing Silver layer to: %s", silver_path)

    # Drop the ingestion_date column (Bronze metadata, not needed in Silver)
    if "ingestion_date" in df.columns:
        df = df.drop("ingestion_date")

    df.write.mode("overwrite").partitionBy("event_date").parquet(silver_path)

    df_written = df.sparkSession.read.parquet(silver_path)
    row_count = df_written.count()
    logger.info("Silver layer written: %s rows", f"{row_count:,}")
    return df_written, row_count



def run_cleaning() -> None:
    """
    Execute the full cleaning pipeline (Phase 2).

    Steps:
        1. Read Bronze layer Parquet
        2. Cast data types
        3. Handle null values
        4. Normalize strings
        5. Remove duplicates
        6. Create event_date column
        7. Validate cleaned data
        8. Write to Silver layer
    """
    spark = create_spark_session(app_name="Phase2_Cleaning")

    try:
        with ETLMetadataTracker("cleaning") as tracker:
            # Step 1: Read Bronze layer
            bronze_path = os.path.join(BRONZE_DATA_PATH, "events_raw.parquet")
            logger.info("Reading Bronze layer from: %s", bronze_path)
            df = spark.read.parquet(bronze_path)

            # Step 2-6: Apply cleaning transformations
            df = cast_data_types(df)
            df = handle_nulls(df)
            df = normalize_strings(df)
            df = remove_duplicates(df)
            df = create_event_date(df)

            # Step 8: Write to Silver layer (Write first to avoid repeating dropDuplicates on downstream operations)
            df_written, row_count = write_silver(df, SILVER_DATA_PATH)
            tracker.set_rows_processed(row_count)

            # Step 7: Validate cleaned data using the already written and loaded DataFrame
            quality_results = []
            row_result = validate_row_count(df_written, "silver", min_rows=1)
            quality_results.append(row_result)

            null_result = validate_nulls(
                df_written,
                critical_columns=["event_time", "user_id", "product_id"],
                stage_name="silver",
                fail_on_nulls=True,
            )
            quality_results.append(null_result)

            profile = generate_profiling_report(df_written, "silver", row_count=row_count)
            quality_results.append(profile)

            save_quality_report(quality_results, "silver")

            logger.info("Phase 2 (Cleaning) completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/cleaning.log"),
        ],
    )
    os.makedirs("logs", exist_ok=True)
    run_cleaning()
