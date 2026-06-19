"""
Phase 1: Data Ingestion (Bronze Layer)
======================================
Reads raw Parquet data from data/raw/, validates the schema, generates a
profiling report, and writes to the Bronze layer as partitioned Parquet.

Input:  data/raw/events_raw.parquet
Output: data/bronze/events_raw.parquet (partitioned by ingestion_date)

Architecture Decision:
    The Bronze layer stores data as-is from the source with minimal
    transformation. Only an ingestion_date column is added for partition
    management and data lineage tracking. All type casting and cleaning
    is deferred to the Silver layer (Phase 2).
"""

import logging
import os
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.spark_config import create_spark_session
from src.common.data_quality import (
    validate_row_count,
    validate_schema,
    generate_profiling_report,
    save_quality_report,
)
from src.common.etl_metadata import ETLMetadataTracker

load_dotenv()

logger = logging.getLogger(__name__)

# Expected columns in the raw CSV data
EXPECTED_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw")
BRONZE_DATA_PATH = os.getenv("BRONZE_DATA_PATH", "data/bronze")


def read_raw_parquet(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw Parquet file(s) from the specified path.

    Args:
        spark: Active SparkSession.
        input_path: Path to directory containing Parquet file(s).

    Returns:
        Raw DataFrame with original types from Parquet.

    Design Decision:
        Data is downloaded from Google Drive as Parquet via
        gdrive_to_parquet.py (streamed CSV → Parquet with ZSTD).
        Reading Parquet preserves exact types and avoids CSV schema
        inference issues on 68M rows.
    """
    logger.info("Reading Parquet files from: %s", input_path)

    df = spark.read.parquet(input_path)

    logger.info("Raw Parquet loaded: %d columns, schema: %s",
                len(df.columns), df.columns)
    return df


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Add ingestion metadata columns for Bronze layer partitioning.

    Args:
        df: Raw DataFrame from CSV.

    Returns:
        DataFrame with ingestion_date column added.

    Design Decision:
        ingestion_date uses the current date (not event_time) because
        Bronze layer tracks WHEN data was ingested, not when events occurred.
        This supports incremental ingestion patterns and data lineage.
    """
    today = date.today().isoformat()
    df = df.withColumn("ingestion_date", F.lit(today))

    logger.info("Added ingestion_date: %s", today)
    return df


def write_bronze(df: DataFrame, output_path: str, row_count: int) -> int:
    """
    Write DataFrame to Bronze layer as partitioned Parquet.

    Args:
        df: DataFrame with ingestion metadata.
        output_path: Output directory path.
        row_count: Pre-calculated number of rows.

    Returns:
        Number of rows written.

    Design Decision:
        Partitioned by ingestion_date to support incremental loads.
        Dimension tables are NOT partitioned (small), but the Bronze
        events table benefits from partition pruning on date-based queries.
    """
    bronze_path = os.path.join(output_path, "events_raw.parquet")
    logger.info("Writing Bronze layer to: %s", bronze_path)

    df.write.mode("overwrite").partitionBy("ingestion_date").parquet(bronze_path)

    logger.info("Bronze layer written: %s rows", f"{row_count:,}")
    return row_count


def run_ingestion() -> None:
    """
    Execute the full ingestion pipeline (Phase 1).

    Steps:
        1. Create SparkSession with optimized configuration
        2. Read raw CSV files
        3. Validate schema against expected columns
        4. Generate data profiling report
        5. Add ingestion metadata
        6. Write to Bronze layer as partitioned Parquet
        7. Save quality report
    """
    bronze_file = os.path.join(BRONZE_DATA_PATH, "events_raw.parquet")
    raw_exists = os.path.exists(RAW_DATA_PATH) and os.path.isdir(RAW_DATA_PATH) and len(os.listdir(RAW_DATA_PATH)) > 0

    if not raw_exists and os.path.exists(bronze_file):
        logger.info("Raw data is missing but Bronze layer already exists. Skipping ingestion to save disk space.")
        return

    spark = create_spark_session(app_name="Phase1_Ingestion")

    try:
        with ETLMetadataTracker("ingestion") as tracker:
            # Step 1: Read raw data
            df = read_raw_parquet(spark, RAW_DATA_PATH)

            # Step 2: Validate schema
            quality_results = []
            schema_result = validate_schema(df, EXPECTED_COLUMNS, "bronze")
            quality_results.append(schema_result)

            # Step 3: Validate row count (at least 1 row)
            row_result = validate_row_count(df, "bronze", min_rows=1)
            row_count = row_result["actual_count"]
            quality_results.append(row_result)

            # Step 4: Generate profiling report (pass row_count to optimize)
            profile = generate_profiling_report(df, "bronze", row_count=row_count)
            quality_results.append(profile)

            # Step 5: Add ingestion metadata
            df = add_ingestion_metadata(df)

            # Step 6: Write to Bronze layer (pass row_count to optimize)
            row_count = write_bronze(df, BRONZE_DATA_PATH, row_count)
            tracker.set_rows_processed(row_count)

            # Step 7: Save quality report
            save_quality_report(quality_results, "bronze")

            logger.info("Phase 1 (Ingestion) completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/ingestion.log"),
        ],
    )
    os.makedirs("logs", exist_ok=True)
    run_ingestion()
