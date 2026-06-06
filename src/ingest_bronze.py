"""
Phase 1: Data Ingestion (Bronze Layer)
======================================
Reads raw CSV data from data/raw/, validates the schema, generates a
profiling report, and writes to the Bronze layer as partitioned Parquet.

Input:  data/raw/*.csv
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


def read_raw_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw CSV files from the specified path.

    Args:
        spark: Active SparkSession.
        input_path: Path to directory containing CSV files.

    Returns:
        Raw DataFrame with inferred string types.

    Design Decision:
        All columns are read as strings initially (inferSchema=False)
        to prevent type inference errors on 68M rows. Type casting
        happens in the Silver layer (Phase 2).
    """
    logger.info("Reading CSV files from: %s", input_path)

    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=False,  # Read all as strings for Bronze layer safety
        multiLine=False,
    )

    logger.info("Raw CSV loaded: %d columns", len(df.columns))
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


def write_bronze(df: DataFrame, output_path: str) -> int:
    """
    Write DataFrame to Bronze layer as partitioned Parquet.

    Args:
        df: DataFrame with ingestion metadata.
        output_path: Output directory path.

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

    row_count = df.count()
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
    spark = create_spark_session(app_name="Phase1_Ingestion")

    try:
        with ETLMetadataTracker("ingestion") as tracker:
            # Step 1: Read raw data
            df = read_raw_csv(spark, RAW_DATA_PATH)

            # Step 2: Validate schema
            quality_results = []
            schema_result = validate_schema(df, EXPECTED_COLUMNS, "bronze")
            quality_results.append(schema_result)

            # Step 3: Validate row count (at least 1 row)
            row_result = validate_row_count(df, "bronze", min_rows=1)
            quality_results.append(row_result)

            # Step 4: Generate profiling report
            profile = generate_profiling_report(df, "bronze")
            quality_results.append(profile)

            # Step 5: Add ingestion metadata
            df = add_ingestion_metadata(df)

            # Step 6: Write to Bronze layer
            row_count = write_bronze(df, BRONZE_DATA_PATH)
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
