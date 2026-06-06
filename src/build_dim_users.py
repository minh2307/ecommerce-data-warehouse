"""
Phase 3: Dimension - dim_users (Gold Layer)
===========================================
Builds the dim_users dimension table from Silver layer events.

Input:  data/silver/events_clean.parquet
Output: data/gold/dim_users.parquet (NOT partitioned — small table)

Architecture Decision:
    dim_users is derived by aggregating Silver events to find each user's
    first interaction. This table is intentionally NOT partitioned because
    dimension tables are relatively small compared to fact tables, and
    partitioning would add unnecessary overhead for broadcast joins.
"""

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.spark_config import create_spark_session
from src.common.data_quality import (
    validate_row_count,
    validate_primary_key,
    save_quality_report,
)
from src.common.etl_metadata import ETLMetadataTracker

load_dotenv()

logger = logging.getLogger(__name__)

SILVER_DATA_PATH = os.getenv("SILVER_DATA_PATH", "data/silver")
GOLD_DATA_PATH = os.getenv("GOLD_DATA_PATH", "data/gold")


def build_dim_users(df: DataFrame) -> DataFrame:
    """
    Build dim_users dimension from Silver events.

    Columns:
        - user_id: Unique user identifier (PK)
        - first_event_ts: Timestamp of the user's first recorded event
        - first_event_date: Date of the user's first recorded event

    Args:
        df: Silver layer events DataFrame.

    Returns:
        dim_users DataFrame.

    Design Decision:
        first_event_ts/first_event_date capture when a user was first
        observed in the system. This supports cohort analysis and
        user lifecycle metrics without requiring a separate user registration table.
    """
    logger.info("Building dim_users...")

    dim_users = (
        df
        .groupBy("user_id")
        .agg(
            F.min("event_time").alias("first_event_ts"),
        )
        .withColumn("first_event_date", F.to_date(F.col("first_event_ts")))
        .select("user_id", "first_event_date", "first_event_ts")
    )

    count = dim_users.count()
    logger.info("dim_users built: %s unique users", f"{count:,}")
    return dim_users


def write_dim_users(df: DataFrame, output_path: str) -> int:
    """
    Write dim_users to Gold layer. NOT partitioned (small dimension table).

    Args:
        df: dim_users DataFrame.
        output_path: Gold layer output directory.

    Returns:
        Number of rows written.
    """
    dim_path = os.path.join(output_path, "dim_users.parquet")
    logger.info("Writing dim_users to: %s", dim_path)

    # No partitioning for dimension tables — they are small and benefit from
    # being read as a single file for broadcast joins.
    df.write.mode("overwrite").parquet(dim_path)

    row_count = df.count()
    logger.info("dim_users written: %s rows", f"{row_count:,}")
    return row_count


def run_dim_users() -> None:
    """
    Execute dim_users generation pipeline.

    Steps:
        1. Read Silver layer events
        2. Build dim_users via aggregation
        3. Validate PK uniqueness
        4. Write to Gold layer
    """
    spark = create_spark_session(app_name="Phase3_DimUsers")

    try:
        with ETLMetadataTracker("dim_users") as tracker:
            # Step 1: Read Silver layer
            silver_path = os.path.join(SILVER_DATA_PATH, "events_clean.parquet")
            logger.info("Reading Silver layer from: %s", silver_path)
            df = spark.read.parquet(silver_path)

            # Step 2: Build dimension
            dim_users = build_dim_users(df)

            # Step 3: Validate
            quality_results = []
            row_result = validate_row_count(dim_users, "dim_users", min_rows=1)
            quality_results.append(row_result)

            pk_result = validate_primary_key(dim_users, "user_id", "dim_users")
            quality_results.append(pk_result)

            # Step 4: Write
            row_count = write_dim_users(dim_users, GOLD_DATA_PATH)
            tracker.set_rows_processed(row_count)

            save_quality_report(quality_results, "dim_users")
            logger.info("Phase 3 (dim_users) completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/dim_users.log"),
        ],
    )
    os.makedirs("logs", exist_ok=True)
    run_dim_users()
