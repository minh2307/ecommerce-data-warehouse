"""
Phase 3/4: Fact Table - fact_events (Gold Layer)
================================================
Builds the central fact table by joining Silver events with the
SCD Type 2 products dimension to resolve the correct product version.

Input:
    - data/silver/events_clean.parquet
    - data/gold/dim_products_scd2.parquet
Output:
    - data/gold/fact_events.parquet (partitioned by event_date)

Architecture Decision:
    The fact table uses a temporal join to link each event to the correct
    product version based on when the event occurred (event_time).
    This ensures accurate price_at_event values for revenue analysis.

Event ID Strategy:
    Uses sha2(user_id || product_id || event_time) for deterministic
    event_id generation. SHA2 is used (instead of xxhash64) because
    event_id is a STRING identifier used for deduplication and auditing,
    not for join performance like product_sk.
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
    validate_nulls,
    save_quality_report,
)
from src.common.etl_metadata import ETLMetadataTracker

load_dotenv()

logger = logging.getLogger(__name__)

SILVER_DATA_PATH = os.getenv("SILVER_DATA_PATH", "data/silver")
GOLD_DATA_PATH = os.getenv("GOLD_DATA_PATH", "data/gold")


def generate_event_id(df: DataFrame) -> DataFrame:
    """
    Generate deterministic event_id using SHA2 hash.

    Args:
        df: Events DataFrame with user_id, product_id, event_time.

    Returns:
        DataFrame with event_id column added.

    Design Decision:
        SHA2-256 hash of (user_id, product_id, event_time) creates a
        deterministic, collision-resistant identifier. This ensures:
        - Idempotency: re-running produces identical IDs
        - Deduplication: identical events get the same ID
        - Auditability: ID can be verified from source data
    """
    logger.info("Generating deterministic event_id...")

    df = df.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("user_id").cast("string"),
                F.col("product_id").cast("string"),
                F.col("event_time").cast("string"),
            ),
            256,
        ),
    )

    return df


def resolve_product_sk(
    events_df: DataFrame,
    scd2_df: DataFrame,
) -> DataFrame:
    """
    Join events with SCD2 products dimension to resolve the correct
    product_sk based on temporal validity.

    Join Condition:
        events.product_id = scd2.product_id
        AND events.event_time >= scd2.valid_from
        AND (events.event_time < scd2.valid_to OR scd2.valid_to IS NULL)

    The IS NULL check on valid_to handles current records (latest version).

    Args:
        events_df: Silver events DataFrame.
        scd2_df: SCD Type 2 products dimension DataFrame.

    Returns:
        Events joined with product_sk.

    Design Decision:
        Left join is used to preserve all events, even those without
        a matching product version (which would indicate a data quality
        issue worth investigating). Unmatched events get NULL product_sk.
    """
    logger.info("Resolving product_sk via temporal join with SCD2...")

    # Rename SCD2 columns to avoid ambiguity after join
    scd2_renamed = scd2_df.select(
        F.col("product_sk"),
        F.col("product_id").alias("scd2_product_id"),
        F.col("valid_from"),
        F.col("valid_to"),
    )

    # Temporal join: find the product version that was active at event_time
    # valid_to IS NULL means the record is still current (latest version)
    joined = events_df.join(
        scd2_renamed,
        on=(
            (events_df["product_id"] == scd2_renamed["scd2_product_id"])
            & (events_df["event_time"] >= scd2_renamed["valid_from"])
            & (
                (events_df["event_time"] < scd2_renamed["valid_to"])
                | scd2_renamed["valid_to"].isNull()
            )
        ),
        how="left",
    )

    # Drop temporary join columns
    joined = joined.drop("scd2_product_id", "valid_from", "valid_to")

    # Log join quality
    total = joined.count()
    unmatched = joined.filter(F.col("product_sk").isNull()).count()
    if unmatched > 0:
        logger.warning(
            "Temporal join: %s/%s events have no matching product version",
            f"{unmatched:,}", f"{total:,}",
        )
    else:
        logger.info("Temporal join: all %s events matched", f"{total:,}")

    return joined


def build_fact_events(df: DataFrame) -> DataFrame:
    """
    Select and rename columns for the final fact_events table.

    Columns:
        - event_id: Deterministic hash-based unique identifier
        - user_id: Reference to dim_users
        - product_sk: Reference to dim_products_scd2 (temporal FK)
        - event_time: Timestamp of the event
        - event_type: Type of event (view, cart, purchase)
        - price_at_event: Price at the time of event
        - user_session: Session identifier
        - event_date: Partition column (date only)

    Args:
        df: Joined DataFrame with product_sk resolved.

    Returns:
        Final fact_events DataFrame.
    """
    logger.info("Building fact_events...")

    fact = df.select(
        "event_id",
        "user_id",
        "product_sk",
        "event_time",
        "event_type",
        F.col("price").alias("price_at_event"),
        "user_session",
        "event_date",
    )

    count = fact.count()
    logger.info("fact_events built: %s rows", f"{count:,}")
    return fact


def write_fact_events(df: DataFrame, output_path: str) -> int:
    """
    Write fact_events to Gold layer, partitioned by event_date.

    Args:
        df: fact_events DataFrame.
        output_path: Gold layer output directory.

    Returns:
        Number of rows written.

    Design Decision:
        Partitioned by event_date because:
        - Most analytical queries filter by date range
        - Partition pruning dramatically reduces I/O for time-based queries
        - Aligns with the Silver layer partitioning for consistency
    """
    fact_path = os.path.join(output_path, "fact_events.parquet")
    logger.info("Writing fact_events to: %s", fact_path)

    df.write.mode("overwrite").partitionBy("event_date").parquet(fact_path)

    row_count = df.count()
    logger.info("fact_events written: %s rows", f"{row_count:,}")
    return row_count


def run_fact_events() -> None:
    """
    Execute fact_events generation pipeline.

    Steps:
        1. Read Silver layer events
        2. Read SCD2 products dimension
        3. Generate deterministic event_id
        4. Resolve product_sk via temporal join
        5. Build final fact table
        6. Validate and write to Gold layer
    """
    spark = create_spark_session(app_name="Phase4_FactEvents")

    try:
        with ETLMetadataTracker("fact_events") as tracker:
            # Step 1: Read Silver layer
            silver_path = os.path.join(SILVER_DATA_PATH, "events_clean.parquet")
            logger.info("Reading Silver layer from: %s", silver_path)
            events = spark.read.parquet(silver_path)

            # Step 2: Read SCD2 dimension
            scd2_path = os.path.join(GOLD_DATA_PATH, "dim_products_scd2.parquet")
            logger.info("Reading SCD2 dimension from: %s", scd2_path)
            scd2 = spark.read.parquet(scd2_path)

            # Step 3: Generate event_id
            events = generate_event_id(events)

            # Step 4: Resolve product_sk
            events = resolve_product_sk(events, scd2)

            # Step 5: Build fact table
            fact = build_fact_events(events)

            # Step 6: Validate
            quality_results = []
            row_result = validate_row_count(fact, "fact_events", min_rows=1)
            quality_results.append(row_result)

            null_result = validate_nulls(
                fact,
                critical_columns=["event_id", "user_id", "event_time"],
                stage_name="fact_events",
                fail_on_nulls=True,
            )
            quality_results.append(null_result)

            # Write
            row_count = write_fact_events(fact, GOLD_DATA_PATH)
            tracker.set_rows_processed(row_count)

            save_quality_report(quality_results, "fact_events")
            logger.info("Fact events pipeline completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/fact_events.log"),
        ],
    )
    os.makedirs("logs", exist_ok=True)
    run_fact_events()
