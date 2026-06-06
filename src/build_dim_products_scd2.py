"""
Phase 4: Slowly Changing Dimension Type 2 - dim_products_scd2 (Gold Layer)
==========================================================================
Implements SCD Type 2 for the products dimension to track price changes
over time. Each price change creates a new version of the product record.

Input:  data/silver/events_clean.parquet
Output: data/gold/dim_products_scd2.parquet (NOT partitioned — dimension table)

Architecture Decision:
    SCD Type 2 preserves the full history of product price changes.
    This allows the fact table to join to the correct product version
    based on when an event occurred (temporal join). Without SCD2,
    we would lose price history and all events would reflect only
    the latest price — making revenue analysis inaccurate.

Surrogate Key Strategy:
    Uses xxhash64(product_id, valid_from) to generate deterministic,
    distributed-safe BIGINT surrogate keys. This is preferred over
    monotonically_increasing_id() because:
    - Deterministic: same inputs always produce the same key
    - Reproducible: re-running the pipeline yields identical keys
    - Distributed-safe: no coordination needed between partitions
    - Faster joins: BIGINT comparison is faster than SHA2 string comparison
"""

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession, Window
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


def extract_product_timeline(df: DataFrame) -> DataFrame:
    """
    Extract the timeline of product attribute changes from Silver events.

    For each product, get the records where price, category_id, or brand changes
    chronologically.

    Args:
        df: Silver layer events DataFrame.

    Returns:
        DataFrame with one row per product-attribute change,
        ordered chronologically.
    """
    logger.info("Extracting product attribute timeline...")

    # Define window ordered by event_time per product
    window_spec = Window.partitionBy("product_id").orderBy("event_time")

    # Lag values of attributes
    df_lagged = (
        df
        .withColumn("prev_price", F.lag("price").over(window_spec))
        .withColumn("prev_category_id", F.lag("category_id").over(window_spec))
        .withColumn("prev_brand", F.lag("brand").over(window_spec))
    )

    # A change occurred if it's the first record or any attribute has changed
    df_changes = df_lagged.withColumn(
        "is_change",
        F.when(
            F.col("prev_price").isNull() |
            F.col("prev_category_id").isNull() |
            F.col("prev_brand").isNull() |
            (~F.col("price").eqNullSafe(F.col("prev_price"))) |
            (~F.col("category_id").eqNullSafe(F.col("prev_category_id"))) |
            (~F.col("brand").eqNullSafe(F.col("prev_brand"))),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # Filter to keep only the change events and rename event_time to first_seen
    product_versions = (
        df_changes
        .filter(F.col("is_change") == 1)
        .select(
            "product_id",
            "category_id",
            "brand",
            "price",
            F.col("event_time").alias("first_seen")
        )
    )

    count = product_versions.count()
    logger.info("Product versions extracted: %s records", f"{count:,}")
    return product_versions


def detect_changes_and_build_scd2(df: DataFrame) -> DataFrame:
    """
    Detect price changes using Window + lag() and build SCD Type 2 records.

    Logic:
        1. Order product versions chronologically per product_id
        2. Use lag() to detect when price/brand/category changes between consecutive versions
        3. Create version groups using cumulative sum of change flags
        4. Generate valid_from (start of version validity)
        5. Generate valid_to (end of version validity, NULL for current)
        6. Mark current record with is_current flag

    Args:
        df: Product versions DataFrame from extract_product_timeline.

    Returns:
        SCD Type 2 DataFrame with version tracking columns.
    """
    logger.info("Building SCD Type 2 with change detection...")

    # Step 1: Define window ordered by first_seen timestamp per product
    product_window = Window.partitionBy("product_id").orderBy("first_seen")

    # Step 2: Detect changes using lag()
    df_with_lag = (
        df
        .withColumn("prev_price", F.lag("price").over(product_window))
        .withColumn("prev_category_id", F.lag("category_id").over(product_window))
        .withColumn("prev_brand", F.lag("brand").over(product_window))
    )

    # Step 3: Create change flag
    df_with_changes = df_with_lag.withColumn(
        "is_new_version",
        F.when(
            F.col("prev_price").isNull() |
            F.col("prev_category_id").isNull() |
            F.col("prev_brand").isNull() |
            (~F.col("price").eqNullSafe(F.col("prev_price"))) |
            (~F.col("category_id").eqNullSafe(F.col("prev_category_id"))) |
            (~F.col("brand").eqNullSafe(F.col("prev_brand"))),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # Step 4: Create version groups using cumulative sum
    df_with_groups = df_with_changes.withColumn(
        "version_group",
        F.sum("is_new_version").over(product_window),
    )

    # Step 5: Collapse version groups to get one record per version
    # Take the first occurrence (valid_from) of each version group
    scd2_base = (
        df_with_groups
        .groupBy("product_id", "version_group")
        .agg(
            F.first("category_id").alias("category_id"),
            F.first("brand").alias("brand"),
            F.first("price").alias("price"),
            F.min("first_seen").alias("valid_from"),
        )
    )

    # Step 6: Generate valid_to using lead()
    # valid_to = the valid_from of the NEXT version for this product.
    # NULL valid_to means the version is still current.
    version_window = Window.partitionBy("product_id").orderBy("valid_from")

    scd2_with_validity = scd2_base.withColumn(
        "valid_to",
        F.lead("valid_from").over(version_window),
    )

    # Step 7: Mark current records
    # is_current = True when valid_to IS NULL (latest version)
    scd2_with_current = scd2_with_validity.withColumn(
        "is_current",
        F.when(F.col("valid_to").isNull(), F.lit(True)).otherwise(F.lit(False)),
    )

    # Step 8: Generate deterministic surrogate key using xxhash64
    # xxhash64(product_id, valid_from) produces a BIGINT that is:
    # - Deterministic: same inputs → same key (idempotent pipeline)
    # - Distributed-safe: no global state needed
    # - Fast: BIGINT comparison is faster than string comparison in joins
    scd2_final = scd2_with_current.withColumn(
        "product_sk",
        F.xxhash64(
            F.col("product_id").cast("string"),
            F.col("valid_from").cast("string"),
        ),
    )

    # Step 9: Select final columns in the specified order
    result = scd2_final.select(
        "product_sk",
        "product_id",
        "category_id",
        "brand",
        "price",
        "valid_from",
        "valid_to",
        "is_current",
    )

    count = result.count()
    current_count = result.filter(F.col("is_current")).count()
    logger.info(
        "SCD2 built: %s total versions, %s current products",
        f"{count:,}", f"{current_count:,}",
    )
    return result


def write_dim_products_scd2(df: DataFrame, output_path: str) -> int:
    """
    Write SCD Type 2 dimension to Gold layer.

    NOT partitioned because dimension tables are relatively small
    and need to be broadcast-joined with the large fact table.

    Args:
        df: SCD2 DataFrame.
        output_path: Gold layer output directory.

    Returns:
        Number of rows written.
    """
    dim_path = os.path.join(output_path, "dim_products_scd2.parquet")
    logger.info("Writing dim_products_scd2 to: %s", dim_path)

    df.write.mode("overwrite").parquet(dim_path)

    row_count = df.count()
    logger.info("dim_products_scd2 written: %s rows", f"{row_count:,}")
    return row_count


def run_dim_products_scd2() -> None:
    """
    Execute dim_products_scd2 generation pipeline (Phase 4).

    Steps:
        1. Read Silver layer events
        2. Extract product attribute timeline
        3. Detect changes and build SCD Type 2 records
        4. Validate surrogate key uniqueness
        5. Write to Gold layer
    """
    spark = create_spark_session(app_name="Phase4_SCD2")

    try:
        with ETLMetadataTracker("dim_products_scd2") as tracker:
            silver_path = os.path.join(SILVER_DATA_PATH, "events_clean.parquet")
            logger.info("Reading Silver layer from: %s", silver_path)
            df = spark.read.parquet(silver_path)

            # Extract product timeline
            product_versions = extract_product_timeline(df)

            # Build SCD Type 2
            scd2 = detect_changes_and_build_scd2(product_versions)

            # Validate
            quality_results = []
            row_result = validate_row_count(scd2, "dim_products_scd2", min_rows=1)
            quality_results.append(row_result)

            pk_result = validate_primary_key(scd2, "product_sk", "dim_products_scd2")
            quality_results.append(pk_result)

            # Write
            row_count = write_dim_products_scd2(scd2, GOLD_DATA_PATH)
            tracker.set_rows_processed(row_count)

            save_quality_report(quality_results, "dim_products_scd2")
            logger.info("Phase 4 (SCD Type 2) completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/dim_products_scd2.log"),
        ],
    )
    os.makedirs("logs", exist_ok=True)
    run_dim_products_scd2()
