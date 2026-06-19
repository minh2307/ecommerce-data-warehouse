"""
Phase 3: Dimension - dim_categories (Gold Layer)
=================================================
Builds the dim_categories dimension table from Silver layer events.

Input:  data/silver/events_clean.parquet
Output: data/gold/dim_categories.parquet (NOT partitioned — small table)

Architecture Decision:
    Categories are extracted from the category_code column, which contains
    hierarchical category paths (e.g., "electronics.smartphone.apple").
    We split this into category_l1 (top-level) and category_l2 (sub-level)
    to support both high-level and detailed category analysis.
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


def build_dim_categories(df: DataFrame) -> DataFrame:
    """
    Build dim_categories dimension from Silver events.

    Columns:
        - category_id: Original category identifier (PK)
        - category_code: Full category path (e.g., "electronics.smartphone")
        - category_l1: Top-level category (e.g., "electronics")
        - category_l2: Second-level category (e.g., "smartphone")

    Args:
        df: Silver layer events DataFrame.

    Returns:
        dim_categories DataFrame.

    Design Decision:
        category_code is split using "." delimiter to create a 2-level
        hierarchy. This avoids storing deeply nested hierarchies while
        supporting the most common analytical groupings (department → subcategory).
        "unknown" category_code produces "unknown" for both levels.
    """
    logger.info("Building dim_categories...")

    dim_categories = (
        df
        .select("category_id", "category_code")
        .distinct()
        # Split category_code into hierarchical levels
        # e.g., "electronics.smartphone" → l1="electronics", l2="smartphone"
        .withColumn(
            "category_l1",
            F.coalesce(
                F.split(F.col("category_code"), r"\.").getItem(0),
                F.lit("unknown"),
            )
        )
        .withColumn(
            "category_l2",
            F.coalesce(
                F.split(F.col("category_code"), r"\.").getItem(1),
                F.lit("unknown"),
            )
        )
        .select("category_id", "category_code", "category_l1", "category_l2")
    )
    return dim_categories


def write_dim_categories(df: DataFrame, output_path: str) -> tuple[DataFrame, int]:
    """
    Write dim_categories to Gold layer. NOT partitioned (small dimension table).

    Args:
        df: dim_categories DataFrame.
        output_path: Gold layer output directory.

    Returns:
        Tuple of (df_written, row_count).
    """
    dim_path = os.path.join(output_path, "dim_categories.parquet")
    logger.info("Writing dim_categories to: %s", dim_path)

    df.write.mode("overwrite").parquet(dim_path)

    df_written = df.sparkSession.read.parquet(dim_path)
    row_count = df_written.count()
    logger.info("dim_categories written: %s rows", f"{row_count:,}")
    return df_written, row_count



def run_dim_categories() -> None:
    """
    Execute dim_categories generation pipeline.

    Steps:
        1. Read Silver layer events
        2. Build dim_categories via distinct + split
        3. Validate PK uniqueness
        4. Write to Gold layer
    """
    spark = create_spark_session(app_name="Phase3_DimCategories")

    try:
        with ETLMetadataTracker("dim_categories") as tracker:
            silver_path = os.path.join(SILVER_DATA_PATH, "events_clean.parquet")
            logger.info("Reading Silver layer from: %s", silver_path)
            df = spark.read.parquet(silver_path)

            dim_categories = build_dim_categories(df)

            # Step 4: Write first to avoid repeating aggregation shuffles
            dim_categories_written, row_count = write_dim_categories(dim_categories, GOLD_DATA_PATH)
            tracker.set_rows_processed(row_count)

            # Step 3: Validate
            quality_results = []
            row_result = validate_row_count(dim_categories_written, "dim_categories", min_rows=1)
            quality_results.append(row_result)

            pk_result = validate_primary_key(dim_categories_written, "category_id", "dim_categories")
            quality_results.append(pk_result)

            save_quality_report(quality_results, "dim_categories")
            logger.info("Phase 3 (dim_categories) completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/dim_categories.log"),
        ],
    )
    os.makedirs("logs", exist_ok=True)
    run_dim_categories()
