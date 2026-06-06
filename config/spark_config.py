"""
Spark Configuration Module
==========================
Centralized SparkSession factory with performance tuning optimized
for processing ~68 million rows of web analytics data in local mode.

Architecture Decision:
    All Spark configurations are centralized here so that every module
    in the pipeline uses the same, consistently-tuned SparkSession.
    Settings are loaded from environment variables to support different
    deployment environments (dev, staging, production) without code changes.
"""

import os
import logging
from typing import Optional

from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def get_spark_config() -> dict[str, str]:
    """
    Build Spark configuration dictionary from environment variables.

    Returns:
        Dictionary of Spark configuration key-value pairs.

    Design Decisions:
        - AQE (Adaptive Query Execution): Enabled to let Spark dynamically
          optimize shuffle partitions, join strategies, and skew handling
          at runtime. Critical for 68M rows where data distribution is uneven.
        - Kryo Serializer: ~10x faster than Java serialization for shuffles.
        - ZSTD Compression: Best compression ratio for Parquet while maintaining
          good read/write speed. Reduces storage by ~30-50% vs Snappy.
        - Shuffle Partitions: Set to 200 (default) but AQE will coalesce
          small partitions automatically. Override via env var if needed.
        - Max Partition Bytes: 128MB per partition keeps individual tasks
          manageable in memory while minimizing task scheduling overhead.
    """
    return {
        # --- Application Identity ---
        "spark.app.name": os.getenv("SPARK_APP_NAME", "WebAnalyticsETL"),
        "spark.master": os.getenv("SPARK_MASTER", "local[*]"),

        # --- Memory Configuration ---
        # 4GB driver memory handles the 68M row dataset comfortably in local mode.
        # Each row ~200 bytes → ~13GB uncompressed → fits in memory with spill.
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
        "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "4"),

        # --- Adaptive Query Execution (AQE) ---
        # AQE dynamically adjusts query plans at runtime based on actual data statistics.
        # Essential for optimizing joins and aggregations on 68M rows.
        "spark.sql.adaptive.enabled": "true",
        # Automatically coalesces small shuffle partitions to reduce overhead.
        "spark.sql.adaptive.coalescePartitions.enabled": "true",

        # --- Shuffle Configuration ---
        # 200 initial shuffle partitions; AQE will merge small ones automatically.
        # For 68M rows, this provides a good balance between parallelism and overhead.
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"),

        # --- Serialization ---
        # Kryo is significantly faster and more compact than Java serialization.
        # Critical for shuffle-heavy operations (joins, groupBy, window functions).
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",

        # --- I/O Optimization ---
        # 128MB max partition size when reading files.
        # Balances parallelism with per-task memory usage.
        "spark.sql.files.maxPartitionBytes": os.getenv(
            "SPARK_MAX_PARTITION_BYTES", "134217728"
        ),

        # --- Parquet Configuration ---
        # ZSTD provides best compression ratio for analytics workloads.
        # ~30-50% smaller files than Snappy with comparable read speed.
        "spark.sql.parquet.compression.codec": "zstd",

        # --- Additional Optimizations ---
        # Broadcast join threshold: 10MB (default). Tables smaller than this
        # are broadcast to all executors to avoid expensive shuffle joins.
        "spark.sql.autoBroadcastJoinThreshold": "10485760",

        # Enable predicate pushdown for Parquet to skip irrelevant row groups.
        "spark.sql.parquet.filterPushdown": "true",

        # --- Local Scratch Directory ---
        # Set local scratch directory to a folder inside the mounted /app/data volume
        # to prevent filling up the root partition '/' (which has very limited space).
        "spark.local.dir": "/app/data/tmp",
    }


def create_spark_session(
    app_name: Optional[str] = None,
    extra_config: Optional[dict[str, str]] = None,
) -> SparkSession:
    """
    Create and return a configured SparkSession.

    Args:
        app_name: Optional override for the Spark application name.
        extra_config: Optional additional Spark configurations to merge.

    Returns:
        Configured SparkSession instance.

    Example:
        >>> spark = create_spark_session()
        >>> df = spark.read.parquet("data/bronze/events_raw.parquet")
    """
    config = get_spark_config()

    if app_name:
        config["spark.app.name"] = app_name

    if extra_config:
        config.update(extra_config)

    builder = SparkSession.builder
    for key, value in config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Set log level to reduce Spark's verbose output
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession created: app=%s, master=%s, driver_memory=%s",
        config.get("spark.app.name"),
        config.get("spark.master"),
        config.get("spark.driver.memory"),
    )

    return spark
