"""
Test Suite: SCD Type 2 Logic
=============================
Tests the core SCD2 change detection and version management logic
using realistic sample datasets.

Covers:
    - Price change detection via Window + lag()
    - Version group creation
    - valid_from / valid_to generation
    - Surrogate key determinism (xxhash64)
    - Current record marking (is_current flag)
"""

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.build_dim_products_scd2 import (
    detect_changes_and_build_scd2,
    extract_product_timeline,
)


@pytest.fixture(scope="module")
def spark():
    """Create a test SparkSession."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_scd2")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_events(spark):
    """
    Create a realistic sample dataset simulating product price changes.

    Product 101: price changes from 29.99 → 34.99 → 29.99
    Product 202: price stays at 99.99 (no changes)
    """
    schema = StructType([
        StructField("product_id", LongType(), False),
        StructField("category_id", LongType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("event_type", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ])

    data = [
        # Product 101: 3 price versions
        (101, 1, "apple", 29.99, datetime(2024, 1, 1, 10, 0), "view", 1, "s1"),
        (101, 1, "apple", 29.99, datetime(2024, 1, 2, 11, 0), "cart", 2, "s2"),
        (101, 1, "apple", 34.99, datetime(2024, 2, 1, 12, 0), "view", 3, "s3"),
        (101, 1, "apple", 34.99, datetime(2024, 2, 15, 13, 0), "purchase", 1, "s4"),
        (101, 1, "apple", 29.99, datetime(2024, 3, 1, 14, 0), "view", 4, "s5"),
        # Product 202: 1 price version (no changes)
        (202, 2, "samsung", 99.99, datetime(2024, 1, 5, 10, 0), "view", 5, "s6"),
        (202, 2, "samsung", 99.99, datetime(2024, 2, 10, 11, 0), "cart", 6, "s7"),
        (202, 2, "samsung", 99.99, datetime(2024, 3, 15, 12, 0), "purchase", 7, "s8"),
    ]

    return spark.createDataFrame(data, schema)


class TestSCD2Logic:
    """Test SCD Type 2 change detection and version management."""

    def test_extract_product_timeline(self, spark, sample_events):
        """Test that product timeline extraction produces correct versions."""
        timeline = extract_product_timeline(sample_events)

        # Product 101 should have 3 unique price points
        product_101 = timeline.filter(F.col("product_id") == 101)
        assert product_101.count() == 3, "Product 101 should have 3 price versions"

        # Product 202 should have 1 price point (no changes)
        product_202 = timeline.filter(F.col("product_id") == 202)
        assert product_202.count() == 1, "Product 202 should have 1 price version"

    def test_scd2_version_count(self, spark, sample_events):
        """Test that SCD2 generates correct number of versions."""
        timeline = extract_product_timeline(sample_events)
        scd2 = detect_changes_and_build_scd2(timeline)

        # Product 101: 29.99 → 34.99 → 29.99 = 3 versions
        p101_versions = scd2.filter(F.col("product_id") == 101).count()
        assert p101_versions == 3, f"Expected 3 versions for product 101, got {p101_versions}"

        # Product 202: stable price = 1 version
        p202_versions = scd2.filter(F.col("product_id") == 202).count()
        assert p202_versions == 1, f"Expected 1 version for product 202, got {p202_versions}"

    def test_scd2_current_record(self, spark, sample_events):
        """Test that exactly one record per product is marked as current."""
        timeline = extract_product_timeline(sample_events)
        scd2 = detect_changes_and_build_scd2(timeline)

        for pid in [101, 202]:
            current = scd2.filter(
                (F.col("product_id") == pid) & (F.col("is_current") == True)
            ).count()
            assert current == 1, f"Product {pid} should have exactly 1 current record"

    def test_scd2_valid_to_null_for_current(self, spark, sample_events):
        """Test that current records have NULL valid_to."""
        timeline = extract_product_timeline(sample_events)
        scd2 = detect_changes_and_build_scd2(timeline)

        current_records = scd2.filter(F.col("is_current") == True)
        null_valid_to = current_records.filter(F.col("valid_to").isNull()).count()
        total_current = current_records.count()

        assert null_valid_to == total_current, "All current records must have NULL valid_to"

    def test_scd2_valid_from_before_valid_to(self, spark, sample_events):
        """Test that valid_from < valid_to for non-current records."""
        timeline = extract_product_timeline(sample_events)
        scd2 = detect_changes_and_build_scd2(timeline)

        non_current = scd2.filter(F.col("is_current") == False)
        invalid = non_current.filter(F.col("valid_from") >= F.col("valid_to")).count()

        assert invalid == 0, "valid_from must be before valid_to for non-current records"

    def test_scd2_surrogate_key_uniqueness(self, spark, sample_events):
        """Test that product_sk (xxhash64) values are unique."""
        timeline = extract_product_timeline(sample_events)
        scd2 = detect_changes_and_build_scd2(timeline)

        total = scd2.count()
        distinct_sk = scd2.select("product_sk").distinct().count()

        assert total == distinct_sk, "All product_sk values must be unique"

    def test_scd2_surrogate_key_determinism(self, spark, sample_events):
        """Test that running SCD2 twice produces identical surrogate keys."""
        timeline = extract_product_timeline(sample_events)
        scd2_run1 = detect_changes_and_build_scd2(timeline)
        scd2_run2 = detect_changes_and_build_scd2(timeline)

        keys_run1 = sorted(
            [row.product_sk for row in scd2_run1.select("product_sk").collect()]
        )
        keys_run2 = sorted(
            [row.product_sk for row in scd2_run2.select("product_sk").collect()]
        )

        assert keys_run1 == keys_run2, "Surrogate keys must be deterministic across runs"

    def test_scd2_price_values(self, spark, sample_events):
        """Test that price values are correctly captured in each version."""
        timeline = extract_product_timeline(sample_events)
        scd2 = detect_changes_and_build_scd2(timeline)

        prices = sorted(
            [row.price for row in scd2.filter(F.col("product_id") == 101)
             .orderBy("valid_from").select("price").collect()]
        )
        # Product 101 prices: 29.99, 29.99, 34.99 (sorted)
        assert 29.99 in prices, "Price 29.99 should be present"
        assert 34.99 in prices, "Price 34.99 should be present"
