"""
Test Suite: 3NF Integrity
=========================
Validates referential integrity between dimension and fact tables
in the 3NF data warehouse (Gold layer).

Covers:
    - dim_users PK uniqueness
    - dim_categories PK uniqueness and hierarchy parsing
    - dim_products_scd2 → dim_categories FK relationship
    - fact_events → dim_products_scd2 temporal FK relationship
    - fact_events → dim_users FK relationship
"""

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def spark():
    """Create a test SparkSession."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_3nf")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def dim_users(spark):
    """Sample dim_users data."""
    schema = StructType([
        StructField("user_id", LongType(), False),
        StructField("first_event_date", StringType(), True),
        StructField("first_event_ts", TimestampType(), True),
    ])
    data = [
        (1, "2024-01-01", datetime(2024, 1, 1, 10, 0)),
        (2, "2024-01-02", datetime(2024, 1, 2, 11, 0)),
        (3, "2024-02-01", datetime(2024, 2, 1, 12, 0)),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dim_categories(spark):
    """Sample dim_categories data."""
    schema = StructType([
        StructField("category_id", LongType(), False),
        StructField("category_code", StringType(), True),
        StructField("category_l1", StringType(), True),
        StructField("category_l2", StringType(), True),
    ])
    data = [
        (1, "electronics.smartphone", "electronics", "smartphone"),
        (2, "clothing.shoes", "clothing", "shoes"),
        (3, "unknown", "unknown", "unknown"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dim_products_scd2(spark):
    """Sample SCD2 products dimension."""
    schema = StructType([
        StructField("product_sk", LongType(), False),
        StructField("product_id", LongType(), False),
        StructField("category_id", LongType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ])
    data = [
        (1001, 101, 1, "apple", 29.99, datetime(2024, 1, 1), datetime(2024, 2, 1), False),
        (1002, 101, 1, "apple", 34.99, datetime(2024, 2, 1), None, True),
        (2001, 202, 2, "nike", 99.99, datetime(2024, 1, 5), None, True),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def fact_events(spark):
    """Sample fact_events data."""
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", LongType(), False),
        StructField("product_sk", LongType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("event_type", StringType(), True),
        StructField("price_at_event", DoubleType(), True),
        StructField("user_session", StringType(), True),
    ])
    data = [
        ("e1", 1, 1001, datetime(2024, 1, 15), "view", 29.99, "s1"),
        ("e2", 2, 1002, datetime(2024, 2, 15), "cart", 34.99, "s2"),
        ("e3", 3, 2001, datetime(2024, 1, 10), "purchase", 99.99, "s3"),
    ]
    return spark.createDataFrame(data, schema)


class TestDimensionIntegrity:
    """Test dimension table primary key constraints."""

    def test_dim_users_pk_unique(self, dim_users):
        """dim_users.user_id must be unique."""
        total = dim_users.count()
        distinct = dim_users.select("user_id").distinct().count()
        assert total == distinct, "dim_users.user_id must be unique"

    def test_dim_users_pk_non_null(self, dim_users):
        """dim_users.user_id must not contain nulls."""
        null_count = dim_users.filter(F.col("user_id").isNull()).count()
        assert null_count == 0, "dim_users.user_id must not contain nulls"

    def test_dim_categories_pk_unique(self, dim_categories):
        """dim_categories.category_id must be unique."""
        total = dim_categories.count()
        distinct = dim_categories.select("category_id").distinct().count()
        assert total == distinct, "dim_categories.category_id must be unique"

    def test_dim_categories_hierarchy(self, dim_categories):
        """Category hierarchy L1/L2 should not be null."""
        null_l1 = dim_categories.filter(F.col("category_l1").isNull()).count()
        null_l2 = dim_categories.filter(F.col("category_l2").isNull()).count()
        assert null_l1 == 0, "category_l1 must not be null"
        assert null_l2 == 0, "category_l2 must not be null"

    def test_scd2_pk_unique(self, dim_products_scd2):
        """dim_products_scd2.product_sk must be unique."""
        total = dim_products_scd2.count()
        distinct = dim_products_scd2.select("product_sk").distinct().count()
        assert total == distinct, "product_sk must be unique"

    def test_scd2_one_current_per_product(self, dim_products_scd2):
        """Each product_id should have exactly one current record."""
        current_counts = (
            dim_products_scd2
            .filter(F.col("is_current") == True)
            .groupBy("product_id")
            .count()
        )
        multi_current = current_counts.filter(F.col("count") > 1).count()
        assert multi_current == 0, "Each product must have at most 1 current record"


class TestReferentialIntegrity:
    """Test foreign key relationships between fact and dimension tables."""

    def test_fact_user_fk(self, fact_events, dim_users):
        """All fact_events.user_id must exist in dim_users."""
        orphan_users = (
            fact_events
            .join(dim_users, on="user_id", how="left_anti")
            .count()
        )
        assert orphan_users == 0, "All fact user_ids must exist in dim_users"

    def test_fact_product_sk_fk(self, fact_events, dim_products_scd2):
        """All non-null fact_events.product_sk must exist in dim_products_scd2."""
        orphan_products = (
            fact_events
            .filter(F.col("product_sk").isNotNull())
            .join(dim_products_scd2, on="product_sk", how="left_anti")
            .count()
        )
        assert orphan_products == 0, "All fact product_sk must exist in dim_products_scd2"

    def test_scd2_category_fk(self, dim_products_scd2, dim_categories):
        """All dim_products_scd2.category_id must exist in dim_categories."""
        orphan_categories = (
            dim_products_scd2
            .filter(F.col("category_id").isNotNull())
            .join(dim_categories, on="category_id", how="left_anti")
            .count()
        )
        assert orphan_categories == 0, "All SCD2 category_ids must exist in dim_categories"

    def test_fact_event_id_unique(self, fact_events):
        """fact_events.event_id must be unique."""
        total = fact_events.count()
        distinct = fact_events.select("event_id").distinct().count()
        assert total == distinct, "fact_events.event_id must be unique"
