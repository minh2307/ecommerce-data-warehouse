"""
Test Suite: Data Quality Checks
================================
Tests the reusable data quality validation module.

Covers:
    - Row count validation
    - Schema validation
    - Null detection
    - Duplicate detection
    - Primary key validation
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
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.common.data_quality import (
    DataQualityError,
    validate_row_count,
    validate_schema,
    validate_nulls,
    validate_duplicates,
    validate_primary_key,
)


@pytest.fixture(scope="module")
def spark():
    """Create a test SparkSession."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_data_quality")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def good_data(spark):
    """Valid dataset with no quality issues."""
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
        StructField("value", DoubleType(), True),
    ])
    data = [
        (1, "alpha", 10.0),
        (2, "beta", 20.0),
        (3, "gamma", 30.0),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def data_with_nulls(spark):
    """Dataset with null values in critical columns."""
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
    ])
    data = [
        (1, "alpha", 10.0),
        (None, "beta", 20.0),
        (3, None, None),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def data_with_duplicates(spark):
    """Dataset with duplicate rows."""
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
    ])
    data = [
        (1, "alpha"),
        (1, "alpha"),  # duplicate
        (2, "beta"),
    ]
    return spark.createDataFrame(data, schema)


class TestRowCountValidation:
    """Test row count validation logic."""

    def test_valid_row_count(self, good_data):
        """Should pass when row count meets minimum."""
        result = validate_row_count(good_data, "test", min_rows=1)
        assert result["passed"] is True
        assert result["actual_count"] == 3

    def test_insufficient_row_count(self, spark):
        """Should raise DataQualityError when row count is below minimum."""
        empty_df = spark.createDataFrame([], StructType([
            StructField("id", LongType(), False),
        ]))
        with pytest.raises(DataQualityError):
            validate_row_count(empty_df, "test", min_rows=1)


class TestSchemaValidation:
    """Test schema validation logic."""

    def test_valid_schema(self, good_data):
        """Should pass when all expected columns exist."""
        result = validate_schema(good_data, ["id", "name", "value"], "test")
        assert result["passed"] is True
        assert len(result["missing_columns"]) == 0

    def test_missing_columns(self, good_data):
        """Should raise DataQualityError when columns are missing."""
        with pytest.raises(DataQualityError):
            validate_schema(good_data, ["id", "name", "nonexistent"], "test")


class TestNullValidation:
    """Test null value detection."""

    def test_no_nulls(self, good_data):
        """Should pass when no nulls in critical columns."""
        result = validate_nulls(good_data, ["id", "name"], "test", fail_on_nulls=True)
        assert result["passed"] is True

    def test_detect_nulls(self, data_with_nulls):
        """Should detect nulls in critical columns."""
        result = validate_nulls(
            data_with_nulls, ["id", "name"], "test", fail_on_nulls=False
        )
        assert result["passed"] is False
        assert result["null_counts"]["id"] == 1
        assert result["null_counts"]["name"] == 1

    def test_fail_on_nulls(self, data_with_nulls):
        """Should raise DataQualityError when fail_on_nulls is True."""
        with pytest.raises(DataQualityError):
            validate_nulls(data_with_nulls, ["id"], "test", fail_on_nulls=True)


class TestDuplicateValidation:
    """Test duplicate detection."""

    def test_no_duplicates(self, good_data):
        """Should pass when no duplicates exist."""
        result = validate_duplicates(
            good_data, ["id"], "test", fail_on_duplicates=True
        )
        assert result["passed"] is True
        assert result["duplicate_rows"] == 0

    def test_detect_duplicates(self, data_with_duplicates):
        """Should detect duplicate rows."""
        result = validate_duplicates(
            data_with_duplicates, ["id"], "test", fail_on_duplicates=False
        )
        assert result["passed"] is False
        assert result["duplicate_rows"] == 1


class TestPrimaryKeyValidation:
    """Test primary key validation."""

    def test_valid_pk(self, good_data):
        """Should pass for unique, non-null PK."""
        result = validate_primary_key(good_data, "id", "test")
        assert result["passed"] is True
        assert result["is_unique"] is True
        assert result["is_non_null"] is True

    def test_duplicate_pk(self, data_with_duplicates):
        """Should fail for duplicate PK values."""
        with pytest.raises(DataQualityError):
            validate_primary_key(data_with_duplicates, "id", "test")

    def test_null_pk(self, data_with_nulls):
        """Should fail when PK contains nulls."""
        with pytest.raises(DataQualityError):
            validate_primary_key(data_with_nulls, "id", "test")
