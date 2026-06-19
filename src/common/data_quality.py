"""
Data Quality Validation Module
==============================
Reusable data quality checks used across all ETL stages.
Generates quality_report.json for each stage and raises exceptions
when critical validations fail.

Architecture Decision:
    Centralized in src/common/ so every ETL step (ingest, clean, warehouse)
    imports the same validation logic. This ensures consistent quality gates
    throughout the Medallion Architecture pipeline.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

METADATA_PATH = os.getenv("METADATA_PATH", "metadata")


class DataQualityError(Exception):
    """Raised when a critical data quality check fails."""
    pass


def validate_row_count(
    df: DataFrame,
    stage_name: str,
    min_rows: int = 1,
) -> dict[str, Any]:
    """
    Validate that the DataFrame has at least min_rows rows.

    Args:
        df: PySpark DataFrame to validate.
        stage_name: Name of the ETL stage (e.g., 'bronze', 'silver').
        min_rows: Minimum expected row count.

    Returns:
        Validation result dictionary.

    Raises:
        DataQualityError: If row count is below minimum.
    """
    count = df.count()
    passed = count >= min_rows
    result = {
        "check": "row_count",
        "stage": stage_name,
        "actual_count": count,
        "min_expected": min_rows,
        "passed": passed,
    }

    if not passed:
        msg = f"[{stage_name}] Row count {count:,} is below minimum {min_rows:,}"
        logger.error(msg)
        raise DataQualityError(msg)

    logger.info("[%s] Row count validation passed: %s rows", stage_name, f"{count:,}")
    return result


def validate_schema(
    df: DataFrame,
    expected_columns: list[str],
    stage_name: str,
) -> dict[str, Any]:
    """
    Validate that the DataFrame contains all expected columns.

    Args:
        df: PySpark DataFrame to validate.
        expected_columns: List of column names that must be present.
        stage_name: Name of the ETL stage.

    Returns:
        Validation result dictionary.

    Raises:
        DataQualityError: If required columns are missing.
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set

    passed = len(missing) == 0
    result = {
        "check": "schema_validation",
        "stage": stage_name,
        "expected_columns": sorted(expected_columns),
        "actual_columns": sorted(df.columns),
        "missing_columns": sorted(missing),
        "extra_columns": sorted(extra),
        "passed": passed,
    }

    if not passed:
        msg = f"[{stage_name}] Missing columns: {sorted(missing)}"
        logger.error(msg)
        raise DataQualityError(msg)

    logger.info("[%s] Schema validation passed: %d columns", stage_name, len(actual_columns))
    return result


def validate_nulls(
    df: DataFrame,
    critical_columns: list[str],
    stage_name: str,
    fail_on_nulls: bool = True,
) -> dict[str, Any]:
    """
    Check for null values in critical columns using a single optimized scan.
    """
    null_counts = {}
    valid_cols = [c for c in critical_columns if c in df.columns]
    
    if valid_cols:
        agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in valid_cols]
        agg_row = df.agg(*agg_exprs).collect()[0]
        for c in valid_cols:
            val = agg_row[c]
            null_counts[c] = int(val) if val is not None else 0
    
    has_nulls = any(v > 0 for v in null_counts.values())
    result = {
        "check": "null_validation",
        "stage": stage_name,
        "null_counts": null_counts,
        "passed": not has_nulls,
    }

    if has_nulls:
        null_summary = {k: v for k, v in null_counts.items() if v > 0}
        msg = f"[{stage_name}] Nulls found in critical columns: {null_summary}"
        logger.warning(msg)
        if fail_on_nulls:
            raise DataQualityError(msg)

    logger.info("[%s] Null validation completed", stage_name)
    return result


def validate_duplicates(
    df: DataFrame,
    key_columns: list[str],
    stage_name: str,
    fail_on_duplicates: bool = True,
) -> dict[str, Any]:
    """
    Check for duplicate rows based on key columns.

    Args:
        df: PySpark DataFrame to validate.
        key_columns: Columns that form the unique key.
        stage_name: Name of the ETL stage.
        fail_on_duplicates: If True, raise error when duplicates are found.

    Returns:
        Validation result with duplicate count.
    """
    total_count = df.count()
    distinct_count = df.select(key_columns).distinct().count()
    duplicate_count = total_count - distinct_count

    passed = duplicate_count == 0
    result = {
        "check": "duplicate_validation",
        "stage": stage_name,
        "total_rows": total_count,
        "distinct_rows": distinct_count,
        "duplicate_rows": duplicate_count,
        "key_columns": key_columns,
        "passed": passed,
    }

    if not passed:
        msg = f"[{stage_name}] {duplicate_count:,} duplicates found on keys {key_columns}"
        logger.warning(msg)
        if fail_on_duplicates:
            raise DataQualityError(msg)

    logger.info("[%s] Duplicate validation completed", stage_name)
    return result


def validate_primary_key(
    df: DataFrame,
    pk_column: str,
    stage_name: str,
) -> dict[str, Any]:
    """
    Validate that a primary key column is unique and non-null.

    Args:
        df: PySpark DataFrame to validate.
        pk_column: Primary key column name.
        stage_name: Name of the ETL stage.

    Returns:
        Validation result dictionary.

    Raises:
        DataQualityError: If PK constraint is violated.
    """
    total = df.count()
    distinct = df.select(pk_column).distinct().count()
    null_count = df.filter(F.col(pk_column).isNull()).count()

    is_unique = total == distinct
    is_non_null = null_count == 0
    passed = is_unique and is_non_null

    result = {
        "check": "primary_key_validation",
        "stage": stage_name,
        "column": pk_column,
        "total_rows": total,
        "distinct_values": distinct,
        "null_count": null_count,
        "is_unique": is_unique,
        "is_non_null": is_non_null,
        "passed": passed,
    }

    if not passed:
        msg = (
            f"[{stage_name}] PK violation on '{pk_column}': "
            f"unique={is_unique}, non_null={is_non_null}"
        )
        logger.error(msg)
        raise DataQualityError(msg)

    logger.info("[%s] Primary key '%s' validation passed", stage_name, pk_column)
    return result


def generate_profiling_report(
    df: DataFrame,
    stage_name: str,
    row_count: Optional[int] = None,
) -> dict[str, Any]:
    """
    Generate a data profiling report with row count, null counts,
    distinct counts, and data types for every column.
    Uses a single optimized scan to support massive datasets.
    """
    if row_count is None:
        row_count = df.count()
    column_profiles = []

    if row_count == 0:
        for field in df.schema.fields:
            column_profiles.append({
                "column": field.name,
                "data_type": str(field.dataType),
                "null_count": 0,
                "null_pct": 0.0,
                "non_null_pct": 0.0,
                "distinct_count": 0,
            })
    else:
        # For huge datasets, sample data for profiling to prevent memory and disk space overflow.
        # approx_count_distinct requires heavy shuffles and spills to disk.
        sampled = False
        sample_fraction = 1.0
        profile_df = df

        if row_count > 2000000:
            sampled = True
            sample_fraction = 0.05
            logger.info(
                "[%s] Dataset is large (%s rows). Sampling %d%% for profiling to save memory/disk space.",
                stage_name, f"{row_count:,}", int(sample_fraction * 100)
            )
            profile_df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

        # Build a single aggregation query for nulls and approx distinct counts
        agg_exprs = []
        for field in profile_df.schema.fields:
            col_name = field.name
            agg_exprs.append(F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"{col_name}_null_count"))
            agg_exprs.append(F.approx_count_distinct(col_name).alias(f"{col_name}_distinct_count"))

        # Run aggregation in one scan
        agg_row = profile_df.agg(*agg_exprs).collect()[0]

        for field in profile_df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)

            null_count = agg_row[f"{col_name}_null_count"]
            null_count = int(null_count) if null_count is not None else 0

            distinct_count = agg_row[f"{col_name}_distinct_count"]
            distinct_count = int(distinct_count) if distinct_count is not None else 0

            if sampled:
                # Scale back counts to estimate overall numbers
                null_count = min(int(null_count / sample_fraction), row_count)
                distinct_count = min(int(distinct_count / sample_fraction), row_count)

            null_pct = round(null_count / row_count * 100, 2)
            non_null_pct = round((1 - null_count / row_count) * 100, 2)

            column_profiles.append({
                "column": col_name,
                "data_type": col_type,
                "null_count": null_count,
                "null_pct": null_pct,
                "non_null_pct": non_null_pct,
                "distinct_count": distinct_count,
            })

    report = {
        "stage": stage_name,
        "row_count": row_count,
        "column_count": len(df.columns),
        "columns": column_profiles,
        "sampled": sampled,
        "sample_fraction": sample_fraction,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "[%s] Profiling complete: %s rows, %d columns (sampled: %s)",
        stage_name, f"{row_count:,}", len(df.columns), str(sampled),
    )
    return report


def save_quality_report(
    results: list[dict[str, Any]],
    stage_name: str,
    output_dir: Optional[str] = None,
) -> str:
    """
    Save quality validation results as a JSON report.

    Args:
        results: List of validation result dictionaries.
        stage_name: Name of the ETL stage.
        output_dir: Directory to save the report. Defaults to METADATA_PATH.

    Returns:
        Path to the saved report file.
    """
    if output_dir is None:
        output_dir = METADATA_PATH

    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"quality_report_{stage_name}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    report = {
        "stage": stage_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checks": results,
        "overall_passed": all(r.get("passed", False) for r in results),
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("[%s] Quality report saved: %s", stage_name, filepath)
    return filepath
