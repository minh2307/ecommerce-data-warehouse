"""
Phase 5: ETL Pipeline Orchestrator
===================================
Executes the full ETL pipeline in the correct order with centralized
logging, error handling, and metadata tracking.

Pipeline Order:
    1. Ingestion   → Bronze Layer
    2. Cleaning    → Silver Layer
    3. dim_users   → Gold Layer
    4. dim_categories → Gold Layer
    5. dim_products_scd2 → Gold Layer (SCD Type 2)
    6. fact_events → Gold Layer (Fact Table)

Architecture Decision:
    This orchestrator replaces Airflow for local development.
    Each step is independently importable and testable.
    The orchestrator provides:
    - Sequential execution with dependency ordering
    - Centralized logging to logs/ directory
    - Graceful failure handling (logs error, stops pipeline)
    - Execution duration tracking for performance analysis
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

load_dotenv()

# Create logs directory before configuring logging
os.makedirs("logs", exist_ok=True)

# Configure centralized logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"logs/pipeline_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        ),
    ],
)

logger = logging.getLogger("pipeline")


def run_step(step_name: str, step_func, step_number: int, total_steps: int) -> None:
    """
    Execute a single pipeline step with logging and timing.

    Args:
        step_name: Human-readable name of the step.
        step_func: Callable function to execute.
        step_number: Current step number (1-indexed).
        total_steps: Total number of steps.

    Raises:
        Exception: Re-raises any exception from the step after logging.
    """
    logger.info(
        "=" * 70 + "\n"
        "[%d/%d] Starting: %s\n" + "=" * 70,
        step_number, total_steps, step_name,
    )

    start = time.monotonic()
    try:
        step_func()
        duration = time.monotonic() - start
        logger.info(
            "[%d/%d] ✅ %s completed in %.2f seconds",
            step_number, total_steps, step_name, duration,
        )
    except Exception as e:
        duration = time.monotonic() - start
        logger.error(
            "[%d/%d] ❌ %s FAILED after %.2f seconds: %s",
            step_number, total_steps, step_name, duration, str(e),
        )
        raise


def run_pipeline() -> None:
    """
    Execute the complete ETL pipeline.

    The pipeline runs each step sequentially. If any step fails,
    the pipeline stops and logs the error. Steps that have already
    completed are NOT rolled back (idempotent re-runs are safe).
    """
    # Import step functions using standard Python imports.
    # File naming convention: <action>_<layer/entity>.py
    from src.ingest_bronze import run_ingestion
    from src.clean_silver import run_cleaning
    from src.build_dim_users import run_dim_users
    from src.build_dim_categories import run_dim_categories
    from src.build_dim_products_scd2 import run_dim_products_scd2
    from src.build_fact_events import run_fact_events
    from src.load_gold_to_bigquery import run_bigquery_load

    # Define pipeline steps in execution order
    # Order matters: dimensions must be built before the fact table
    steps = [
        ("Phase 1: Data Ingestion (Bronze)", run_ingestion),
        ("Phase 2: Data Cleaning (Silver)", run_cleaning),
        ("Phase 3: Dimension Users (Gold)", run_dim_users),
        ("Phase 3: Dimension Categories (Gold)", run_dim_categories),
        ("Phase 4: SCD Type 2 Products (Gold)", run_dim_products_scd2),
        ("Phase 5: Fact Events (Gold)", run_fact_events),
        ("Phase 6: Load Gold Layer to BigQuery", run_bigquery_load),
    ]

    total_steps = len(steps)
    pipeline_start = time.monotonic()

    logger.info(
        "\n" + "🚀" * 35 + "\n"
        "STARTING WEB ANALYTICS ETL PIPELINE\n"
        "Steps: %d | Time: %s\n"
        + "🚀" * 35,
        total_steps,
        datetime.now(timezone.utc).isoformat(),
    )

    completed = 0
    try:
        for i, (name, func) in enumerate(steps, 1):
            run_step(name, func, i, total_steps)
            completed += 1

    except Exception:
        logger.error(
            "\n❌ PIPELINE STOPPED at step %d/%d. "
            "Previously completed steps: %d. "
            "Fix the error and re-run — all steps are idempotent.",
            completed + 1, total_steps, completed,
        )
        raise

    finally:
        total_duration = time.monotonic() - pipeline_start
        logger.info(
            "\n" + "=" * 70 + "\n"
            "PIPELINE SUMMARY\n"
            "Completed: %d/%d steps\n"
            "Total Duration: %.2f seconds (%.2f minutes)\n"
            "Status: %s\n"
            + "=" * 70,
            completed, total_steps,
            total_duration, total_duration / 60,
            "SUCCESS ✅" if completed == total_steps else "FAILED ❌",
        )


if __name__ == "__main__":
    run_pipeline()
