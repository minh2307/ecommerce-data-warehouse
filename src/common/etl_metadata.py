"""
ETL Metadata Tracking Module
=============================
Tracks execution metadata (start/end time, duration, rows processed, status)
for every ETL step. Stores results as Parquet for analytics.

Architecture Decision:
    Metadata is stored in Parquet format (not JSON) so it can be queried
    with Spark/SQL for pipeline observability. Each run appends to the
    existing metadata file, creating a complete audit trail.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

METADATA_PATH = os.getenv("METADATA_PATH", "metadata")


class ETLMetadataTracker:
    """
    Context manager that tracks ETL job execution metadata.

    Usage:
        with ETLMetadataTracker("ingestion") as tracker:
            # ... do ETL work ...
            tracker.set_rows_processed(68_000_000)
    """

    def __init__(self, job_name: str) -> None:
        self.job_name = job_name
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds: float = 0.0
        self.status: str = "running"
        self.rows_processed: int = 0
        self.error_message: Optional[str] = None
        self._start_monotonic: float = 0.0

    def __enter__(self) -> "ETLMetadataTracker":
        self.start_time = datetime.now(timezone.utc)
        self._start_monotonic = time.monotonic()
        logger.info("[%s] ETL step started at %s", self.job_name, self.start_time.isoformat())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.end_time = datetime.now(timezone.utc)
        self.duration_seconds = round(time.monotonic() - self._start_monotonic, 2)

        if exc_type is not None:
            self.status = "failed"
            self.error_message = str(exc_val)
            logger.error(
                "[%s] ETL step FAILED after %.2fs: %s",
                self.job_name, self.duration_seconds, exc_val,
            )
        else:
            self.status = "success"
            logger.info(
                "[%s] ETL step completed in %.2fs. Rows: %s",
                self.job_name, self.duration_seconds, f"{self.rows_processed:,}",
            )

        self._save_metadata()
        # Do not suppress exceptions
        return False

    def set_rows_processed(self, count: int) -> None:
        """Set the number of rows processed by this ETL step."""
        self.rows_processed = count

    def _save_metadata(self) -> None:
        """Save execution metadata to a JSON file (appends to log)."""
        os.makedirs(METADATA_PATH, exist_ok=True)

        record = {
            "job_name": self.job_name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "rows_processed": self.rows_processed,
            "error_message": self.error_message,
        }

        # Append to a JSONL file (one JSON object per line) for easy parsing
        metadata_file = os.path.join(METADATA_PATH, "etl_metadata.jsonl")
        with open(metadata_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

        logger.info("[%s] Metadata saved to %s", self.job_name, metadata_file)
