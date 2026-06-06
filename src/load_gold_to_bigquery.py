"""
Phase 6: Load Gold Layer to Google BigQuery
===========================================
Reads the processed Gold layer tables (Parquet format) and loads them 
into Google BigQuery. 

Uses optimized Parquet loading (WRITE_TRUNCATE/WRITE_APPEND) which is faster
and does not require defining BigQuery schemas manually.
"""

import os
import glob
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def get_bigquery_client() -> bigquery.Client:
    """
    Initialize and return the BigQuery client.
    
    Reads credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
    If not specified, falls back to Google Application Default Credentials (ADC).
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in environment variables.")
        
    if credentials_path and os.path.exists(credentials_path):
        logger.info("Initializing BigQuery client with credentials from: %s", credentials_path)
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        return bigquery.Client(project=project_id, credentials=credentials)
    else:
        logger.info("Initializing BigQuery client with Application Default Credentials (ADC).")
        return bigquery.Client(project=project_id)


def load_parquet_to_bigquery(client: bigquery.Client, local_dir: str, dataset_id: str, table_name: str) -> None:
    """
    Load all part-*.parquet files inside local_dir into the target BigQuery table.
    """
    # Spark writes Parquet files to directories with part-* naming conventions.
    search_path = os.path.join(local_dir, "part-*.parquet")
    parquet_files = glob.glob(search_path)
    
    if not parquet_files:
        # Check if the directory itself is a parquet file (e.g. if single file)
        if local_dir.endswith(".parquet") and os.path.isfile(local_dir):
            parquet_files = [local_dir]
        else:
            # Recursive check as fallback
            search_path_recursive = os.path.join(local_dir, "**", "*.parquet")
            parquet_files = glob.glob(search_path_recursive, recursive=True)
            
    if not parquet_files:
        logger.warning("No parquet files found in directory: %s", local_dir)
        return

    table_id = f"{client.project}.{dataset_id}.{table_name}"
    logger.info("Loading %d parquet files from %s into %s...", len(parquet_files), local_dir, table_id)

    # Configure the load job
    # Use WRITE_TRUNCATE for the first file to overwrite the table, 
    # then WRITE_APPEND for subsequent files to append all partitions.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    for i, file_path in enumerate(parquet_files):
        if i > 0:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete.
            
    # Verify rows loaded
    table = client.get_table(table_id)
    logger.info("✅ Table %s loaded successfully. Total rows: %s", table_name, f"{table.num_rows:,}")


def run_bigquery_load() -> None:
    """
    Orchestrate the loading of Gold layer Parquet files to BigQuery.
    """
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "ecommerce_dw")
    gold_path = os.getenv("GOLD_DATA_PATH", "data/gold")
    
    # Check if GCP project is set, if not, skip BigQuery stage gracefully
    gcp_project = os.getenv("GCP_PROJECT_ID")
    if not gcp_project:
        logger.warning("GCP_PROJECT_ID is not configured. Skipping BigQuery load phase.")
        return

    try:
        client = get_bigquery_client()
    except Exception as e:
        logger.error("Failed to initialize BigQuery client: %s", str(e))
        logger.error("Please configure your GCP credentials in .env correctly.")
        raise

    # Create dataset if it doesn't exist
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = os.getenv("BIGQUERY_LOCATION", "US")
    
    try:
        client.get_dataset(dataset.reference)
        logger.info("Dataset %s already exists.", dataset_id)
    except Exception:
        logger.info("Dataset %s does not exist. Creating dataset...", dataset_id)
        client.create_dataset(dataset, timeout=30)
        logger.info("Dataset %s created successfully.", dataset_id)

    # Define tables mapping
    tables_to_load = {
        "dim_users.parquet": "dim_users",
        "dim_categories.parquet": "dim_categories",
        "dim_products_scd2.parquet": "dim_products_scd2",
        "fact_events.parquet": "fact_events",
    }

    for dir_name, table_name in tables_to_load.items():
        local_dir = os.path.join(gold_path, dir_name)
        if os.path.exists(local_dir):
            try:
                load_parquet_to_bigquery(client, local_dir, dataset_id, table_name)
            except Exception as e:
                logger.error("Failed to load table %s to BigQuery: %s", table_name, str(e))
                raise
        else:
            logger.warning("Gold directory not found, skipping table %s: %s", table_name, local_dir)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_bigquery_load()
