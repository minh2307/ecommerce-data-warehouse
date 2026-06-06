# Web Analytics ETL — eCommerce Data Warehouse

A production-style Data Engineering project that processes **~68 million rows** of web analytics data through a **Medallion Architecture** (Bronze → Silver → Gold) pipeline using **PySpark** and **Docker**.

---

## 🏗️ Architecture

```
Google Drive CSV
       ↓
  ┌─────────────────┐
  │   Bronze Layer   │  Raw data as-is (partitioned by ingestion_date)
  └────────┬────────┘
           ↓
  ┌─────────────────┐
  │   Silver Layer   │  Cleaned, typed, deduplicated (partitioned by event_date)
  └────────┬────────┘
           ↓
  ┌─────────────────┐
  │    Gold Layer    │  3NF Warehouse: Dimensions + Fact Table
  └────────┬────────┘
           ↓
     Ready for BigQuery
```

### Data Model (3NF)

| Table | Type | Description |
|-------|------|-------------|
| `dim_users` | Dimension | Unique users with first event timestamp |
| `dim_categories` | Dimension | Category hierarchy (L1/L2 split) |
| `dim_products_scd2` | Dimension (SCD2) | Products with full price history |
| `fact_events` | Fact | Events with temporal FK to product versions |

### Partitioning Strategy

| Layer | Partition Key | Reason |
|-------|--------------|--------|
| Bronze | `ingestion_date` | Track when data was ingested |
| Silver | `event_date` | Optimize time-range queries |
| Gold (Fact) | `event_date` | Partition pruning for analytics |
| Gold (Dims) | None | Small tables — broadcast joins |

---

## 📁 Project Structure

```
web_analytics_etl/
├── config/
│   └── spark_config.py          # Centralized Spark tuning (AQE, Kryo, ZSTD)
├── data/
│   ├── raw/                     # Source CSV files
│   ├── bronze/                  # Raw Parquet (ingestion_date partitioned)
│   ├── silver/                  # Cleaned Parquet (event_date partitioned)
│   └── gold/                    # 3NF Warehouse (dims + fact)
├── metadata/                    # ETL execution metadata (JSONL)
├── logs/                        # Pipeline execution logs
├── src/
│   ├── common/
│   │   ├── data_quality.py      # Reusable quality checks
│   │   └── etl_metadata.py      # Execution metadata tracker
│   ├── ingest_bronze.py          # Phase 1: CSV → Bronze
│   ├── clean_silver.py           # Phase 2: Bronze → Silver
│   ├── build_dim_users.py        # Phase 3: Silver → Gold (dim_users)
│   ├── build_dim_categories.py   # Phase 3: Silver → Gold (dim_categories)
│   ├── build_dim_products_scd2.py # Phase 4: Silver → Gold (SCD Type 2)
│   ├── build_fact_events.py      # Phase 4: Silver+Gold → Gold (fact_events)
│   └── run_pipeline.py           # Phase 5: Full pipeline orchestrator
├── tests/
│   ├── test_scd2_logic.py       # SCD2 change detection tests
│   ├── test_3nf_integrity.py    # FK/PK integrity tests
│   └── test_data_quality.py     # Data quality module tests
├── docker/
│   └── Dockerfile               # Python 3.11 + OpenJDK 17 + PySpark
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- OR Python 3.11 + Java 17 (for local development)

### Option 1: Docker (Recommended)

```bash
# 1. Clone and configure
cp .env.example .env

# 2. Place your CSV data in data/raw/
#    (download from Google Drive or use gdrive_to_parquet.py)

# 3. Build and run the full pipeline
docker compose build
docker compose run --rm spark-etl
```

### Option 2: Local Development

```bash
# 1. Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env

# 4. Place CSV data in data/raw/

# 5. Run the full pipeline
python src/run_pipeline.py

# 6. Or run individual phases
python src/ingest_bronze.py
python src/clean_silver.py
python src/build_dim_users.py
python src/build_dim_categories.py
python src/build_dim_products_scd2.py
python src/build_fact_events.py
```

### Running Tests

```bash
pytest tests/ -v
```

---

## ⚙️ Spark Performance Tuning

All Spark configurations are centralized in `config/spark_config.py` and loaded from environment variables:

| Setting | Default | Purpose |
|---------|---------|---------|
| `spark.driver.memory` | 4g | Memory for 68M row processing |
| `spark.sql.adaptive.enabled` | true | Dynamic query optimization |
| `spark.serializer` | KryoSerializer | ~10x faster serialization |
| `spark.sql.parquet.compression.codec` | zstd | Best compression ratio |
| `spark.sql.shuffle.partitions` | 200 | AQE auto-coalesces small partitions |

---

## 🔄 SCD Type 2 Implementation

The `dim_products_scd2` tracks product price changes over time:

1. **Change Detection**: Window + `lag()` to detect price changes
2. **Version Groups**: Cumulative sum of change flags
3. **Temporal Validity**: `valid_from` / `valid_to` using `lead()`
4. **Surrogate Keys**: `xxhash64(product_id, valid_from)` — deterministic, distributed-safe BIGINT
5. **Current Record**: `is_current = True` where `valid_to IS NULL`

### Temporal Join (Fact → SCD2)

```
event_time >= valid_from AND (event_time < valid_to OR valid_to IS NULL)
```

---

## 📊 Data Quality

Every ETL step generates a `quality_report.json` in `metadata/` covering:

- ✅ Row count validation
- ✅ Schema validation
- ✅ Null checks on critical columns
- ✅ Duplicate detection
- ✅ Primary key uniqueness

Pipeline **fails fast** if critical quality checks do not pass.

---

## 📝 Pipeline Phases

| Phase | Script | Input | Output |
|-------|--------|-------|--------|
| 0 | Setup | — | Config, Docker, README |
| 1 | `ingest_bronze.py` | `data/raw/*.csv` | `data/bronze/events_raw.parquet` |
| 2 | `clean_silver.py` | Bronze | `data/silver/events_clean.parquet` |
| 3 | `build_dim_users.py` | Silver | `data/gold/dim_users.parquet` |
| 3 | `build_dim_categories.py` | Silver | `data/gold/dim_categories.parquet` |
| 4 | `build_dim_products_scd2.py` | Silver | `data/gold/dim_products_scd2.parquet` |
| 4 | `build_fact_events.py` | Silver + Gold | `data/gold/fact_events.parquet` |
| 5 | `run_pipeline.py` | — | Orchestrates all phases |

---

## 🔮 Future Phases

- **BigQuery Loading**: Upload Gold layer to BigQuery for BI
- **Airflow Orchestration**: Schedule and monitor pipeline runs
- **Data Observability**: Integrate with monitoring dashboards
