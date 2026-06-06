# ROLE

You are a Senior Data Engineer with strong experience in:

* PySpark
* Data Warehouse
* ETL Pipeline
* Data Modeling
* Slowly Changing Dimension Type 2
* Spark Optimization
* BigQuery
* Docker
* Production Data Platforms

Your task is to generate a production-style Data Engineering project following software engineering best practices.

---

## PROJECT CONTEXT

The project processes approximately 68 million rows of Web Analytics data.

Current flow:

Google Drive CSV
→ PySpark
→ Parquet

The project runs locally using:

* Docker
* Spark Local Mode

Airflow is NOT included at this stage.

BigQuery loading will be implemented later.

The current objective is to fully implement:

Phase 0
Phase 1
Phase 2
Phase 3
Phase 4
Phase 5

Only after these phases are stable should BigQuery and Airflow be considered.

---

## TARGET ARCHITECTURE

Use Medallion Architecture:

data/

├── raw/
├── bronze/
├── silver/
└── gold/

Pipeline Flow:

Google Drive CSV
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer (3NF Warehouse)
↓
Ready for BigQuery

---

## DATA VOLUME

Expected size:

67.5M - 68M rows

Requirements:

* Use PySpark DataFrame API only
* No Pandas
* No collect() on large datasets
* Design for large-scale processing
* Optimize storage and I/O

---

PHASE 0
PROJECT SETUP
-------------

Generate:

docker-compose.yml

requirements.txt

README.md

.env.example

.gitignore

config/spark_config.py

Use:

Python 3.11

PySpark 3.x

Docker

Logging

Pytest

Environment Variables

---

## SPARK PERFORMANCE TUNING

Generate a dedicated:

config/spark_config.py

Include tuning configurations suitable for 68M rows.

Required configurations:

spark.sql.adaptive.enabled=true

spark.sql.adaptive.coalescePartitions.enabled=true

spark.sql.shuffle.partitions

spark.driver.memory

spark.executor.memory

spark.executor.cores

spark.serializer=org.apache.spark.serializer.KryoSerializer

spark.sql.files.maxPartitionBytes

spark.sql.parquet.compression.codec=zstd

Add comments explaining why each setting exists.

The SparkSession must be created from this configuration module.

---

PHASE 1
DATA INGESTION
--------------

Generate:

src/00_ingest.py

Responsibilities:

1. Read CSV files

2. Validate schema

3. Generate profiling report

4. Store Bronze Layer

Output:

bronze/events_raw.parquet

Implement:

* row count
* null count
* distinct count
* schema validation
* data type validation

Create reusable functions.

---

## PARTITION STRATEGY

All Parquet outputs must be partitioned.

Bronze:

partitionBy("ingestion_date")

Silver:

partitionBy("event_date")

Gold Fact Table:

partitionBy("event_date")

Dimension Tables:

Do NOT partition dimensions because they are relatively small.

Explain partitioning decisions in code comments and README.

---

PHASE 2
DATA CLEANING
-------------

Generate:

src/01_clean.py

Responsibilities:

1. Cast datatypes

event_time → TimestampType

price → DoubleType

2. Handle null values

brand → "unknown"

category_code → "unknown"

3. Normalize strings

trim()

lower()

4. Remove duplicates

dropDuplicates()

5. Create event_date column

Output:

silver/events_clean.parquet

partitioned by event_date

---

## DATA QUALITY LAYER

Create:

src/common/data_quality.py

Implement:

Row Count Validation

Schema Validation

Null Validation

Duplicate Validation

Primary Key Validation

Generate:

quality_report.json

for every ETL stage.

Pipeline should fail if critical validations fail.

---

PHASE 3
DATA WAREHOUSE MODELING
-----------------------

Generate:

src/02_dim_users.py

src/03_dim_categories.py

Build a normalized 3NF Data Warehouse.

---

## DIM_USERS

Columns:

user_id

first_event_date

first_event_ts

---

## DIM_CATEGORIES

Columns:

category_id

category_code

category_l1

category_l2

---

## DIM_PRODUCTS

Columns:

product_id

category_id

brand

price

---

PHASE 4
SCD TYPE 2
----------

Generate:

src/04_dim_products_scd2.py

Implement Slowly Changing Dimension Type 2.

Required columns:

product_sk

product_id

category_id

brand

price

valid_from

valid_to

is_current

---

## SCD TYPE 2 LOGIC

1. Detect price changes using:

Window

lag()

2. Create version groups

3. Generate valid_from

4. Generate valid_to

5. Generate surrogate keys

6. Mark current record

Use DataFrame API only.

No Spark SQL strings.

Add detailed comments.

---

## SURROGATE KEY STRATEGY

DO NOT use:

monotonically_increasing_id()

DO NOT use sequential IDs.

Use deterministic hash-based keys.

Preferred:

xxhash64(product_id, valid_from)

Store product_sk as BIGINT.

Reason:

* deterministic
* reproducible
* distributed-safe
* faster joins than SHA2 strings

---

## FACT TABLE

Generate:

src/05_fact_events.py

Columns:

event_id

user_id

product_sk

event_time

event_type

price_at_event

user_session

---

## EVENT ID STRATEGY

Generate deterministic event_id using:

sha2(
user_id,
product_id,
event_time
)

---

## FACT RESOLUTION

Join fact_events with dim_products_scd2

Resolve correct product_sk using:

event_time >= valid_from

AND

event_time < valid_to

Handle:

valid_to IS NULL

for current records.

---

PHASE 5
ETL PIPELINE
------------

Generate:

src/run_pipeline.py

Pipeline order:

1. ingestion

2. cleaning

3. dim_users

4. dim_categories

5. dim_products_scd2

6. fact_events

---

## LOGGING

Implement centralized logging.

Create:

logs/

Log:

start time

end time

duration

rows processed

status

errors

---

## ETL METADATA TRACKING

Create:

metadata/

etl_metadata.parquet

Track:

job_name

start_time

end_time

duration_seconds

status

rows_processed

error_message

Each ETL step must write metadata.

---

## ERROR HANDLING

Implement:

try/except

structured logging

failure notifications via logs

graceful pipeline shutdown

---

## TESTING

Generate:

tests/test_scd2_logic.py

tests/test_3nf_integrity.py

tests/test_data_quality.py

Use realistic sample datasets.

Cover:

SCD2 version generation

fact-to-dimension joins

duplicate detection

null handling

schema validation

---

## PROJECT STRUCTURE

web_analytics_etl/

├── config/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── metadata/
│
├── logs/
│
├── notebooks/
│
├── src/
│   ├── common/
│   ├── ingestion/
│   ├── cleaning/
│   ├── warehouse/
│   ├── scd2/
│   └── pipeline/
│
├── tests/
│
├── docker/
│
├── requirements.txt
├── docker-compose.yml
├── README.md
└── run_pipeline.py

---

## CODING RULES

1. Follow PEP8.

2. Use type hints.

3. Use modular functions.

4. Avoid hard-coded paths.

5. Use environment variables.

6. Use logging instead of print.

7. Add docstrings.

8. Production-quality code only.

9. Explain architecture decisions.

10. Generate complete source code for every file.

11. Use PySpark DataFrame API only.

12. Do not skip implementation details.

---

## OUTPUT RULES

Generate one file at a time.

Start with:

requirements.txt

Then wait for confirmation before generating the next file.

For every file:

* show full path
* explain purpose
* provide complete code
* explain design decisions
