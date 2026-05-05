# 🛒 Retail Store End-to-End ETL Pipeline

A production-grade batch data pipeline for a retail store built with modern data engineering tools on AWS. The pipeline extracts data from a transactional PostgreSQL database, transforms it using PySpark on AWS Glue, models it with dbt, and makes it queryable via Athena — all orchestrated by Apache Airflow.

---

## 🏗️ Architecture

```
PostgreSQL (OLTP - Docker)
        │
        ▼
┌─────────────────────┐
│   AWS S3 Raw Bucket  │  ← Daily CSV exports
│  (Staging Area)      │
└─────────────────────┘
        │
        │  Airflow S3KeySensor detects files
        ▼
┌─────────────────────┐
│   AWS Glue Job       │  ← PySpark transformation
│   (PySpark ETL)      │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│ AWS S3 Processed     │  ← Parquet (partitioned)
│ Bucket               │
└─────────────────────┘
        │
        │  Glue Crawler catalogs tables
        ▼
┌─────────────────────┐
│ AWS Glue Catalog +   │  ← Queryable via Athena
│ Amazon Athena        │
└─────────────────────┘
        │
        │  dbt transforms + tests
        ▼
┌─────────────────────┐
│ dbt Marts            │  ← Enriched weekly fact table
│ (Data Quality ✅)    │     6/6 tests passing
└─────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Source Database | PostgreSQL 15 (Docker) | OLTP transactional source |
| Data Generation | Python + Faker | Realistic synthetic retail data |
| Cloud Storage | AWS S3 | Raw and processed data lake |
| Transformation | AWS Glue (PySpark) | Heavy ETL transformation |
| Orchestration | Apache Airflow (Astro CLI) | Pipeline scheduling and monitoring |
| Data Catalog | AWS Glue Catalog | Metadata management |
| Query Engine | AWS Athena | SQL queries on S3 data |
| Data Modeling | dbt | Staging views + enriched marts |
| Data Quality | dbt tests | 6/6 not_null tests passing |
| Infrastructure | Terraform | Infrastructure as code |

---

## 📊 Business Requirements

The pipeline produces a **weekly fact table** grouped by store and product with the following metrics:

| Metric | Description |
|---|---|
| `total_sales_qty` | Total units sold per week |
| `total_sales_amt` | Total revenue per week |
| `avg_sales_price` | Average selling price |
| `total_cost_amt` | Total cost of goods sold |
| `gross_profit` | Revenue minus cost |
| `gross_margin_pct` | Gross margin percentage |
| `stock_on_hand_qty` | End of week inventory level |
| `ordered_stock_qty` | End of week on-order quantity |
| `pct_in_stock` | % of week product was in stock |
| `no_stock_instances` | Number of out-of-stock days |
| `low_stock_instances` | Number of low-stock days |
| `weeks_of_supply` | How many weeks stock will last |

---

## 📁 Project Structure

```
retail-etl-pipeline/
│
├── infrastructure/                  # Terraform IaC
│   └── main.tf                      # S3, IAM, Glue resources
│
├── data_generator/                  # Synthetic data scripts
│   ├── create_schema.sql            # PostgreSQL schema DDL
│   └── generate_data.py             # Generates realistic retail data
│
├── ingestion/                       # Airflow project (Astro CLI)
│   └── dags/
│       └── retail_etl_dag.py        # Full pipeline DAG (10 tasks)
│
├── transformation/                  # PySpark transformation
│   └── spark_jobs/
│       └── glue_retail_transform.py # AWS Glue PySpark job
│
├── dbt_transform/                   # dbt project
│   └── models/
│       ├── staging/                 # Staging views on Athena tables
│       │   ├── sources.yml
│       │   ├── stg_weekly_fact.sql
│       │   ├── stg_store_dim.sql
│       │   ├── stg_product_dim.sql
│       │   └── stg_calendar_dim.sql
│       └── marts/                   # Enriched final tables
│           ├── mart_weekly_sales.sql
│           └── mart_weekly_sales.yml  # dbt data quality tests
│
├── raw_data/                        # Sample CSV exports
├── docker-compose.yml               # PostgreSQL local setup
└── README.md
```

---

## 🚀 Setup Instructions

### Prerequisites

- Docker Desktop
- AWS CLI configured (`aws configure`)
- Python 3.9+
- Astro CLI
- Terraform

### 1. Clone the repository

```bash
git clone [https://github.com/YOUR_USERNAME/retail-etl-pipeline.git](https://github.com/dhrumilp7255/retailflow.git)
cd retail-etl-pipeline
```

### 2. Start PostgreSQL

```bash
docker compose up -d
```

### 3. Create schema and generate synthetic data

```bash
pip install psycopg2-binary faker
# Load schema
Get-Content data_generator/create_schema.sql | docker exec -i retail_oltp_db psql -U retail_user -d retail_oltp
# Generate data (~70k sales, ~230k inventory rows)
python data_generator/generate_data.py
```

### 4. Export CSVs to S3

```bash
docker exec -it retail_oltp_db psql -U retail_user -d retail_oltp -c "\COPY sales TO '/tmp/sales.csv' WITH CSV HEADER"
docker cp retail_oltp_db:/tmp/sales.csv raw_data/sales.csv
aws s3 cp raw_data/ s3://retail-etl-raw-data/ --recursive
```

### 5. Provision infrastructure with Terraform

```bash
cd infrastructure
terraform init
terraform apply
```

### 6. Run AWS Glue transformation job

```bash
aws glue start-job-run --job-name retail-etl-glue-job --region us-east-1
```

### 7. Start Airflow and trigger the pipeline

```bash
cd ingestion
astro dev start
# Open http://localhost:8080 and trigger retail_etl_pipeline DAG
```

### 8. Run dbt models and tests

```bash
cd dbt_transform
dbt run
dbt test
```

---

## ✅ Airflow DAG

The pipeline DAG has **10 tasks** running in sequence:

```
log_pipeline_start
        │
        ├── check_sales_file
        ├── check_inventory_file      ← S3KeySensors (parallel)
        ├── check_store_file
        ├── check_product_file
        └── check_calendar_file
                │
        run_glue_transformation       ← PySpark ETL on AWS Glue
                │
        verify_processed_output       ← Data quality check
                │
        run_glue_crawler              ← Updates Glue Catalog
                │
        log_success
```

---

## 🧪 dbt Data Quality Tests

All **6/6 data quality tests passing**:

```
not_null_mart_weekly_sales_year_wk_num       ✅
not_null_mart_weekly_sales_store_key         ✅
not_null_mart_weekly_sales_prod_key          ✅
not_null_mart_weekly_sales_total_sales_amt   ✅
not_null_mart_weekly_sales_gross_margin_pct  ✅
not_null_mart_weekly_sales_pct_in_stock      ✅
```

---

## 📦 Dataset

Synthetic retail dataset generated with Python Faker library:

| Table | Rows | Description |
|---|---|---|
| sales | ~71,000 | Daily transactions with seasonal patterns |
| inventory | ~234,000 | Daily stock snapshots per store/product |
| store | 20 | Store master data across 4 countries |
| product | 50 | Product catalog across 5 categories |
| calendar | 731 | Calendar dimension (2023-2024) |

---

## 👤 Author

**Dhrumil Patel**
- LinkedIn: [https://www.linkedin.com/in/dhrumil-patel-02b92a1b2/](#)
