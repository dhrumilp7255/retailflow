from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta

RAW_BUCKET       = "retail-etl-raw-data"
PROCESSED_BUCKET = "retail-etl-processed-data"
GLUE_JOB_NAME    = "retail-etl-glue-job"
GLUE_CRAWLER     = "retail-etl-crawler"
AWS_CONN_ID      = "aws_default"
REGION           = "us-east-1"

DEFAULT_ARGS = {
    "owner":            "retail_etl",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

def log_pipeline_start(**context):
    execution_date = context["logical_date"]
    print(f"Starting Retail ETL Pipeline for execution date: {execution_date}")
    print(f"Raw bucket      : {RAW_BUCKET}")
    print(f"Processed bucket: {PROCESSED_BUCKET}")
    print(f"Glue job        : {GLUE_JOB_NAME}")

def verify_processed_output(**context):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    expected_prefixes = [
        "weekly_fact/",
        "store_dim/",
        "product_dim/",
        "calendar_dim/",
    ]
    missing = []
    for prefix in expected_prefixes:
        keys = hook.list_keys(bucket_name=PROCESSED_BUCKET, prefix=prefix)
        if not keys:
            missing.append(prefix)
    if missing:
        raise ValueError(f"Missing output folders in S3: {missing}")
    print("All expected output folders verified in S3:")
    for prefix in expected_prefixes:
        print(f"  s3://{PROCESSED_BUCKET}/{prefix}")

def log_success(**context):
    print("Retail ETL Pipeline completed successfully!")
    print("Data is available in Athena under database: retail_etl_db")
    print("Tables: weekly_fact, store_dim, product_dim, calendar_dim")

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retail ETL pipeline: S3 -> Glue -> Parquet -> Catalog",
    schedule="0 1 * * 1",
    catchup=False,
    tags=["retail", "etl", "glue"],
) as dag:

    start = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
    )

    check_sales = S3KeySensor(
        task_id="check_sales_file",
        bucket_name=RAW_BUCKET,
        bucket_key="sales/sales.csv",
        aws_conn_id=AWS_CONN_ID,
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    check_inventory = S3KeySensor(
        task_id="check_inventory_file",
        bucket_name=RAW_BUCKET,
        bucket_key="inventory/inventory.csv",
        aws_conn_id=AWS_CONN_ID,
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    check_store = S3KeySensor(
        task_id="check_store_file",
        bucket_name=RAW_BUCKET,
        bucket_key="store/store.csv",
        aws_conn_id=AWS_CONN_ID,
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    check_product = S3KeySensor(
        task_id="check_product_file",
        bucket_name=RAW_BUCKET,
        bucket_key="product/product.csv",
        aws_conn_id=AWS_CONN_ID,
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    check_calendar = S3KeySensor(
        task_id="check_calendar_file",
        bucket_name=RAW_BUCKET,
        bucket_key="calendar/calendar.csv",
        aws_conn_id=AWS_CONN_ID,
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    run_glue_job = GlueJobOperator(
        task_id="run_glue_transformation",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
        wait_for_completion=True,
        verbose=True,
    )

    verify_output = PythonOperator(
        task_id="verify_processed_output",
        python_callable=verify_processed_output,
    )

    run_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": GLUE_CRAWLER},
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
    )

    complete = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    start >> [check_sales, check_inventory, check_store, check_product, check_calendar]
    [check_sales, check_inventory, check_store, check_product, check_calendar] >> run_glue_job
    run_glue_job >> verify_output >> run_crawler >> complete