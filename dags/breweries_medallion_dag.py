from __future__ import annotations

import os
from datetime import datetime, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# -----------------------
# Env / Defaults
# -----------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio123")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")

# Prefixes in the bucket
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/breweries")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver/breweries")
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "gold/breweries")

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

# S3A access (Spark <-> MinIO)
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def guard_bronze_metadata(**context) -> None:
    """
    Fail fast guard:
    - ensures bronze wrote the manifest _metadata.json
    - this avoids running silver/gold on incomplete or failed bronze runs
    """
    run_id = context["templates_dict"]["run_id"]
    key = f"{BRONZE_PREFIX}/run_id={run_id}/_metadata.json"
    s3 = _s3_client()

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        raise RuntimeError(
            f"Bronze guard failed: manifest not found. "
            f"bucket={S3_BUCKET} key={key} error_code={code}"
        ) from e


default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout":timedelta(minutes=15),
}

with DAG(
    dag_id="breweries_medallion_pipeline",
    description=(
        "Open Brewery DB -> Bronze(JSONL) -> Silver(Parquet) -> Gold(Aggregates). "
        "Includes a fail-fast guard that validates bronze manifest before running silver/gold. "
        "Scheduled daily; can also be triggered manually for evaluation."
    ),
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["case", "brewery", "medallion"],
) as dag:


    run_id = "{{ data_interval_end.in_timezone('UTC').strftime('%Y%m%dT%H%M%S') }}"

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            "/opt/airflow/src/jobs/bronze_ingest.py "
            f"--run-id {run_id} "
            "--per-page 200 "
            "--max-pages 10 "
            f"--out-prefix s3a://{S3_BUCKET}/{BRONZE_PREFIX} "
            "--timeout-s 30 "
            "--skip-if-exists "
        ),
        env={
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
            "S3_ENDPOINT": S3_ENDPOINT,
        },
    )

    guard = PythonOperator(
        task_id="guard_bronze_manifest",
        python_callable=guard_bronze_metadata,
        templates_dict={"run_id": run_id},
    )

    silver = BashOperator(
        task_id="silver_curate",
        bash_command=(
            "spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            "/opt/airflow/src/jobs/silver_curate.py "
            f"--run-id {run_id} "
            f"--bronze-prefix s3a://{S3_BUCKET}/{BRONZE_PREFIX} "
            f"--silver-prefix s3a://{S3_BUCKET}/{SILVER_PREFIX} "
        ),
        env={
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
            "S3_ENDPOINT": S3_ENDPOINT,
        },
    )

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command=(
            "spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            "/opt/airflow/src/jobs/gold_aggregate.py "
            f"--run-id {run_id} "
            f"--silver-prefix s3a://{S3_BUCKET}/{SILVER_PREFIX} "
            f"--gold-prefix s3a://{S3_BUCKET}/{GOLD_PREFIX} "
        ),
        env={
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
            "S3_ENDPOINT": S3_ENDPOINT,
        },
    )

    bronze >> guard >> silver >> gold