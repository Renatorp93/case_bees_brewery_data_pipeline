from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from breweries_pipeline.guard.s3_guard import guard_bronze_metadata
from breweries_pipeline.monitoring.alerts import (
    on_dag_failure,
    on_dag_success,
    on_task_failure,
    on_task_retry,
)

JOBS_PATH = "/opt/airflow/src/breweries_pipeline/jobs"


def _required_env(name: str) -> str:
    """Read a required env var and fail fast when it is missing/empty."""

    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# -----------------------
# Env / Defaults
# -----------------------
S3_ENDPOINT = _required_env("S3_ENDPOINT")
S3_ACCESS_KEY = _required_env("S3_ACCESS_KEY")
S3_SECRET_KEY = _required_env("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")

BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/breweries")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver/breweries")
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "gold/breweries")
QUALITY_PREFIX = os.getenv("QUALITY_PREFIX", "monitoring/data_quality")

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}

SPARK_ENV = {
    "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
    "S3_ENDPOINT": S3_ENDPOINT,
    "PYTHONPATH": "/opt/airflow/src",
    "PYSPARK_PYTHON": "python3",
    "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python",
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
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    tags=["case", "brewery", "medallion"],
    params={
        # For manual triggers:
        # - run_id: override the computed run_id
        # - write_mode: skip | overwrite | fail
        "run_id": None,
        "write_mode": "skip",
        "per_page": 200,
        "max_pages": 10,
        "timeout_s": 30,
        "dq_min_rows": 1,
    },
) as dag:
    # Manual runs should be unique by trigger time; scheduled runs stay stable by interval end.
    run_id = (
        "{{ params.run_id or "
        "(ts_nodash if dag_run and dag_run.run_type == 'manual' "
        "else data_interval_end.in_timezone('UTC').strftime('%Y%m%dT%H%M%S')) }}"
    )

    write_mode = "{{ params.write_mode }}"
    per_page = "{{ params.per_page }}"
    max_pages = "{{ params.max_pages }}"
    timeout_s = "{{ params.timeout_s }}"
    dq_min_rows = "{{ params.dq_min_rows }}"

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            f"{JOBS_PATH}/bronze_ingest.py "
            f"--run-id '{run_id}' "
            f"--per-page {per_page} "
            f"--max-pages {max_pages} "
            f"--out-prefix s3a://{S3_BUCKET}/{BRONZE_PREFIX} "
            f"--timeout-s {timeout_s} "
            f"--write-mode {write_mode} "
        ),
        env=SPARK_ENV,
    )

    guard = PythonOperator(
        task_id="guard_bronze_manifest",
        python_callable=guard_bronze_metadata,
        op_kwargs={
            "bucket": S3_BUCKET,
            "bronze_prefix": BRONZE_PREFIX,
            "run_id": run_id,
        },
    )

    silver = BashOperator(
        task_id="silver_curate",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            f"{JOBS_PATH}/silver_curate.py "
            f"--run-id '{run_id}' "
            f"--bronze-prefix s3a://{S3_BUCKET}/{BRONZE_PREFIX} "
            f"--silver-prefix s3a://{S3_BUCKET}/{SILVER_PREFIX} "
        ),
        env=SPARK_ENV,
    )

    dq_silver = BashOperator(
        task_id="dq_silver",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            f"{JOBS_PATH}/data_quality.py "
            f"--run-id '{run_id}' "
            f"--silver-prefix s3a://{S3_BUCKET}/{SILVER_PREFIX} "
            f"--quality-prefix s3a://{S3_BUCKET}/{QUALITY_PREFIX} "
            f"--min-rows {dq_min_rows} "
        ),
        env=SPARK_ENV,
    )

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command=(
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            f"{JOBS_PATH}/gold_aggregate.py "
            f"--run-id '{run_id}' "
            f"--silver-prefix s3a://{S3_BUCKET}/{SILVER_PREFIX} "
            f"--gold-prefix s3a://{S3_BUCKET}/{GOLD_PREFIX} "
        ),
        env=SPARK_ENV,
    )

    bronze >> guard >> silver >> dq_silver >> gold
