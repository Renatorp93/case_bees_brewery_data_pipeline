from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

try:
    import airflow  # noqa: F401
except Exception as exc:  # pragma: no cover - environment-dependent guard
    pytest.skip(f"airflow unavailable for DAG import: {exc}", allow_module_level=True)


def _load_dag_module():
    os.environ.setdefault("S3_ENDPOINT", "http://minio:9000")
    os.environ.setdefault("S3_ACCESS_KEY", "minio")
    os.environ.setdefault("S3_SECRET_KEY", "minio123")
    dag_path = Path("dags") / "breweries_medallion_dag.py"
    spec = importlib.util.spec_from_file_location("breweries_medallion_dag", dag_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_dag_contains_data_quality_task_and_order():
    module = _load_dag_module()
    dag = module.dag

    task_ids = set(dag.task_ids)
    assert {"bronze_ingest", "guard_bronze_manifest", "silver_curate", "dq_silver", "gold_aggregate"} <= task_ids

    downstream_of_silver = {task.task_id for task in dag.get_task("silver_curate").downstream_list}
    assert "dq_silver" in downstream_of_silver

    downstream_of_dq = {task.task_id for task in dag.get_task("dq_silver").downstream_list}
    assert "gold_aggregate" in downstream_of_dq


def test_dag_has_monitoring_callbacks():
    module = _load_dag_module()
    dag = module.dag

    assert dag.on_failure_callback is not None
    assert dag.on_success_callback is not None

    bronze_task = dag.get_task("bronze_ingest")
    assert bronze_task.on_failure_callback is not None
    assert bronze_task.on_retry_callback is not None
