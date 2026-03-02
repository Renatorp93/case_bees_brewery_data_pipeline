from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from breweries_pipeline.quality.checks import assert_quality_or_raise, evaluate_silver_quality


@pytest.fixture(scope="session")
def spark():
    try:
        return SparkSession.builder.appName("pytest-quality").master("local[1]").getOrCreate()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Spark unavailable in test environment: {exc}")


def test_evaluate_silver_quality_passes_for_valid_dataset(spark):
    run_id = "20260302T120000"
    data = [
        {
            "id": "1",
            "brewery_type": "micro",
            "country": "US",
            "state_province": "CA",
            "city": "Los Angeles",
            "ingestion_run_id": run_id,
        },
        {
            "id": "2",
            "brewery_type": "regional",
            "country": "US",
            "state_province": "CA",
            "city": "San Diego",
            "ingestion_run_id": run_id,
        },
    ]

    df = spark.createDataFrame(data)
    report = evaluate_silver_quality(df, run_id=run_id, min_rows=1)

    assert report.status == "passed"
    assert report.row_count == 2
    assert_quality_or_raise(report)


def test_evaluate_silver_quality_fails_for_null_id_and_wrong_run(spark):
    run_id = "20260302T120000"
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("country", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("city", StringType(), True),
            StructField("ingestion_run_id", StringType(), True),
        ]
    )
    data = [
        {
            "id": None,
            "brewery_type": "micro",
            "country": "US",
            "state_province": "CA",
            "city": "Los Angeles",
            "ingestion_run_id": "other_run",
        }
    ]

    df = spark.createDataFrame(data, schema=schema)
    report = evaluate_silver_quality(df, run_id=run_id, min_rows=1)

    assert report.status == "failed"
    failed_critical_names = {check.name for check in report.checks if (not check.passed and check.severity == "critical")}
    assert "null_id_ratio" in failed_critical_names
    assert "ingestion_run_id_consistency" in failed_critical_names

    with pytest.raises(RuntimeError, match="Silver data quality checks failed"):
        assert_quality_or_raise(report)
