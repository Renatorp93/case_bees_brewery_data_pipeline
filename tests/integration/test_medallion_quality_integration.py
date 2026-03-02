from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from breweries_pipeline.jobs.gold_aggregate import aggregate_gold
from breweries_pipeline.jobs.silver_curate import transform_silver
from breweries_pipeline.quality.checks import assert_quality_or_raise, evaluate_silver_quality


@pytest.fixture(scope="session")
def spark():
    try:
        return SparkSession.builder.appName("pytest-integration").master("local[1]").getOrCreate()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Spark unavailable in test environment: {exc}")


@pytest.mark.integration
def test_silver_to_quality_to_gold_flow(spark):
    run_id = "20260302T130000"
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("country", StringType(), True),
            StructField("state", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )

    raw_rows = [
        ("1", "A", "micro", "United States", "CA", None, "Los Angeles"),
        ("1", "A", "micro", "United States", "CA", None, "Los Angeles"),
        ("2", "B", "brewpub", "United States", "CA", "CA", "San Diego"),
    ]
    bronze_df = spark.createDataFrame(raw_rows, schema=schema)

    silver_df = transform_silver(bronze_df, run_id)
    quality_report = evaluate_silver_quality(silver_df, run_id=run_id, min_rows=1)
    assert_quality_or_raise(quality_report)

    duplicate_check = next(check for check in quality_report.checks if check.name == "duplicate_id_groups")
    assert duplicate_check.severity == "warning"
    assert duplicate_check.passed is False

    gold_df = aggregate_gold(silver_df)
    out = {(r["city"], r["brewery_type"]): r["brewery_count"] for r in gold_df.collect()}
    assert out[("Los Angeles", "micro")] == 1
    assert out[("San Diego", "brewpub")] == 1
