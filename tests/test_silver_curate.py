import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from breweries_pipeline.jobs.silver_curate import transform_silver


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("pytest")
        .master("local[1]")
        .getOrCreate()
    )


def test_silver_transforms_and_normalizes_fields(spark):
    run_id = "20260228T000000"

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("city", StringType(), True),
    ])

    data = [
        ("1", "A", "micro", "United States", "CA", None, "LA"),
        ("2", "B", "micro", None, None, None, None),
    ]

    df = spark.createDataFrame(data, schema=schema)

    out = transform_silver(df, run_id)

    cols = set(out.columns)
    assert "brewery_type" in cols
    assert "country" in cols
    assert "state_province" in cols
    assert "city" in cols
    assert "ingestion_run_id" in cols
    assert "ingestion_ts_utc" in cols
    assert out.count() == 2

    # valida normalização (não fica null)
    rows = {r["id"]: r for r in out.select("id", "country", "state_province", "city").collect()}
    assert rows["2"]["country"] == "UNKNOWN"
    assert rows["2"]["state_province"] == "UNKNOWN"
    assert rows["2"]["city"] == "UNKNOWN"
