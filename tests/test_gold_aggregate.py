import pytest
from pyspark.sql import SparkSession

from breweries_pipeline.jobs.gold_aggregate import aggregate_gold


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("pytest")
        .master("local[1]")
        .getOrCreate()
    )


def test_aggregate_gold_counts_distinct_ids(spark):
    data = [
        {"id": "1", "country": "Brazil", "state_province": "SP", "city": "Sao Paulo", "brewery_type": "micro"},
        {"id": "2", "country": "Brazil", "state_province": "SP", "city": "Sao Paulo", "brewery_type": "micro"},
        {"id": "2", "country": "Brazil", "state_province": "SP", "city": "Sao Paulo", "brewery_type": "micro"},
        {"id": "3", "country": "Brazil", "state_province": "SP", "city": "Sao Paulo", "brewery_type": "brewpub"},
    ]
    df = spark.createDataFrame(data)

    out = aggregate_gold(df)

    rows = {(r["country"], r["state_province"], r["city"], r["brewery_type"]): r["brewery_count"] for r in out.collect()}

    assert rows[("Brazil", "SP", "Sao Paulo", "micro")] == 2
    assert rows[("Brazil", "SP", "Sao Paulo", "brewpub")] == 1
