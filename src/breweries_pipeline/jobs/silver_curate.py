from __future__ import annotations

import argparse
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from breweries_pipeline.lib.spark import build_spark
from breweries_pipeline.utils.time import utc_run_id


def transform_silver(df: DataFrame, run_id: str) -> DataFrame:
    """Normalize selected fields and attach ingestion metadata columns."""

    return (
        df
        .withColumn("ingestion_run_id", F.lit(run_id))
        .withColumn("ingestion_ts_utc", F.current_timestamp())
        .withColumn("country", F.coalesce(F.col("country"), F.lit("UNKNOWN")).cast(StringType()))
        .withColumn(
            "state_province",
            F.coalesce(F.col("state_province"), F.col("state"), F.lit("UNKNOWN")).cast(StringType()),
        )
        .withColumn("city", F.coalesce(F.col("city"), F.lit("UNKNOWN")).cast(StringType()))
    )


def main(run_id: str, bronze_prefix: str, silver_prefix: str) -> None:
    """Read bronze JSON data for a run and write curated silver parquet output."""

    spark = build_spark("silver-curate")

    bronze_path = f"{bronze_prefix}/run_id={run_id}"
    df = spark.read.json(bronze_path)

    out = transform_silver(df, run_id)

    silver_path = f"{silver_prefix}/breweries"
    (
        out.write.mode("overwrite")
        .partitionBy("country", "state_province", "city")
        .parquet(silver_path)
    )

    print(f"[silver] wrote {silver_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=utc_run_id())
    parser.add_argument("--bronze-prefix", default="s3a://datalake/bronze/breweries")
    parser.add_argument("--silver-prefix", default="s3a://datalake/silver")
    args = parser.parse_args()

    main(args.run_id, args.bronze_prefix, args.silver_prefix)
