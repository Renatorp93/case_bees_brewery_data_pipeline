import argparse
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.lib.spark import build_spark


def main(run_id: str, bronze_prefix: str, silver_prefix: str) -> None:
    spark = build_spark("silver-curate")

    bronze_path = f"{bronze_prefix}/run_id={run_id}/*.jsonl"
    df = spark.read.json(bronze_path)

    df = (
        df
        .withColumn("ingestion_run_id", F.lit(run_id))
        .withColumn("ingestion_ts_utc", F.current_timestamp())
        .withColumn("country", F.coalesce(F.col("country"), F.lit("UNKNOWN")).cast(StringType()))
        .withColumn("state_province", F.coalesce(F.col("state_province"), F.col("state"), F.lit("UNKNOWN")).cast(StringType()))
        .withColumn("city", F.coalesce(F.col("city"), F.lit("UNKNOWN")).cast(StringType()))
    )

    silver_path = f"{silver_prefix}/breweries"
    (
        df.write.mode("overwrite")
        .partitionBy("country", "state_province", "city")
        .parquet(silver_path)
    )

    print(f"[silver] wrote {silver_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    parser.add_argument("--bronze-prefix", default="s3a://datalake/bronze/breweries")
    parser.add_argument("--silver-prefix", default="s3a://datalake/silver")
    args = parser.parse_args()
    main(args.run_id, args.bronze_prefix, args.silver_prefix)