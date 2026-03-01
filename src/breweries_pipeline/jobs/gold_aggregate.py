import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from breweries_pipeline.lib.spark import build_spark


def aggregate_gold(df: DataFrame) -> DataFrame:
    """Aggregate brewery counts by location and brewery type."""

    return (
        df.groupBy("country", "state_province", "city", "brewery_type")
        .agg(F.countDistinct("id").alias("brewery_count"))
        .orderBy(F.desc("brewery_count"))
    )


def main(run_id: str, silver_prefix: str, gold_prefix: str) -> None:
    """Read silver parquet, compute gold aggregates, and persist run-scoped output."""

    spark = build_spark("gold-aggregate")

    silver_path = f"{silver_prefix}/breweries"
    df = spark.read.parquet(silver_path)

    agg = aggregate_gold(df).withColumn("ingestion_run_id", F.lit(run_id))

    gold_path = f"{gold_prefix}/breweries_by_type_location/run_id={run_id}"
    agg.write.mode("overwrite").parquet(gold_path)

    print(f"[gold] wrote {gold_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--silver-prefix", default="s3a://datalake/silver")
    parser.add_argument("--gold-prefix", default="s3a://datalake/gold")
    args = parser.parse_args()
    main(args.run_id, args.silver_prefix, args.gold_prefix)
