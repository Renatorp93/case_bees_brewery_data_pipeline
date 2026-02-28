import argparse
from pyspark.sql import functions as F
from src.lib.spark import build_spark


def main(silver_prefix: str, gold_prefix: str) -> None:
    spark = build_spark("gold-aggregate")

    silver_path = f"{silver_prefix}/breweries"
    df = spark.read.parquet(silver_path)

    agg = (
        df.groupBy("country", "state_province", "city", "brewery_type")
        .agg(F.countDistinct("id").alias("brewery_count"))
        .orderBy(F.desc("brewery_count"))
    )

    gold_path = f"{gold_prefix}/breweries_by_type_location"
    agg.write.mode("overwrite").parquet(gold_path)

    print(f"[gold] wrote {gold_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-prefix", default="s3a://datalake/silver")
    parser.add_argument("--gold-prefix", default="s3a://datalake/gold")
    args = parser.parse_args()
    main(args.silver_prefix, args.gold_prefix)