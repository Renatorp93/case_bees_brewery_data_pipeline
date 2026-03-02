from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from breweries_pipeline.guard.s3_guard import write_text
from breweries_pipeline.lib.spark import build_spark
from breweries_pipeline.quality.checks import assert_quality_or_raise, evaluate_silver_quality


def main(run_id: str, silver_prefix: str, quality_prefix: str, min_rows: int) -> None:
    """Run silver-layer data quality checks and persist report to S3A."""

    spark = build_spark("data-quality")
    try:
        silver_path = f"{silver_prefix.rstrip('/')}/breweries"
        df = spark.read.parquet(silver_path).filter(F.col("ingestion_run_id") == F.lit(run_id))

        report = evaluate_silver_quality(df, run_id=run_id, min_rows=min_rows)

        report_path = f"{quality_prefix.rstrip('/')}/silver/run_id={run_id}/report.json"
        write_text(spark, report_path, json.dumps(report.to_dict(), ensure_ascii=False, indent=2))

        print(f"[dq] report_path={report_path} status={report.status} row_count={report.row_count}")
        assert_quality_or_raise(report)
        print("[dq] critical checks passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--silver-prefix", default="s3a://datalake/silver")
    parser.add_argument("--quality-prefix", default="s3a://datalake/monitoring/data_quality")
    parser.add_argument("--min-rows", type=int, default=1)
    args = parser.parse_args()

    main(
        run_id=args.run_id,
        silver_prefix=args.silver_prefix,
        quality_prefix=args.quality_prefix,
        min_rows=args.min_rows,
    )

