from __future__ import annotations

"""
Helpers and guards for s3a:// paths.

Design decision:
- Spark jobs use Hadoop FileSystem API (s3a://)
- Airflow guard uses boto3 (S3 API)
This keeps responsibilities separated and deterministic.
"""

from dataclasses import dataclass
from typing import Tuple

import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession


# ------------------------------------------------------------------
# Path helpers
# ------------------------------------------------------------------

@dataclass(frozen=True)
class BronzeGuardPaths:
    out_dir: str
    manifest_path: str


def bronze_paths(*, bronze_prefix: str, run_id: str) -> BronzeGuardPaths:
    out_dir = f"{bronze_prefix.rstrip('/')}/run_id={run_id}"
    manifest_path = f"{out_dir}/_metadata.json"
    return BronzeGuardPaths(out_dir=out_dir, manifest_path=manifest_path)


# ------------------------------------------------------------------
# Spark (Hadoop FS) helpers
# ------------------------------------------------------------------

def _fs_and_path(spark: SparkSession, uri: str):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(p.toUri(), hconf)
    return fs, p


def exists(spark: SparkSession, uri: str) -> bool:
    fs, p = _fs_and_path(spark, uri)
    return bool(fs.exists(p))


def delete(spark: SparkSession, uri: str, *, recursive: bool = True) -> None:
    fs, p = _fs_and_path(spark, uri)
    ok = fs.delete(p, recursive)
    if not ok:
        raise RuntimeError(f"Failed to delete path: {uri}")


def write_text(spark: SparkSession, uri: str, content: str) -> None:
    fs, p = _fs_and_path(spark, uri)
    out = fs.create(p, True)  # overwrite=True
    out.write(bytearray(content.encode("utf-8")))
    out.close()


def rename(spark: SparkSession, src: str, dst: str) -> None:
    fs_src, src_p = _fs_and_path(spark, src)
    _, dst_p = _fs_and_path(spark, dst)

    ok = fs_src.rename(src_p, dst_p)
    if not ok:
        raise RuntimeError(f"Failed to rename {src} -> {dst}")


# ------------------------------------------------------------------
# Airflow Guard (boto3)
# ------------------------------------------------------------------

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minio123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def guard_bronze_metadata(*, bucket: str, bronze_prefix: str, run_id: str) -> None:
    """
    Fail-fast guard used by Airflow.

    Checks if _metadata.json exists.
    If not, downstream layers (silver/gold) should not execute.
    """

    key = f"{bronze_prefix.rstrip('/')}/run_id={run_id}/_metadata.json"
    s3 = _s3_client()

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        raise RuntimeError(
            f"Bronze guard failed: manifest not found. bucket={bucket} key={key}"
        ) from e