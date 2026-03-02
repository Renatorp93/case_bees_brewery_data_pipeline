from __future__ import annotations

"""
Helpers and guards for s3a:// paths.

Design decision:
- Spark jobs use Hadoop FileSystem API (s3a://)
- Airflow guard uses boto3 (S3 API)
This keeps responsibilities separated and deterministic.
"""

import os
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

@dataclass(frozen=True)
class BronzeGuardPaths:
    """Container for bronze output and manifest paths tied to one run."""

    out_dir: str
    manifest_path: str


def bronze_paths(*, bronze_prefix: str, run_id: str) -> BronzeGuardPaths:
    """Build bronze directory and manifest paths for a given run id."""

    out_dir = f"{bronze_prefix.rstrip('/')}/run_id={run_id}"
    manifest_path = f"{out_dir}/_metadata.json"
    return BronzeGuardPaths(out_dir=out_dir, manifest_path=manifest_path)

def _fs_and_path(spark: SparkSession, uri: str):
    """Return Hadoop FileSystem and Path handles for a URI."""

    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(p.toUri(), hconf)
    return fs, p


def exists(spark: SparkSession, uri: str) -> bool:
    """Check whether a path exists in the configured Hadoop filesystem."""

    fs, p = _fs_and_path(spark, uri)
    return bool(fs.exists(p))


def delete(spark: SparkSession, uri: str, *, recursive: bool = True) -> None:
    """Delete a path from the configured Hadoop filesystem."""

    fs, p = _fs_and_path(spark, uri)
    ok = fs.delete(p, recursive)
    if not ok:
        raise RuntimeError(f"Failed to delete path: {uri}")


def write_text(spark: SparkSession, uri: str, content: str) -> None:
    """Write UTF-8 text content to a filesystem path, overwriting if needed."""

    fs, p = _fs_and_path(spark, uri)
    out = fs.create(p, True)
    out.write(bytearray(content.encode("utf-8")))
    out.close()


def rename(spark: SparkSession, src: str, dst: str) -> None:
    """Rename or move a filesystem object between two URIs."""

    fs_src, src_p = _fs_and_path(spark, src)
    _, dst_p = _fs_and_path(spark, dst)

    ok = fs_src.rename(src_p, dst_p)
    if not ok:
        raise RuntimeError(f"Failed to rename {src} -> {dst}")


def _required_env(name: str) -> str:
    """Read a required env var and fail fast when it is missing/empty."""

    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _s3_client():
    """Build a boto3 S3 client using environment-backed MinIO credentials."""

    return boto3.client(
        "s3",
        endpoint_url=_required_env("S3_ENDPOINT"),
        aws_access_key_id=_required_env("S3_ACCESS_KEY"),
        aws_secret_access_key=_required_env("S3_SECRET_KEY"),
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
        error_code = e.response.get("Error", {}).get("Code", "unknown")
        raise RuntimeError(
            f"Bronze guard failed: manifest not found. "
            f"bucket={bucket} key={key} error_code={error_code}"
        ) from e
