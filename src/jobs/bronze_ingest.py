from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from src.lib.spark import build_spark

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

API_URL = "https://api.openbrewerydb.org/v1/breweries"


@dataclass(frozen=True)
class BronzeManifest:
    run_id: str
    fetched_rows: int
    pages_fetched: int
    per_page: int
    max_pages: int
    started_at_utc: str
    finished_at_utc: str
    api_url: str


def _is_retryable(exc: Exception) -> bool:
    # network-level
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True

    # HTTP status-level
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        # retry only on throttling / transient server errors
        return status in (429, 500, 502, 503, 504)

    return False


@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def fetch_page(session: requests.Session, page: int, per_page: int, timeout_s: int) -> List[Dict]:
    r = session.get(API_URL, params={"page": page, "per_page": per_page}, timeout=timeout_s)
    if r.status_code >= 400:
        r.raise_for_status()
    return r.json()


def _hdfs_path_exists(spark, path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def _write_text_single_file(spark, path: str, content: str) -> None:
    """
    Writes a small text file (single object) to S3A using Hadoop FS API.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    p = jvm.org.apache.hadoop.fs.Path(path)
    out = fs.create(p, True)  # overwrite=True
    out.write(bytearray(content.encode("utf-8")))
    out.close()


def main(
    run_id: str,
    per_page: int,
    max_pages: int,
    out_prefix: str,
    timeout_s: int,
    skip_if_exists: bool,
) -> None:
    started = datetime.now(timezone.utc)

    spark = build_spark("bronze-ingest")

    out_dir = f"{out_prefix}/run_id={run_id}"

    if skip_if_exists and _hdfs_path_exists(spark, out_dir):
        logger.info("[bronze] run_id=%s already exists at %s -> skipping", run_id, out_dir)
        spark.stop()
        return

    session = requests.Session()

    all_rows: List[Dict] = []
    pages_fetched = 0

    for page in range(1, max_pages + 1):
        rows = fetch_page(session=session, page=page, per_page=per_page, timeout_s=timeout_s)
        if not rows:
            break
        pages_fetched += 1
        all_rows.extend(rows)

    if len(all_rows) == 0:
        raise RuntimeError("Bronze ingest returned 0 rows. Aborting to avoid empty downstream layers.")

    lines = [json.dumps(r, ensure_ascii=False) for r in all_rows]

    spark.sparkContext.parallelize(lines, numSlices=min(8, max(1, len(lines) // 500))).saveAsTextFile(out_dir)

    finished = datetime.now(timezone.utc)

    manifest = BronzeManifest(
        run_id=run_id,
        fetched_rows=len(all_rows),
        pages_fetched=pages_fetched,
        per_page=per_page,
        max_pages=max_pages,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        api_url=API_URL,
    )

    _write_text_single_file(
        spark,
        f"{out_dir}/_metadata.json",
        json.dumps(asdict(manifest), ensure_ascii=False, indent=2),
    )

    logger.info("[bronze] wrote %s (rows=%d pages=%d)", out_dir, len(all_rows), pages_fetched)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    parser.add_argument("--per-page", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--timeout-s", type=int, default=30)
    parser.add_argument("--out-prefix", default="s3a://datalake/bronze/breweries")
    parser.add_argument("--skip-if-exists", action="store_true", default=True)
    args = parser.parse_args()

    main(
        run_id=args.run_id,
        per_page=args.per_page,
        max_pages=args.max_pages,
        out_prefix=args.out_prefix,
        timeout_s=args.timeout_s,
        skip_if_exists=args.skip_if_exists,
    )