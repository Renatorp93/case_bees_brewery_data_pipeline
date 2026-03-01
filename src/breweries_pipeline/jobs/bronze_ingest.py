from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Literal

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from breweries_pipeline.lib.spark import build_spark
from breweries_pipeline.guard.s3_guard import bronze_paths, delete, exists, write_text

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

API_URL = "https://api.openbrewerydb.org/v1/breweries"

WriteMode = Literal["skip", "overwrite", "fail"]


@dataclass(frozen=True)
class BronzeManifest:
    """Metadata emitted after a successful bronze ingestion run."""

    run_id: str
    fetched_rows: int
    pages_fetched: int
    per_page: int
    max_pages: int
    started_at_utc: str
    finished_at_utc: str
    api_url: str


def _is_retryable(exc: Exception) -> bool:
    """Return True when an HTTP exception is considered transient."""

    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True

    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in (429, 500, 502, 503, 504)

    return False


@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def fetch_page(session: requests.Session, page: int, per_page: int, timeout_s: int, api_url: str = API_URL):
    """Fetch one API page and raise for HTTP status >= 400."""

    r = session.get(api_url, params={"page": page, "per_page": per_page}, timeout=timeout_s)
    if r.status_code >= 400:
        r.raise_for_status()
    return r.json()


def _num_slices(n_lines: int) -> int:
    """Choose a bounded number of Spark partitions for text output."""

    return min(8, max(1, n_lines // 500))


def main(
    run_id: str,
    per_page: int,
    max_pages: int,
    out_prefix: str,
    timeout_s: int,
    write_mode: WriteMode,
) -> None:
    """Ingest breweries from the API and persist bronze data plus manifest."""

    started = datetime.now(timezone.utc)
    spark = build_spark("bronze-ingest")

    try:
        paths = bronze_paths(bronze_prefix=out_prefix, run_id=run_id)

        manifest_exists = exists(spark, paths.manifest_path)
        out_dir_exists = exists(spark, paths.out_dir)

        logger.info(
            "[bronze] run_id=%s write_mode=%s out_dir=%s manifest=%s",
            run_id,
            write_mode,
            paths.out_dir,
            paths.manifest_path,
        )

        if manifest_exists:
            if write_mode == "skip":
                logger.info("[bronze] manifest exists -> SKIP (success)")
                return
            if write_mode == "fail":
                raise RuntimeError(f"[bronze] manifest exists and write_mode=fail. manifest={paths.manifest_path}")
            logger.info("[bronze] manifest exists -> OVERWRITE: deleting %s", paths.out_dir)
            delete(spark, paths.out_dir, recursive=True)
            out_dir_exists = False

        if out_dir_exists and not manifest_exists:
            if write_mode == "fail":
                raise RuntimeError(
                    f"[bronze] out_dir exists but manifest missing (incomplete run) and write_mode=fail. "
                    f"out_dir={paths.out_dir}"
                )
            logger.info("[bronze] incomplete run detected (no manifest) -> deleting %s", paths.out_dir)
            delete(spark, paths.out_dir, recursive=True)

        session = requests.Session()
        try:
            all_rows: List[Dict] = []
            pages_fetched = 0

            for page in range(1, max_pages + 1):
                rows = fetch_page(session=session, page=page, per_page=per_page, timeout_s=timeout_s)
                if not rows:
                    break
                pages_fetched += 1
                all_rows.extend(rows)
        finally:
            session.close()

        if not all_rows:
            raise RuntimeError("[bronze] ingest returned 0 rows. Aborting to avoid empty downstream layers.")

        lines = [json.dumps(r, ensure_ascii=False) for r in all_rows]

        logger.info(
            "[bronze] writing dataset rows=%d pages=%d slices=%d -> %s",
            len(all_rows),
            pages_fetched,
            _num_slices(len(lines)),
            paths.out_dir,
        )

        spark.sparkContext.parallelize(lines, numSlices=_num_slices(len(lines))).saveAsTextFile(paths.out_dir)

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

        write_text(
            spark,
            paths.manifest_path,
            json.dumps(asdict(manifest), ensure_ascii=False, indent=2),
        )

        logger.info("[bronze] success out_dir=%s manifest=%s", paths.out_dir, paths.manifest_path)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    parser.add_argument("--per-page", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--timeout-s", type=int, default=30)
    parser.add_argument("--out-prefix", default="s3a://datalake/bronze/breweries")
    parser.add_argument(
        "--write-mode",
        choices=["skip", "overwrite", "fail"],
        default="skip",
        help="Behavior when output for the same run_id already exists.",
    )
    args = parser.parse_args()

    main(
        run_id=args.run_id,
        per_page=args.per_page,
        max_pages=args.max_pages,
        out_prefix=args.out_prefix,
        timeout_s=args.timeout_s,
        write_mode=args.write_mode,
    )
