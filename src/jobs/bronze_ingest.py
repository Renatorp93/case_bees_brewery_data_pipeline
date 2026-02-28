import argparse
import json
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests


API_URL = "https://api.openbrewerydb.org/v1/breweries"


def fetch_page(page: int, per_page: int, session: requests.Session, timeout_s: int = 30) -> List[Dict]:
    r = session.get(API_URL, params={"page": page, "per_page": per_page}, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def main(run_id: str, per_page: int, max_pages: int, out_prefix: str) -> None:
    session = requests.Session()

    all_rows: List[Dict] = []
    for page in range(1, max_pages + 1):
        for attempt in range(1, 4):
            try:
                rows = fetch_page(page, per_page, session)
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)
        if not rows:
            break
        all_rows.extend(rows)

    print(f"[bronze] run_id={run_id} rows={len(all_rows)} out={out_prefix}")

    tmp_path = f"/tmp/breweries_{run_id}.jsonl"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for row in all_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(tmp_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    parser.add_argument("--per-page", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--out-prefix", default="s3a://datalake/bronze/breweries")
    args = parser.parse_args()

    main(args.run_id, args.per_page, args.max_pages, args.out_prefix)