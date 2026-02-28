from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class HttpClientConfig:
    timeout_s: int = 30
    connect_retries: int = 3
    backoff_factor: float = 0.5


def build_session(cfg: HttpClientConfig) -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=cfg.connect_retries,
        connect=cfg.connect_retries,
        read=cfg.connect_retries,
        status=0,
        backoff_factor=cfg.backoff_factor,
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session