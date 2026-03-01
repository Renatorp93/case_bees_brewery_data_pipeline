from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Timezone-aware UTC now()."""
    return datetime.now(timezone.utc)


def utc_run_id(fmt: str = "%Y%m%dT%H%M%S") -> str:
    """Default run_id format used across the pipeline."""
    return utc_now().strftime(fmt)