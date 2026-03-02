from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_SILVER_COLUMNS: Sequence[str] = (
    "id",
    "brewery_type",
    "country",
    "state_province",
    "city",
    "ingestion_run_id",
)


@dataclass(frozen=True)
class QualityCheckResult:
    """Container for one data quality rule evaluation."""

    name: str
    passed: bool
    severity: str
    metric: float
    threshold: str
    details: str


@dataclass(frozen=True)
class SilverQualityReport:
    """Structured quality report produced for each silver run."""

    run_id: str
    evaluated_at_utc: str
    row_count: int
    checks: List[QualityCheckResult]

    @property
    def status(self) -> str:
        failed_critical = any((not check.passed and check.severity == "critical") for check in self.checks)
        return "failed" if failed_critical else "passed"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "evaluated_at_utc": self.evaluated_at_utc,
            "row_count": self.row_count,
            "status": self.status,
            "checks": [asdict(check) for check in self.checks],
        }


def _make_check(name: str, passed: bool, severity: str, metric: float, threshold: str, details: str) -> QualityCheckResult:
    return QualityCheckResult(
        name=name,
        passed=bool(passed),
        severity=severity,
        metric=float(metric),
        threshold=threshold,
        details=details,
    )


def evaluate_silver_quality(
    df: DataFrame,
    *,
    run_id: str,
    min_rows: int = 1,
    max_null_id_ratio: float = 0.0,
) -> SilverQualityReport:
    """Evaluate quality checks for silver dataframe content."""

    checks: List[QualityCheckResult] = []
    row_count = df.count()

    missing_cols = [col for col in REQUIRED_SILVER_COLUMNS if col not in df.columns]
    checks.append(
        _make_check(
            "required_columns_present",
            passed=not missing_cols,
            severity="critical",
            metric=float(len(missing_cols)),
            threshold="missing_columns == 0",
            details=f"missing={missing_cols}",
        )
    )

    checks.append(
        _make_check(
            "minimum_rows",
            passed=row_count >= min_rows,
            severity="critical",
            metric=float(row_count),
            threshold=f"row_count >= {min_rows}",
            details=f"row_count={row_count}",
        )
    )

    if row_count == 0:
        # Remaining checks are not informative for empty datasets.
        return SilverQualityReport(
            run_id=run_id,
            evaluated_at_utc=datetime.now(timezone.utc).isoformat(),
            row_count=row_count,
            checks=checks,
        )

    if "id" in df.columns:
        null_id_count = df.filter(F.col("id").isNull()).count()
        null_id_ratio = null_id_count / float(row_count)
        checks.append(
            _make_check(
                "null_id_ratio",
                passed=null_id_ratio <= max_null_id_ratio,
                severity="critical",
                metric=null_id_ratio,
                threshold=f"null_id_ratio <= {max_null_id_ratio}",
                details=f"null_id_count={null_id_count}",
            )
        )

        duplicate_id_count = (
            df.filter(F.col("id").isNotNull())
            .groupBy("id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        checks.append(
            _make_check(
                "duplicate_id_groups",
                passed=duplicate_id_count == 0,
                severity="warning",
                metric=float(duplicate_id_count),
                threshold="duplicate_id_groups == 0",
                details="Warning-only check to monitor duplicate upstream IDs.",
            )
        )

    if "ingestion_run_id" in df.columns:
        inconsistent_run_rows = df.filter(F.col("ingestion_run_id") != F.lit(run_id)).count()
        checks.append(
            _make_check(
                "ingestion_run_id_consistency",
                passed=inconsistent_run_rows == 0,
                severity="critical",
                metric=float(inconsistent_run_rows),
                threshold="rows_with_wrong_run_id == 0",
                details=f"expected_run_id={run_id}",
            )
        )

    return SilverQualityReport(
        run_id=run_id,
        evaluated_at_utc=datetime.now(timezone.utc).isoformat(),
        row_count=row_count,
        checks=checks,
    )


def assert_quality_or_raise(report: SilverQualityReport) -> None:
    """Raise when any critical quality rule fails."""

    if report.status == "passed":
        return

    failed_checks = [check.name for check in report.checks if (not check.passed and check.severity == "critical")]
    raise RuntimeError(
        "Silver data quality checks failed. "
        f"run_id={report.run_id} failed_critical_checks={failed_checks}"
    )

