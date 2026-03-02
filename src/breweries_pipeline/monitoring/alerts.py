from __future__ import annotations

import json
import logging
import os
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


def _to_bool(value: Optional[str], *, default: bool = False) -> bool:
    """Parse environment-like truthy values into bool."""

    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _split_csv(value: Optional[str]) -> Tuple[str, ...]:
    """Split comma-separated values and remove empty entries."""

    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class EmailAlertConfig:
    """SMTP and routing configuration for operational alerts."""

    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    sender: str
    recipients: Tuple[str, ...]
    use_starttls: bool
    use_ssl: bool
    send_success_email: bool

    @property
    def enabled(self) -> bool:
        return bool(self.smtp_host and self.sender and self.recipients)

    @classmethod
    def from_env(cls) -> "EmailAlertConfig":
        """Build alert configuration from process environment variables."""

        return cls(
            smtp_host=os.getenv("SMTP_HOST", "").strip(),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_user=os.getenv("SMTP_USER", "").strip(),
            smtp_password=os.getenv("SMTP_PASSWORD", ""),
            sender=os.getenv("ALERT_EMAIL_FROM", "").strip(),
            recipients=_split_csv(os.getenv("ALERT_EMAIL_TO", "")),
            use_starttls=_to_bool(os.getenv("SMTP_STARTTLS"), default=True),
            use_ssl=_to_bool(os.getenv("SMTP_SSL"), default=False),
            send_success_email=_to_bool(os.getenv("ALERT_EMAIL_ON_SUCCESS"), default=False),
        )


def build_context_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    """Extract a compact, JSON-serializable monitoring payload from Airflow context."""

    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    exc = context.get("exception")
    logical_date = context.get("logical_date")

    payload = {
        "dag_id": getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(dag_run, "run_id", None) or context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "state": getattr(ti, "state", None),
        "owner": getattr(ti, "task", None).owner if getattr(ti, "task", None) else None,
        "log_url": getattr(ti, "log_url", None),
        "logical_date": logical_date.isoformat() if hasattr(logical_date, "isoformat") else None,
        "exception": str(exc) if exc else None,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    return payload


def emit_monitoring_event(event_name: str, context: Dict[str, Any], *, level: int = logging.INFO) -> Dict[str, Any]:
    """Emit structured monitoring logs for downstream parsing."""

    payload = build_context_payload(context)
    payload["event"] = event_name
    logger.log(level, "[monitoring] %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return payload


def send_email_alert(
    *,
    subject: str,
    body: str,
    config: Optional[EmailAlertConfig] = None,
    recipients: Optional[Sequence[str]] = None,
) -> bool:
    """Send a plain-text e-mail alert through configured SMTP server."""

    cfg = config or EmailAlertConfig.from_env()
    to_recipients: Sequence[str] = tuple(recipients) if recipients is not None else cfg.recipients

    if not (cfg.enabled and to_recipients):
        logger.warning("[alert] e-mail disabled or incomplete SMTP config; skipping alert subject=%s", subject)
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.sender
    msg["To"] = ", ".join(to_recipients)
    msg.set_content(body)

    smtp_factory = smtplib.SMTP_SSL if cfg.use_ssl else smtplib.SMTP
    with smtp_factory(cfg.smtp_host, cfg.smtp_port, timeout=20) as smtp:
        if cfg.use_starttls and not cfg.use_ssl:
            smtp.starttls()
        if cfg.smtp_user:
            smtp.login(cfg.smtp_user, cfg.smtp_password)
        smtp.send_message(msg)

    logger.info("[alert] e-mail sent subject=%s recipients=%s", subject, list(to_recipients))
    return True


def _format_email_body(payload: Dict[str, Any], *, status_label: str) -> str:
    """Format a human-readable alert payload with key task metadata."""

    lines = [
        f"Status: {status_label}",
        f"DAG: {payload.get('dag_id')}",
        f"Task: {payload.get('task_id')}",
        f"Run ID: {payload.get('run_id')}",
        f"Try Number: {payload.get('try_number')}",
        f"State: {payload.get('state')}",
        f"Logical Date (UTC): {payload.get('logical_date')}",
        f"Exception: {payload.get('exception')}",
        f"Log URL: {payload.get('log_url')}",
        f"Captured At (UTC): {payload.get('captured_at_utc')}",
    ]
    return "\n".join(lines)


def on_task_failure(context: Dict[str, Any]) -> None:
    """Airflow callback for task failures: emit event + e-mail alert."""

    payload = emit_monitoring_event("task_failure", context, level=logging.ERROR)
    subject = f"[ALERT][{payload.get('dag_id')}] Task failed: {payload.get('task_id')}"
    send_email_alert(subject=subject, body=_format_email_body(payload, status_label="FAILED"))


def on_task_retry(context: Dict[str, Any]) -> None:
    """Airflow callback for task retry attempts."""

    payload = emit_monitoring_event("task_retry", context, level=logging.WARNING)
    subject = f"[WARN][{payload.get('dag_id')}] Task retry: {payload.get('task_id')}"
    send_email_alert(subject=subject, body=_format_email_body(payload, status_label="RETRY"))


def on_dag_success(context: Dict[str, Any]) -> None:
    """Airflow callback for DAG success; optional success e-mail."""

    payload = emit_monitoring_event("dag_success", context, level=logging.INFO)
    cfg = EmailAlertConfig.from_env()
    if not cfg.send_success_email:
        return
    subject = f"[INFO][{payload.get('dag_id')}] DAG succeeded"
    send_email_alert(subject=subject, body=_format_email_body(payload, status_label="SUCCEEDED"), config=cfg)


def on_dag_failure(context: Dict[str, Any]) -> None:
    """Airflow callback for DAG failures."""

    payload = emit_monitoring_event("dag_failure", context, level=logging.ERROR)
    subject = f"[ALERT][{payload.get('dag_id')}] DAG failed"
    send_email_alert(subject=subject, body=_format_email_body(payload, status_label="FAILED"))

