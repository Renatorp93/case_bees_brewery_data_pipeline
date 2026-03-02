from __future__ import annotations

from datetime import datetime, timezone

import breweries_pipeline.monitoring.alerts as mod


class FakeSMTP:
    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.starttls_called = False
        self.logged_in = None
        self.messages = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def starttls(self):
        self.starttls_called = True

    def login(self, user, password):
        self.logged_in = (user, password)

    def send_message(self, msg):
        self.messages.append(msg)


def test_send_email_alert_success(monkeypatch):
    client = FakeSMTP("smtp.example.com", 587, 20)
    monkeypatch.setattr(mod.smtplib, "SMTP", lambda host, port, timeout: client)

    cfg = mod.EmailAlertConfig(
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_password="secret",
        sender="pipeline@example.com",
        recipients=("ops@example.com",),
        use_starttls=True,
        use_ssl=False,
        send_success_email=False,
    )

    sent = mod.send_email_alert(subject="Test Subject", body="Body", config=cfg)

    assert sent is True
    assert client.starttls_called is True
    assert client.logged_in == ("user", "secret")
    assert len(client.messages) == 1
    assert client.messages[0]["Subject"] == "Test Subject"
    assert client.messages[0]["To"] == "ops@example.com"


def test_send_email_alert_disabled_with_incomplete_config():
    cfg = mod.EmailAlertConfig(
        smtp_host="",
        smtp_port=587,
        smtp_user="",
        smtp_password="",
        sender="",
        recipients=(),
        use_starttls=True,
        use_ssl=False,
        send_success_email=False,
    )
    sent = mod.send_email_alert(subject="disabled", body="disabled", config=cfg)
    assert sent is False


def test_on_task_failure_builds_alert(monkeypatch):
    sent_payload = {}

    def _fake_send_email_alert(*, subject, body, config=None, recipients=None):
        sent_payload["subject"] = subject
        sent_payload["body"] = body
        return True

    monkeypatch.setattr(mod, "send_email_alert", _fake_send_email_alert)

    class Task:
        owner = "data-platform"

    class TaskInstance:
        dag_id = "breweries_medallion_pipeline"
        task_id = "dq_silver"
        try_number = 2
        state = "failed"
        log_url = "http://localhost:8080/log"
        task = Task()

    class DagRun:
        run_id = "manual__20260302T120000"

    class Dag:
        dag_id = "breweries_medallion_pipeline"

    context = {
        "task_instance": TaskInstance(),
        "dag_run": DagRun(),
        "dag": Dag(),
        "logical_date": datetime(2026, 3, 2, 12, 0, 0, tzinfo=timezone.utc),
        "exception": RuntimeError("DQ failed"),
    }

    mod.on_task_failure(context)

    assert "Task failed" in sent_payload["subject"]
    assert "DQ failed" in sent_payload["body"]
    assert "dq_silver" in sent_payload["body"]

