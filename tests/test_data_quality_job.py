from __future__ import annotations

import breweries_pipeline.jobs.data_quality as mod


class FakeDataFrame:
    def filter(self, *_args, **_kwargs):
        return self


class FakeReader:
    def __init__(self):
        self.last_path = None

    def parquet(self, path):
        self.last_path = path
        return FakeDataFrame()


class FakeSpark:
    def __init__(self):
        self.read = FakeReader()
        self.stopped = False

    def stop(self):
        self.stopped = True


class FakeReport:
    def __init__(self, status="passed", row_count=42):
        self.status = status
        self.row_count = row_count
        self.run_id = "run-1"

    def to_dict(self):
        return {"status": self.status, "row_count": self.row_count}


def test_data_quality_job_writes_report_and_stops_spark(monkeypatch):
    class FakeExpr:
        def __eq__(self, other):
            return ("eq", other)

    fake_spark = FakeSpark()
    written = {}
    fake_report = FakeReport(status="passed", row_count=7)

    monkeypatch.setattr(mod, "build_spark", lambda _app_name: fake_spark)
    monkeypatch.setattr(mod.F, "col", lambda _name: FakeExpr())
    monkeypatch.setattr(mod.F, "lit", lambda value: value)
    monkeypatch.setattr(mod, "evaluate_silver_quality", lambda df, run_id, min_rows: fake_report)
    monkeypatch.setattr(mod, "assert_quality_or_raise", lambda report: None)
    monkeypatch.setattr(
        mod,
        "write_text",
        lambda _spark, path, content: written.update({"path": path, "content": content}),
    )

    mod.main(
        run_id="20260302T120000",
        silver_prefix="s3a://datalake/silver",
        quality_prefix="s3a://datalake/monitoring/data_quality",
        min_rows=1,
    )

    assert fake_spark.read.last_path == "s3a://datalake/silver/breweries"
    assert written["path"].endswith("/silver/run_id=20260302T120000/report.json")
    assert "\"status\": \"passed\"" in written["content"]
    assert fake_spark.stopped is True
