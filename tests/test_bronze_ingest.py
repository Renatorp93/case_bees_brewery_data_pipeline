import json
import time

import pytest
import requests

import breweries_pipeline.jobs.bronze_ingest as mod


class FakeRDD:
    def __init__(self, parent):
        self.parent = parent

    def saveAsTextFile(self, path):
        self.parent.saved_path = path


class FakeSparkContext:
    def __init__(self, parent):
        self.parent = parent

    def parallelize(self, lines, numSlices=None):
        self.parent.lines = lines
        self.parent.numSlices = numSlices
        return FakeRDD(self.parent)


class FakeSpark:
    def __init__(self):
        self.lines = None
        self.numSlices = None
        self.saved_path = None
        self.sparkContext = FakeSparkContext(self)
        self.stopped = False

    def stop(self):
        self.stopped = True


def test_main_paginates_and_writes_manifest(monkeypatch):
    fake_spark = FakeSpark()
    monkeypatch.setattr(mod, "build_spark", lambda _: fake_spark)
    monkeypatch.setattr(mod, "exists", lambda spark, path: False)
    monkeypatch.setattr(mod, "delete", lambda *a, **k: None)

    written = {}

    def _fake_write_text(spark, path, content):
        written["path"] = path
        written["content"] = content

    monkeypatch.setattr(mod, "write_text", _fake_write_text)

    pages = {
        1: [{"id": 1}, {"id": 2}],
        2: [{"id": 3}],
        3: [],
    }

    class FakeResp:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload
            self.response = self

        def raise_for_status(self):
            if self.status_code >= 400:
                err = requests.HTTPError()
                err.response = self
                raise err

        def json(self):
            return self._payload

    class FakeSession:
        def get(self, url, params, timeout):
            return FakeResp(200, pages[params["page"]])

        def close(self):
            return None

    monkeypatch.setattr(mod.requests, "Session", lambda: FakeSession())

    run_id = "20260228T120000"
    out_prefix = "s3a://datalake/bronze/breweries"

    mod.main(
        run_id=run_id,
        per_page=200,
        max_pages=10,
        out_prefix=out_prefix,
        timeout_s=30,
        write_mode="skip",
    )

    assert fake_spark.saved_path == f"{out_prefix}/run_id={run_id}"
    assert len(fake_spark.lines) == 3
    assert fake_spark.stopped is True

    assert written["path"].endswith(f"/run_id={run_id}/_metadata.json")
    manifest = json.loads(written["content"])
    assert manifest["run_id"] == run_id
    assert manifest["fetched_rows"] == 3
    assert manifest["pages_fetched"] == 2


def test_main_fail_fast_on_empty(monkeypatch):
    fake_spark = FakeSpark()
    monkeypatch.setattr(mod, "build_spark", lambda _: fake_spark)
    monkeypatch.setattr(mod, "exists", lambda spark, path: False)
    monkeypatch.setattr(mod, "delete", lambda *a, **k: None)
    monkeypatch.setattr(
        mod,
        "write_text",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not write")),
    )

    class FakeResp:
        status_code = 200

        def json(self):
            return []

        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, params, timeout):
            return FakeResp()

        def close(self):
            return None

    monkeypatch.setattr(mod.requests, "Session", lambda: FakeSession())

    with pytest.raises(RuntimeError, match="returned 0 rows"):
        mod.main(
            run_id="x",
            per_page=200,
            max_pages=10,
            out_prefix="s3a://datalake/bronze/breweries",
            timeout_s=30,
            write_mode="skip",
        )


def test_fetch_page_retries_on_429(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    calls = {"n": 0}

    class Resp:
        def __init__(self, status):
            self.status_code = status
            self.response = self

        def raise_for_status(self):
            if self.status_code >= 400:
                err = requests.HTTPError()
                err.response = self
                raise err

        def json(self):
            return [{"ok": True}]

    class Sess:
        def get(self, url, params, timeout):
            calls["n"] += 1
            if calls["n"] == 1:
                return Resp(429)
            return Resp(200)

    out = mod.fetch_page(Sess(), page=1, per_page=200, timeout_s=1)
    assert calls["n"] == 2
    assert out == [{"ok": True}]
