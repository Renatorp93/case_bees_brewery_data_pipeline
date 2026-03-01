import pytest
import boto3
from botocore.stub import Stubber

import breweries_pipeline.guard.s3_guard as mod


def test_guard_ok(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    stubber = Stubber(s3)

    bucket = "datalake"
    bronze_prefix = "bronze/breweries"
    run_id = "20260226T212400"
    key = f"{bronze_prefix}/run_id={run_id}/_metadata.json"

    stubber.add_response(
        "head_object",
        service_response={"ResponseMetadata": {"HTTPStatusCode": 200}},
        expected_params={"Bucket": bucket, "Key": key},
    )

    with stubber:
        monkeypatch.setattr(mod, "_s3_client", lambda: s3)
        mod.guard_bronze_metadata(bucket=bucket, bronze_prefix=bronze_prefix, run_id=run_id)


def test_guard_missing_manifest_raises(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    stubber = Stubber(s3)

    bucket = "datalake"
    bronze_prefix = "bronze/breweries"
    run_id = "20260226T999999"
    key = f"{bronze_prefix}/run_id={run_id}/_metadata.json"

    stubber.add_client_error(
        "head_object",
        service_error_code="404",
        service_message="Not Found",
        http_status_code=404,
        expected_params={"Bucket": bucket, "Key": key},
    )

    with stubber:
        monkeypatch.setattr(mod, "_s3_client", lambda: s3)

        with pytest.raises(RuntimeError) as exc:
            mod.guard_bronze_metadata(bucket=bucket, bronze_prefix=bronze_prefix, run_id=run_id)

        msg = str(exc.value)
        assert "manifest not found" in msg
        assert bucket in msg
        assert key in msg
        assert "404" in msg
