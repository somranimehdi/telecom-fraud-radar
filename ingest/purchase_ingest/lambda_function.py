"""
Lambda: purchase-ingest
------------------------
Consumes bundle purchase events from Kinesis `bundle-purchases-stream`
and writes them to S3 `telco-fraud-bronze/raw/bundles/purchases/`.

This is the same shape as activation-ingest with two changes:
  - different Kinesis stream
  - different S3 prefix

Design notes:
  - Called by Kinesis event source mapping (batch of records).
  - Writes one JSONL file per invocation.
  - Hive-style partitions: year=/month=/day= for Athena compatibility.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3


# ─── Configuration ──────────────────────────────────────────────────────────

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("BRONZE_BUCKET", "telco-fraud-bronze")
PREFIX = os.environ.get("PURCHASES_PREFIX", "raw/bundles/purchases")


# ─── S3 Client ──────────────────────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=AWS_REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _decode_record(record: dict) -> dict:
    """Decode a Kinesis record's base64 payload into a Python dict."""
    raw = base64.b64decode(record["kinesis"]["data"])
    return json.loads(raw)


def _partition_key(now: datetime) -> str:
    """Build the S3 prefix with Hive-style date partitions."""
    return (
        f"{PREFIX}/"
        f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
    )


def _write_jsonl(rows: list[dict], key: str) -> None:
    """Write a list of dicts as JSONL to S3."""
    body = "\n".join(json.dumps(r, separators=(",", ":"), default=str) for r in rows)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


# ─── Handler ────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Entry point for the Lambda. Called by LocalStack's event source mapping.
    """
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "no records"}

    decoded: list[dict] = []
    for r in records:
        try:
            decoded.append(_decode_record(r))
        except Exception as e:
            print(f"WARN: failed to decode record: {e}")
            continue

    if not decoded:
        return {"statusCode": 200, "body": "no valid records"}

    now = datetime.now(timezone.utc)
    key = f"{_partition_key(now)}/{uuid.uuid4().hex}.jsonl"
    _write_jsonl(decoded, key)

    print(f"Wrote {len(decoded)} records to s3://{BUCKET}/{key}")

    return {
        "statusCode": 200,
        "body": f"wrote {len(decoded)} records",
        "s3_key": key,
    }