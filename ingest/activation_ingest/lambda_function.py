"""
Lambda: activation-ingest
---------------------------
Consumes SIM activation events from Kinesis `sim-activation-stream`
and writes them to S3 `telco-fraud-bronze/raw/activation/`.

Design:
  - Invoked by Kinesis event source mapping (batch of records).
  - Buffers records, writes one JSONL file per invocation.
  - Timestamp-based S3 partition keys (Hive-style) for Athena compatibility.
  - Silent on success (CloudWatch logs everything anyway).

Event format (from Kinesis):
  {
    "Records": [
      {
        "kinesis": {
          "data": "<base64-encoded JSON>",
          "sequenceNumber": "...",
          "partitionKey": "SUB00123"
        },
        ...
      }
    ]
  }

Output:
  s3://telco-fraud-bronze/raw/activation/year=YYYY/month=MM/day=DD/<uuid>.jsonl
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



# ─── Configuration ──────────────────────────────────────────────────────────
# All values are overridable via Lambda environment variables. Defaults are
# aimed at LocalStack running on the host (for local testing).

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("BRONZE_BUCKET", "telco-fraud-bronze")
PREFIX = os.environ.get("ACTIVATION_PREFIX", "raw/activation")


# ─── S3 Client ──────────────────────────────────────────────────────────────
# LocalStack accepts any credentials. Passing explicit dummy values keeps
# boto3 from searching the standard credential chain (which fails on hosts
# with no ~/.aws/credentials).

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

    # Decode all Kinesis records
    decoded: list[dict] = []
    for r in records:
        try:
            decoded.append(_decode_record(r))
        except Exception as e:
            # Log and skip malformed records — don't crash the whole batch
            print(f"WARN: failed to decode record: {e}")
            continue

    if not decoded:
        return {"statusCode": 200, "body": "no valid records"}

    # Write one JSONL file per invocation
    now = datetime.now(timezone.utc)
    key = f"{_partition_key(now)}/{uuid.uuid4().hex}.jsonl"
    _write_jsonl(decoded, key)

    print(f"Wrote {len(decoded)} records to s3://{BUCKET}/{key}")

    return {
        "statusCode": 200,
        "body": f"wrote {len(decoded)} records",
        "s3_key": key,
    }