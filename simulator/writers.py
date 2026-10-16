"""
Simulator — Writers
--------------------
Pushes in-memory events to LocalStack (Kinesis, S3, DynamoDB).

Design:
  - All AWS IO lives here. run.py doesn't touch boto3.
  - Decimal → str, datetime → ISO 8601 for JSON serialization.
  - Kinesis uses PutRecords in batches of 500 (from config).
  - S3 batch files are written as Parquet with Hive-style partitions
    (year=YYYY/month=MM/day=DD/).
  - DynamoDB uses the boto3 RESOURCE interface (plain Python types),
    because batch_writer() is a resource method.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import boto3
import polars as pl


# ─── Serialization Helpers ──────────────────────────────────────────────────

class _JSONEncoder(json.JSONEncoder):
    """Handles Decimal and datetime for JSON serialization."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _to_json(obj: Any) -> str:
    return json.dumps(obj, cls=_JSONEncoder, separators=(",", ":"))


# ─── Boto3 Client Factory ───────────────────────────────────────────────────

def make_clients(endpoint: str = "http://localhost:4566", region: str = "us-east-1") -> dict:
    """
    Create boto3 clients/resources pointed at LocalStack.
    Uses dummy credentials (LocalStack accepts anything).

    Note: DynamoDB uses the RESOURCE interface because `batch_writer()`
    is a resource-level method.
    """
    kwargs = {
        "endpoint_url": endpoint,
        "region_name": region,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return {
        "kinesis": boto3.client("kinesis", **kwargs),
        "s3": boto3.client("s3", **kwargs),
        "dynamodb": boto3.resource("dynamodb", **kwargs),
    }


# ─── Kinesis Writer ─────────────────────────────────────────────────────────

def write_events_to_kinesis(
    kinesis,
    stream_name: str,
    events: Iterable[dict],
    batch_size: int = 500,
    verbose: bool = True,
) -> dict:
    """
    Push events to a Kinesis stream using PutRecords in batches.
    Returns a summary: {sent, failed, batches}.
    """
    sent = 0
    failed = 0
    batches = 0
    batch: list[dict] = []

    def _flush():
        nonlocal sent, failed, batches
        if not batch:
            return
        records = [
            {
                "Data": _to_json(evt).encode("utf-8"),
                "PartitionKey": evt.get("subscriber_id", "unknown"),
            }
            for evt in batch
        ]
        resp = kinesis.put_records(StreamName=stream_name, Records=records)
        failed_count = resp.get("FailedRecordCount", 0)
        sent += len(records) - failed_count
        failed += failed_count
        batches += 1
        batch.clear()

    for evt in events:
        batch.append(evt)
        if len(batch) >= batch_size:
            _flush()
    _flush()

    if verbose:
        print(f"      → {stream_name}: {sent:,} sent, {failed:,} failed in {batches} batches")

    return {"sent": sent, "failed": failed, "batches": batches}


# ─── S3 Batch Writer ────────────────────────────────────────────────────────

def _s3_partition_key(base_prefix: str, timestamp: datetime) -> str:
    """
    Build a Hive-partitioned S3 key.
    Example: raw/usage/voice/year=2026/month=09/day=15/data.parquet
    """
    base = base_prefix.rstrip("/")
    return f"{base}/year={timestamp.year:04d}/month={timestamp.month:02d}/day={timestamp.day:02d}"


def write_parquet_to_s3(
    s3,
    bucket: str,
    base_prefix: str,
    rows: list[dict],
    timestamp: datetime,
    filename: str = "data.parquet",
    verbose: bool = True,
) -> str:
    """
    Write rows as a Parquet file to S3 under a date-partitioned prefix.
    Returns the S3 key written.
    """
    if not rows:
        if verbose:
            print(f"      → {base_prefix}: no rows to write")
        return ""

    df = pl.DataFrame(rows)

    # Ensure Decimal/object columns serialize cleanly to Parquet
    for col in df.columns:
        if df[col].dtype == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    # Write to a local temp file, then upload
    tmp_path = Path(f"/tmp/{filename}")
    df.write_parquet(tmp_path)

    key = f"{_s3_partition_key(base_prefix, timestamp)}/{filename}"
    s3.upload_file(str(tmp_path), bucket, key)

    if verbose:
        print(f"      → s3://{bucket}/{key} ({df.height:,} rows)")

    return key


# ─── DynamoDB Writers ───────────────────────────────────────────────────────

def write_subscribers_to_dynamodb(
    dynamodb,
    table_name: str,
    subscribers: list,
    verbose: bool = True,
) -> int:
    """
    Load subscriber profiles into DynamoDB.
    Uses the resource API (plain Python types, not typed wrappers).
    """
    written = 0
    table = dynamodb.Table(table_name)
    with table.batch_writer() as bw:
        for sub in subscribers:
            item = {
                "subscriber_id": sub.subscriber_id,
                "msisdn": sub.msisdn,
                "imsi": sub.imsi,
                "segment": sub.segment,
                "kyc_status": sub.kyc_status,
                "account_age_days": int(sub.account_age_days),
                "lines_per_identity": int(sub.lines_per_identity),
                "risk_score_baseline": str(sub.risk_score_baseline),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            bw.put_item(Item=item)
            written += 1

    if verbose:
        print(f"      → {table_name}: {written:,} items")

    return written


def write_bundles_to_dynamodb(
    dynamodb,
    table_name: str,
    bundles: list,
    verbose: bool = True,
) -> int:
    """
    Load bundle catalog into DynamoDB.
    Uses the resource API (plain Python types).
    """
    written = 0
    table = dynamodb.Table(table_name)
    with table.batch_writer() as bw:
        for b in bundles:
            item = {
                "bundle_id": b.bundle_id,
                "name": b.name,
                "price": str(b.price),
                "allowance_type": b.allowance_type,
                "allowance_amount": int(b.allowance_amount),
                "validity_days": int(b.validity_days),
                "segment": b.segment,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            bw.put_item(Item=item)
            written += 1

    if verbose:
        print(f"      → {table_name}: {written:,} items")

    return written


# ─── Orchestrator for All Writes ────────────────────────────────────────────

def write_all(
    collector,
    subscribers: list,
    cfg: dict,
    clients: dict | None = None,
) -> dict:
    """
    Push everything from run.py's EventCollector to LocalStack.
    """
    if clients is None:
        clients = make_clients()

    from simulator import constants as C

    bucket = cfg["paths"]["s3_bucket"]
    act_stream = cfg["paths"]["kinesis_activation_stream"]
    purch_stream = cfg["paths"]["kinesis_purchases_stream"]
    subs_table = cfg["paths"]["dynamodb_subscribers"]
    bundles_table = cfg["paths"]["dynamodb_bundles"]
    batch_size = cfg["outputs"]["kinesis_batch_size"]

    start_dt = datetime.fromisoformat(cfg["population"]["start_date"]).replace(
        tzinfo=timezone.utc
    )

    summary: dict[str, Any] = {}

    print("─── Writing to LocalStack ─────────────────────")

    # 1. Kinesis: activation events
    if collector.activation_events and cfg["outputs"]["write_to_kinesis"]:
        summary["kinesis_activation"] = write_events_to_kinesis(
            clients["kinesis"], act_stream, collector.activation_events, batch_size
        )

    # 2. Kinesis: bundle purchases
    if collector.bundle_purchases and cfg["outputs"]["write_to_kinesis"]:
        summary["kinesis_purchases"] = write_events_to_kinesis(
            clients["kinesis"], purch_stream, collector.bundle_purchases, batch_size
        )

    # 3. S3: voice CDRs
    if collector.voice_cdrs and cfg["outputs"]["write_to_s3"]:
        summary["s3_voice"] = write_parquet_to_s3(
            clients["s3"], bucket, "raw/usage/voice",
            collector.voice_cdrs, start_dt, filename="voice.parquet",
        )

    # 4. S3: data CDRs
    if collector.data_cdrs and cfg["outputs"]["write_to_s3"]:
        summary["s3_data"] = write_parquet_to_s3(
            clients["s3"], bucket, "raw/usage/data",
            collector.data_cdrs, start_dt, filename="data.parquet",
        )

    # 5. S3: revenue transactions
    if collector.revenue_txs and cfg["outputs"]["write_to_s3"]:
        summary["s3_revenue"] = write_parquet_to_s3(
            clients["s3"], bucket, "raw/revenue/prepaid",
            collector.revenue_txs, start_dt, filename="revenue.parquet",
        )

    # 6. DynamoDB: subscriber profiles
    if cfg["outputs"]["write_to_dynamodb"]:
        summary["dynamodb_subscribers"] = write_subscribers_to_dynamodb(
            clients["dynamodb"], subs_table, subscribers,
        )

    # 7. DynamoDB: bundle catalog
    if cfg["outputs"]["write_to_dynamodb"]:
        summary["dynamodb_bundles"] = write_bundles_to_dynamodb(
            clients["dynamodb"], bundles_table, C.BUNDLE_CATALOG,
        )

    print("─────────────────────────────────────────────")
    return summary