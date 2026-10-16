"""
Quality Gate — I/O
--------------------
Read raw data from S3 and write Silver / Quarantine Parquet files.

Design:
  - Parquet for batch sources (voice, data, revenue).
  - JSONL for streaming sources (activation, purchases).
  - Both are read into Polars DataFrames.
  - Output is always Parquet (Silver and Quarantine) — columnar, compressed,
    queryable by Athena.
  - Overwrite semantics: re-running the gate replaces existing files.

All functions accept an explicit boto3 S3 client (from quality_gate.run or
tests), so this module is testable without hitting S3.
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import polars as pl


# ─── Reading ────────────────────────────────────────────────────────────────

def list_objects(s3, bucket: str, prefix: str) -> list[dict]:
    """
    List all objects under a prefix. Returns a list of dicts with keys:
      - Key, Size, LastModified
    Handles pagination.
    """
    out: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Skip the empty "folder placeholder" objects (keys ending in /)
            if obj["Key"].endswith("/"):
                continue
            out.append(obj)
    return out


def read_parquet_from_s3(s3, bucket: str, keys: list[str]) -> pl.DataFrame:
    """
    Read one or more Parquet files from S3 and concatenate them.
    Returns an empty DataFrame if no keys.
    """
    if not keys:
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []
    for key in keys:
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = resp["Body"].read()
        df = pl.read_parquet(io.BytesIO(data))
        frames.append(df)

    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def read_jsonl_from_s3(s3, bucket: str, keys: list[str]) -> pl.DataFrame:
    """
    Read one or more JSONL files from S3. Each line is one JSON object.
    Concatenates all files into one DataFrame.
    """
    if not keys:
        return pl.DataFrame()

    rows: list[dict[str, Any]] = []
    for key in keys:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip malformed lines — they'll be missing from the output
                # entirely. (A more strict gate could quarantine them.)
                continue

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def read_source(
    s3,
    bucket: str,
    prefix: str,
    file_type: str = "parquet",
) -> tuple[pl.DataFrame, int]:
    """
    Read all files under a prefix. Returns (DataFrame, file_count).

    file_type: "parquet" or "jsonl"
    """
    keys = [o["Key"] for o in list_objects(s3, bucket, prefix)]
    if not keys:
        return pl.DataFrame(), 0

    if file_type == "parquet":
        df = read_parquet_from_s3(s3, bucket, keys)
    elif file_type == "jsonl":
        df = read_jsonl_from_s3(s3, bucket, keys)
    else:
        raise ValueError(f"unknown file_type: {file_type}")

    return df, len(keys)


# ─── Writing ────────────────────────────────────────────────────────────────

def _now_partition_prefix(base_prefix: str, ts: datetime) -> str:
    """Hive-style date partition. Matches the pattern used by the simulator."""
    base = base_prefix.rstrip("/")
    return f"{base}/year={ts.year:04d}/month={ts.month:02d}/day={ts.day:02d}"


def write_parquet_to_s3(
    s3,
    bucket: str,
    base_prefix: str,
    df: pl.DataFrame,
    run_id: str,
    filename: str = "data.parquet",
    timestamp: datetime | None = None,
) -> str:
    """
    Write a DataFrame as a Parquet file under base_prefix with Hive partitioning.
    Returns the S3 key written (empty string if df is empty).
    """
    if df.is_empty():
        return ""

    ts = timestamp or datetime.now(timezone.utc)
    prefix = _now_partition_prefix(base_prefix, ts)
    key = f"{prefix}/{run_id}_{filename}"

    # Serialize to Parquet in memory
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    return key


def write_quarantine_to_s3(
    s3,
    bucket: str,
    base_prefix: str,
    quarantine_rows: list[dict],
    run_id: str,
    timestamp: datetime | None = None,
) -> str:
    """
    Write quarantined rows as a Parquet file.

    Each row must already include the metadata fields:
      reason, check_name, source, quarantined_at
    """
    if not quarantine_rows:
        return ""

    df = pl.DataFrame(quarantine_rows)
    return write_parquet_to_s3(
        s3, bucket, base_prefix, df, run_id,
        filename="quarantine.parquet",
        timestamp=timestamp,
    )


def write_metrics_to_s3(
    s3,
    bucket: str,
    base_prefix: str,
    payload: dict,
    run_id: str,
    timestamp: datetime | None = None,
) -> str:
    """
    Write a JSON metrics file to S3.
    Useful for downstream dashboards.
    """
    ts = timestamp or datetime.now(timezone.utc)
    prefix = _now_partition_prefix(base_prefix, ts)
    key = f"{prefix}/{run_id}.json"

    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key