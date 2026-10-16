"""Build account-day Gold features from the Silver Parquet datasets.

The output is an account-day fact table, suitable for Athena investigations
and as the starting point for model features. Ground-truth labels are never
read here. Each run replaces the materialized file for each simulated day.
"""

from __future__ import annotations

import argparse
import io
from datetime import date
from pathlib import Path
from typing import Any

import boto3
import polars as pl
import yaml


SILVER_SOURCES: dict[str, str] = {
    "activation": "silver/activation/",
    "purchases": "silver/bundles/purchases/",
    "voice": "silver/usage/voice/",
    "data": "silver/usage/data/",
    "revenue": "silver/revenue/prepaid/",
}
GOLD_PREFIX = "gold/account_daily_features/"
FEATURE_TYPES: dict[str, pl.DataType] = {
    "activation_event_count": pl.Int64,
    "sim_swap_count": pl.Int64,
    "bundle_purchase_count": pl.Int64,
    "bundle_spend": pl.Float64,
    "distinct_bundles_purchased": pl.Int64,
    "voice_call_count": pl.Int64,
    "international_call_count": pl.Int64,
    "voice_cost": pl.Float64,
    "voice_duration_sec": pl.Int64,
    "data_session_count": pl.Int64,
    "data_bytes": pl.Int64,
    "data_cost": pl.Float64,
    "revenue_transaction_count": pl.Int64,
    "net_revenue_amount": pl.Float64,
    "failed_payment_count": pl.Int64,
}


def _list_parquet_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(
            obj["Key"] for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        )
    return keys


def _read_silver(s3, bucket: str, prefix: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for key in _list_parquet_keys(s3, bucket, prefix):
        response = s3.get_object(Bucket=bucket, Key=key)
        frames.append(pl.read_parquet(io.BytesIO(response["Body"].read())))
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _daily_base(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "subscriber_id" not in df.columns or "timestamp" not in df.columns:
        return pl.DataFrame(schema={"subscriber_id": pl.String, "event_date": pl.Date})
    timestamp = pl.col("timestamp")
    if df.schema["timestamp"] == pl.String:
        event_date = timestamp.str.to_datetime(time_zone="UTC", strict=False).dt.date()
    else:
        event_date = timestamp.dt.date()
    return df.with_columns(event_date.alias("event_date"))


def _aggregate(s3, bucket: str, source: str) -> pl.DataFrame:
    df = _daily_base(_read_silver(s3, bucket, SILVER_SOURCES[source]))
    if df.is_empty():
        return pl.DataFrame(schema={"subscriber_id": pl.String, "event_date": pl.Date})

    keys = ["subscriber_id", "event_date"]
    # Silver files are run-scoped. Re-running the quality gate can leave older
    # copies in the same prefix, so collapse repeated source IDs before sums.
    id_field = {
        "activation": "event_id",
        "purchases": "purchase_id",
        "voice": "call_id",
        "data": "session_id",
        "revenue": "transaction_id",
    }[source]
    if id_field in df.columns:
        df = df.unique(subset=[id_field], keep="last", maintain_order=True)

    if source == "activation":
        return df.group_by(keys).agg(
            pl.len().cast(pl.Int64).alias("activation_event_count"),
            (pl.col("event_type") == "SWAPPED").cast(pl.Int64).sum().alias("sim_swap_count"),
        )
    if source == "purchases":
        return df.group_by(keys).agg(
            pl.len().cast(pl.Int64).alias("bundle_purchase_count"),
            pl.col("price").cast(pl.Float64).sum().alias("bundle_spend"),
            pl.col("bundle_id").n_unique().cast(pl.Int64).alias("distinct_bundles_purchased"),
        )
    if source == "voice":
        return df.group_by(keys).agg(
            pl.len().cast(pl.Int64).alias("voice_call_count"),
            (pl.col("country_code") != "TN").cast(pl.Int64).sum().alias("international_call_count"),
            pl.col("cost").cast(pl.Float64).sum().alias("voice_cost"),
            pl.col("duration_sec").cast(pl.Int64).sum().alias("voice_duration_sec"),
        )
    if source == "data":
        return df.group_by(keys).agg(
            pl.len().cast(pl.Int64).alias("data_session_count"),
            pl.col("bytes_used").cast(pl.Int64).sum().alias("data_bytes"),
            pl.col("cost").cast(pl.Float64).sum().alias("data_cost"),
        )
    return df.group_by(keys).agg(
        pl.len().cast(pl.Int64).alias("revenue_transaction_count"),
        pl.col("amount").cast(pl.Float64).sum().alias("net_revenue_amount"),
        (pl.col("status") == "failed").cast(pl.Int64).sum().alias("failed_payment_count"),
    )


def build_account_daily_features(s3, bucket: str) -> pl.DataFrame:
    """Outer-join source aggregates so activity from any source is retained."""
    aggregates = [_aggregate(s3, bucket, source) for source in SILVER_SOURCES]
    populated = [df for df in aggregates if not df.is_empty() and df.width > 2]
    if not populated:
        return pl.DataFrame()

    result = populated[0]
    for frame in populated[1:]:
        result = result.join(frame, on=["subscriber_id", "event_date"], how="full", coalesce=True)

    count_columns = [c for c in result.columns if c.endswith("_count")]
    sum_columns = [c for c in result.columns if c in {
        "bundle_spend", "voice_cost", "voice_duration_sec", "data_bytes",
        "data_cost", "net_revenue_amount",
    }]
    result = result.with_columns(
        [pl.col(c).fill_null(0) for c in count_columns + sum_columns]
    )
    missing = [
        pl.lit(0, dtype=dtype).alias(column)
        for column, dtype in FEATURE_TYPES.items()
        if column not in result.columns
    ]
    if missing:
        result = result.with_columns(missing)
    return result.select(["subscriber_id", "event_date", *FEATURE_TYPES.keys()]).sort(
        ["event_date", "subscriber_id"]
    )


def write_gold(s3, bucket: str, features: pl.DataFrame) -> list[str]:
    """Write one deterministic Parquet object per event date (safe to rerun)."""
    if features.is_empty():
        return []
    written: list[str] = []
    for event_date, daily in features.partition_by("event_date", as_dict=True).items():
        day = event_date[0] if isinstance(event_date, tuple) else event_date
        if not isinstance(day, date):
            day = date.fromisoformat(str(day))
        key = f"{GOLD_PREFIX}year={day:%Y}/month={day:%m}/day={day:%d}/account_daily_features.parquet"
        buffer = io.BytesIO()
        daily.drop("event_date").write_parquet(buffer)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        written.append(key)
    return written


def _scan_table(table) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    response = table.scan()
    while True:
        rows.extend(response.get("Items", []))
        key = response.get("LastEvaluatedKey")
        if not key:
            return rows
        response = table.scan(ExclusiveStartKey=key)


def write_reference_snapshots(s3, dynamodb, cfg: dict, bucket: str) -> list[str]:
    """Export small DynamoDB lookup tables for Athena investigations."""
    refs = (
        (cfg["paths"]["dynamodb_subscribers"], "gold/reference/subscriber_profile", "profiles.parquet"),
        (cfg["paths"]["dynamodb_bundles"], "gold/reference/bundle_catalog", "bundles.parquet"),
    )
    snapshot_date = date.today().isoformat()
    written: list[str] = []
    for table_name, prefix, filename in refs:
        rows = _scan_table(dynamodb.Table(table_name))
        if not rows:
            print(f"  [reference] {table_name}: empty, skipped")
            continue
        frame = pl.DataFrame(rows, infer_schema_length=None)
        key = f"{prefix}/snapshot_date={snapshot_date}/{filename}"
        buffer = io.BytesIO()
        frame.write_parquet(buffer)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        written.append(key)
        print(f"  [reference] {table_name}: {len(rows):,} rows → s3://{bucket}/{key}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize account-day Gold features from Silver.")
    parser.add_argument("--config", default="simulator/config.yaml")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()
    with Path(args.config).open() as handle:
        cfg = yaml.safe_load(handle)

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        region_name=cfg.get("region", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url=args.endpoint,
        region_name=cfg.get("region", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    bucket = cfg["paths"]["s3_bucket"]
    features = build_account_daily_features(s3, bucket)
    keys = write_gold(s3, bucket, features)
    print(f"Built {features.height:,} account-day rows across {len(keys)} Gold files")
    for key in keys:
        print(f"  s3://{bucket}/{key}")
    write_reference_snapshots(s3, dynamodb, cfg, bucket)


if __name__ == "__main__":
    main()
