"""Build today's model feature partition from the Silver event history."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
from datetime import date
from pathlib import Path

import boto3
import polars as pl
import yaml

from model.features import build_daily_features
from transform.gold import SILVER_SOURCES, _list_parquet_keys


ROOT = Path(__file__).resolve().parents[1]
BUCKET = "telco-fraud-bronze"
PROFILE_PREFIX = "gold/reference/subscriber_profile/"
FEATURE_PREFIX = "gold/model_features/"


def _clients(endpoint: str):
    return boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
    )


def _read_prefix(s3, bucket: str, prefix: str) -> pl.DataFrame:
    frames = [
        pl.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))
        for key in _list_parquet_keys(s3, bucket, prefix)
    ]
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _read_profiles(s3, bucket: str) -> pl.DataFrame:
    keys = _list_parquet_keys(s3, bucket, PROFILE_PREFIX)
    dated_keys = []
    for key in keys:
        match = re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", key)
        dated_keys.append((match.group(1) if match else "", key))
    if not dated_keys:
        raise ValueError(f"No subscriber profile snapshot found under s3://{bucket}/{PROFILE_PREFIX}")
    latest = max(day for day, _ in dated_keys)
    frames = [
        pl.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))
        for day, key in dated_keys if day == latest
    ]
    profiles = pl.concat(frames, how="diagonal_relaxed")
    return profiles.unique(subset=["subscriber_id"], keep="last", maintain_order=True)


def build_features_for_date(
    frames: dict[str, pl.DataFrame],
    profiles: pl.DataFrame,
    high_risk_countries: list[str],
    target_date: date,
    feature_columns: list[str],
) -> pl.DataFrame:
    """Build the requested subscriber-day features, without reading labels."""
    features = build_daily_features(
        frames["voice"], frames["data"], frames["revenue"], frames["purchases"],
        frames["activation"], profiles, high_risk_countries,
    )
    available_dates = set(features.get_column("date").to_list())
    if target_date not in available_dates:
        raise ValueError(f"No live Silver events for {target_date.isoformat()}")
    required = ["subscriber_id", "date", *feature_columns]
    missing = sorted(set(required) - set(features.columns))
    if missing:
        raise ValueError(f"Live feature builder is missing model columns: {missing}")
    result = (
        features.filter(pl.col("date") == target_date)
        .select(required)
        .sort("subscriber_id")
    )
    if result.is_empty():
        raise ValueError(f"No live feature rows for {target_date.isoformat()}")
    return result


def build_live_partition(s3, target_date: date, bucket: str = BUCKET) -> tuple[str, int]:
    # This builder is only for the live day. Never overwrite historical
    # offline model-feature partitions through this command.
    if target_date != date.today():
        raise ValueError(f"Live feature builds are restricted to today ({date.today()}); got {target_date}")

    frames = {
        source: _read_prefix(s3, bucket, prefix)
        for source, prefix in SILVER_SOURCES.items()
    }
    profiles = _read_profiles(s3, bucket)
    with (ROOT / "model" / "out" / "model_metrics.json").open(encoding="utf-8") as handle:
        feature_columns = json.load(handle)["feature_columns"]
    with (ROOT / "scoring" / "rules_config.yaml").open(encoding="utf-8") as handle:
        high_risk_countries = yaml.safe_load(handle)["rules"]["irsf_high_risk_countries"]

    result = build_features_for_date(frames, profiles, high_risk_countries, target_date, feature_columns)
    buffer = io.BytesIO()
    result.write_parquet(buffer)
    day = target_date.isoformat()
    year, month, day_of_month = day.split("-")
    key = f"{FEATURE_PREFIX}year={year}/month={month}/day={day_of_month}/features.parquet"
    s3.put_object(
        Bucket=bucket, Key=key, Body=buffer.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    print(f"Built features for {day}: {result.height} rows")
    return key, result.height


def main() -> None:
    parser = argparse.ArgumentParser(description="Build today's daily model features from Silver.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="Live date (YYYY-MM-DD)")
    parser.add_argument("--endpoint", default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"))
    parser.add_argument("--bucket", default=BUCKET)
    args = parser.parse_args()
    build_live_partition(_clients(args.endpoint), args.date, args.bucket)


if __name__ == "__main__":
    main()
