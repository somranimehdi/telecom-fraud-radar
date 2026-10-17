"""Publish the local trained model and feature-only daily partitions to LocalStack S3."""

from __future__ import annotations

import io
import json
from pathlib import Path

import boto3
import polars as pl


ROOT = Path(__file__).resolve().parents[1]
MODEL_OUT = ROOT / "model" / "out"
BUCKET = "telco-fraud-bronze"
MODEL_PREFIX = "models/fraud_gbm/v1/"
FEATURE_PREFIX = "gold/model_features/"
FORBIDDEN_COLUMNS = {
    "label", "is_fraud", "archetype", "injection_day", "notes", "split",
}
ARTIFACTS = ("model.joblib", "feature_columns.json", "model_metrics.json")


def build_daily_feature_frames(
    table: pl.DataFrame,
    feature_columns: list[str],
) -> dict[str, pl.DataFrame]:
    """Return deterministic per-date frames containing only account metadata and features."""
    forbidden = FORBIDDEN_COLUMNS.intersection(feature_columns)
    if forbidden:
        raise ValueError(f"Feature list contains target/label metadata: {sorted(forbidden)}")
    required = {"subscriber_id", "date", *feature_columns}
    missing = sorted(required - set(table.columns))
    if missing:
        raise ValueError(f"Training table is missing publish columns: {missing}")

    output: dict[str, pl.DataFrame] = {}
    safe_columns = ["subscriber_id", "date", *feature_columns]
    for day in table.get_column("date").unique().sort().to_list():
        date_text = day.isoformat() if hasattr(day, "isoformat") else str(day)
        date_text = date_text[:10]
        output[date_text] = table.filter(pl.col("date") == day).select(safe_columns).sort("subscriber_id")
    return output


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def publish(s3, table: pl.DataFrame, feature_columns: list[str], model_out: Path = MODEL_OUT) -> list[str]:
    """Upload the three versioned artifacts and deterministic feature partitions."""
    keys: list[str] = []
    for filename in ARTIFACTS:
        key = MODEL_PREFIX + filename
        s3.put_object(Bucket=BUCKET, Key=key, Body=(model_out / filename).read_bytes())
        keys.append(key)

    for day, frame in build_daily_feature_frames(table, feature_columns).items():
        year, month, day_of_month = day.split("-")
        key = f"{FEATURE_PREFIX}year={year}/month={month}/day={day_of_month}/features.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=_parquet_bytes(frame))
        keys.append(key)
    print(f"Uploaded {len(keys)} objects to s3://{BUCKET}")
    for key in keys:
        print(f"s3://{BUCKET}/{key}")
    return keys


def main() -> None:
    with (MODEL_OUT / "feature_columns.json").open(encoding="utf-8") as handle:
        feature_columns = json.load(handle)
    table = pl.read_parquet(MODEL_OUT / "training_table.parquet")
    s3 = boto3.client(
        "s3", endpoint_url="http://localhost:4566", region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
    )
    publish(s3, table, feature_columns)


if __name__ == "__main__":
    main()
