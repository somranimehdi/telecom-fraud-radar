"""Build a leakage-safe subscriber-day training table from Silver and labels."""

from __future__ import annotations

import argparse
import io
import json
import random
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import boto3
import polars as pl
import yaml

from model.features import (
    build_daily_features,
    profile_features,
    skipped_features,
    with_event_date,
)
from transform.gold import SILVER_SOURCES, _list_parquet_keys


LABEL_COLUMNS = ["subscriber_id", "is_fraud", "archetype", "injection_day"]
METADATA_COLUMNS = ["subscriber_id", "date", "archetype", "split"]
TARGET_COLUMN = "label"
FORBIDDEN_FEATURES = {"is_fraud", "archetype", "injection_day", "notes", "label", "split"}
FRAUD_ARCHETYPES = ("sim_swap_takeover", "irsf", "subscription_fraud", "bundle_abuse")


def read_s3_parquet_prefix(s3, bucket: str, prefix: str) -> pl.DataFrame:
    frames = [
        pl.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))
        for key in _list_parquet_keys(s3, bucket, prefix)
    ]
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def read_latest_profile_snapshot(s3, bucket: str, prefix: str) -> pl.DataFrame:
    keys = _list_parquet_keys(s3, bucket, prefix)
    if not keys:
        raise ValueError(f"no subscriber profile snapshot found under s3://{bucket}/{prefix}")
    dated = []
    for key in keys:
        match = re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", key)
        dated.append((match.group(1) if match else "", key))
    latest = max(day for day, _ in dated)
    selected = [key for day, key in dated if day == latest]
    frames = [pl.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read())) for key in selected]
    frame = pl.concat(frames, how="diagonal_relaxed")
    return frame.unique(subset=["subscriber_id"], keep="last", maintain_order=True)


def derive_origin_and_latest(frames: dict[str, pl.DataFrame]) -> tuple[date, date]:
    dates = [
        value
        for frame in frames.values()
        for value in with_event_date(frame).get_column("date").drop_nulls().to_list()
    ]
    if not dates:
        raise ValueError("No valid event timestamps found in Silver sources")
    return min(dates), max(dates)


def sim_swap_match_rate(labels: pl.DataFrame, activation: pl.DataFrame, origin: date) -> float:
    """Fraction of SIM-swap fraud labels whose expected day has a SWAPPED event."""
    label_rows = labels.select(LABEL_COLUMNS).filter(
        (pl.col("is_fraud") == 1) & (pl.col("archetype") == "sim_swap_takeover")
    ).to_dicts()
    if not label_rows:
        return 0.0
    events = with_event_date(activation)
    if not {"subscriber_id", "date", "event_type"}.issubset(events.columns):
        return 0.0
    swaps = set(
        (str(row["subscriber_id"]), row["date"])
        for row in events.filter(pl.col("event_type").cast(pl.String).str.to_uppercase() == "SWAPPED")
        .select("subscriber_id", "date").to_dicts()
    )
    matches = sum(
        (str(row["subscriber_id"]), origin + timedelta(days=int(row["injection_day"]))) in swaps
        for row in label_rows
    )
    return matches / len(label_rows)


def subscriber_splits(labels: pl.DataFrame, seed: int = 42) -> dict[str, str]:
    """Create deterministic subscriber-level 70/30 splits stratified by archetype."""
    rows = labels.select("subscriber_id", "archetype").unique(subset="subscriber_id").to_dicts()
    strata: dict[str, list[str]] = {}
    for row in rows:
        strata.setdefault(str(row.get("archetype") or "none"), []).append(str(row["subscriber_id"]))
    rng = random.Random(seed)
    assignments: dict[str, str] = {}
    for archetype in sorted(strata):
        ids = sorted(strata[archetype])
        rng.shuffle(ids)
        test_size = round(len(ids) * 0.30)
        if len(ids) > 1:
            test_size = min(len(ids) - 1, max(1, test_size))
        else:
            test_size = 0
        assignments.update({sid: "test" for sid in ids[:test_size]})
        assignments.update({sid: "train" for sid in ids[test_size:]})
    return assignments


def assemble_training_table(
    daily_features: pl.DataFrame,
    labels: pl.DataFrame,
    origin: date,
    split_seed: int = 42,
) -> tuple[pl.DataFrame, list[str]]:
    """Join target/split metadata; label columns never enter the feature list."""
    clean_labels = labels.select(LABEL_COLUMNS).with_columns(pl.col("subscriber_id").cast(pl.String))
    split_map = subscriber_splits(clean_labels, split_seed)
    split_frame = pl.DataFrame({"subscriber_id": list(split_map), "split": list(split_map.values())})
    labeled = daily_features.with_columns(pl.col("subscriber_id").cast(pl.String)).join(
        clean_labels, on="subscriber_id", how="left"
    ).join(split_frame, on="subscriber_id", how="left")
    labeled = labeled.with_columns([
        pl.col("is_fraud").fill_null(0).cast(pl.Int8),
        pl.col("archetype").fill_null("none").cast(pl.String),
        pl.col("injection_day").fill_null(-1).cast(pl.Int32),
        pl.col("split").fill_null("train").cast(pl.String),
        ((pl.col("date") - pl.lit(origin)).dt.total_days()).cast(pl.Int32).alias("day_index"),
    ]).with_columns(
        ((pl.col("is_fraud") == 1) & (pl.col("day_index") >= pl.col("injection_day")))
        .cast(pl.Int8).alias(TARGET_COLUMN)
    ).drop(["is_fraud", "injection_day", "day_index"])

    metadata = set(METADATA_COLUMNS + [TARGET_COLUMN])
    feature_columns = [column for column in labeled.columns if column not in metadata and column not in FORBIDDEN_FEATURES]
    return labeled.select([*METADATA_COLUMNS, *feature_columns, TARGET_COLUMN]), feature_columns


def _summarize(table: pl.DataFrame, features: list[str], origin: date,
               match_rate: float, profiles: pl.DataFrame, skipped: dict[str, str]) -> None:
    positives = table.filter(pl.col("label") == 1)
    print(f"Origin date: {origin} (SIM_SWAP match rate: {match_rate:.1%})")
    print(f"Rows: {table.height:,}; positives: {positives.height:,}; positive rate: {positives.height / table.height:.4%}" if table.height else "Rows: 0; positives: 0; positive rate: 0.0000%")
    for split in ("train", "test"):
        subset = table.filter(pl.col("split") == split)
        rate = subset.filter(pl.col("label") == 1).height / subset.height if subset.height else 0.0
        print(f"  {split}: {subset.height:,} rows; positives {subset.filter(pl.col('label') == 1).height:,}; positive rate {rate:.4%}")
    print("Fraud subscribers by archetype:")
    fraud_subscribers = table.filter(pl.col("label") == 1).select(
        "split", "archetype", "subscriber_id"
    ).unique()
    for split in ("train", "test"):
        for archetype in FRAUD_ARCHETYPES:
            count = fraud_subscribers.filter(
                (pl.col("split") == split) & (pl.col("archetype") == archetype)
            ).height
            print(f"  {split} {archetype}: {count}")
    profile_columns = [column for column in profile_features(profiles).columns if column != "subscriber_id"]
    print(f"Features: {len(features)}; profile columns used: {profile_columns}")
    suspicious = [column for column in profile_columns if column == "risk_score_baseline"]
    if suspicious:
        print(f"  Suspicious profile column(s) to audit for label-derived risk: {suspicious}")
    print("  Excluded profile identifiers/operations: msisdn, imsi, last_updated; subscriber_id is metadata.")
    print(f"Skipped features: {skipped or 'none'}")


def build_from_inputs(
    frames: dict[str, pl.DataFrame], profiles: pl.DataFrame, labels: pl.DataFrame,
    high_risk_countries: list[str],
) -> tuple[pl.DataFrame, list[str], date, float, dict[str, str]]:
    """Pure in-memory orchestration used by CLI and tests."""
    origin, _latest = derive_origin_and_latest(frames)
    match_rate = sim_swap_match_rate(labels, frames["activation"], origin)
    if match_rate < 0.80:
        raise ValueError(
            f"SIM_SWAP alignment check failed: match rate {match_rate:.1%} is below 80%. "
            f"Origin date is {origin}; verify the event window and injection_day labels."
        )
    features = build_daily_features(
        frames["voice"], frames["data"], frames["revenue"], frames["purchases"],
        frames["activation"], profiles, high_risk_countries,
    )
    table, feature_columns = assemble_training_table(features, labels, origin)
    missing = skipped_features(frames, profiles)
    # The latest event date is derived from timestamps and is returned through the grid.
    return table, feature_columns, origin, match_rate, missing


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the subscriber-day fraud training table.")
    parser.add_argument("--config", default="simulator/out/config.used.yaml")
    parser.add_argument("--labels", default="simulator/out/labels.parquet")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()
    cfg = yaml.safe_load(Path(args.config).read_text())
    labels = pl.read_parquet(args.labels, columns=LABEL_COLUMNS)
    common = {
        "endpoint_url": args.endpoint, "region_name": "us-east-1",
        "aws_access_key_id": "test", "aws_secret_access_key": "test",
    }
    s3 = boto3.client("s3", **common)
    bucket = cfg["paths"]["s3_bucket"]
    frames = {source: read_s3_parquet_prefix(s3, bucket, prefix) for source, prefix in SILVER_SOURCES.items()}
    profiles = read_latest_profile_snapshot(s3, bucket, "gold/reference/subscriber_profile/")
    rules_cfg = yaml.safe_load(Path("scoring/rules_config.yaml").read_text())["rules"]
    table, feature_columns, origin, match_rate, skipped = build_from_inputs(
        frames, profiles, labels, rules_cfg["irsf_high_risk_countries"]
    )
    out_dir = Path("model/out")
    out_dir.mkdir(parents=True, exist_ok=True)
    table.write_parquet(out_dir / "training_table.parquet")
    (out_dir / "feature_columns.json").write_text(json.dumps(feature_columns, indent=2) + "\n")
    _summarize(table, feature_columns, origin, match_rate, profiles, skipped)
    print(f"Wrote {out_dir / 'training_table.parquet'} and {out_dir / 'feature_columns.json'}")


if __name__ == "__main__":
    main()
