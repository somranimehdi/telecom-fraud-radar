"""
Simulator — Labels Export
--------------------------
Takes the FraudLabel rows produced by fraud_injector and writes them to
Parquet. Also copies the exact config used, so any dataset is reproducible.

Output:
  simulator/out/labels.parquet         — ground truth (the sealed envelope)
  simulator/out/config.used.yaml       — snapshot of the config

The labels file is the SEALED ENVELOPE. It must never be joined to the
events during training feature engineering. It's read only in two places:
  1. Model training (to fit the target y).
  2. Model evaluation (to score precision / recall).
"""

from __future__ import annotations

import shutil
from dataclasses import asdict
from pathlib import Path

import polars as pl

from simulator.fraud_injector import FraudLabel


def write_labels(
    labels: list[FraudLabel],
    cfg: dict,
    config_path: str | Path = "simulator/config.yaml",
    out_dir: str | Path = "simulator/out",
) -> Path:
    """
    Write labels to Parquet + snapshot the config.
    Returns the path to the written Parquet file.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    labels_path = out_dir / "labels.parquet"
    config_snapshot_path = out_dir / "config.used.yaml"

    # Convert dataclasses to dicts, then to a Polars DataFrame
    rows = [asdict(l) for l in labels]
    df = pl.DataFrame(rows)

    # Enforce stable column order and types
    df = df.select([
        pl.col("subscriber_id").cast(pl.Utf8),
        pl.col("is_fraud").cast(pl.Int8),
        pl.col("archetype").cast(pl.Utf8),
        pl.col("injection_day").cast(pl.Int32),
        pl.col("notes").cast(pl.Utf8),
    ])

    df.write_parquet(labels_path)

    # Snapshot the config so this dataset can be reproduced.
    shutil.copyfile(config_path, config_snapshot_path)

    return labels_path


def load_labels(path: str | Path = "simulator/out/labels.parquet") -> pl.DataFrame:
    """Load labels for downstream use (training / evaluation)."""
    return pl.read_parquet(path)


def label_summary(df: pl.DataFrame) -> dict:
    """Return summary counts — useful for logging and README tables."""
    total = df.height
    n_fraud = df.filter(pl.col("is_fraud") == 1).height
    by_arch = (
        df.filter(pl.col("is_fraud") == 1)
        .group_by("archetype")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .to_dicts()
    )
    return {
        "total_subscribers": total,
        "fraud_subscribers": n_fraud,
        "fraud_rate": round(n_fraud / total, 4) if total else 0.0,
        "by_archetype": by_arch,
    }