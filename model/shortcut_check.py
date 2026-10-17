"""Single-feature label association checks for the simulated training table."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.metrics import roc_auc_score


OUT_DIR = Path(__file__).resolve().parent / "out"
FORBIDDEN_FEATURES = {"is_fraud", "archetype", "injection_day", "notes", "label", "split"}
PROFILE_MEANS = ["risk_score_baseline", "kyc_status", "account_age_days", "lines_per_identity"]


def single_feature_auc(frame: pl.DataFrame, feature: str, target: str = "label") -> float:
    """Return direction-independent ROC AUC for one numeric feature."""
    y = frame.get_column(target).cast(pl.Int8).to_numpy()
    x = frame.get_column(feature).cast(pl.Float64, strict=False).to_numpy()
    valid = np.isfinite(x) & np.isin(y, [0, 1])
    y, x = y[valid], x[valid]
    if len(np.unique(y)) < 2:
        return 0.5
    auc = float(roc_auc_score(y, x))
    return max(auc, 1.0 - auc)


def _status(auc: float) -> str:
    if auc > 0.95:
        return "SHORTCUT"
    if auc >= 0.85:
        return "STRONG"
    return ""


def _auc_table(frame: pl.DataFrame, features: list[str]) -> list[tuple[str, float, str]]:
    return sorted(
        ((feature, single_feature_auc(frame, feature), _status(single_feature_auc(frame, feature))) for feature in features),
        key=lambda item: (-item[1], item[0]),
    )


def _markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    lines.extend("| " + " | ".join(str(value) for value in row) + " |" for row in rows)
    return "\n".join(lines)


def build_report(table: pl.DataFrame, features: list[str]) -> str:
    train = table.filter(pl.col("split") == "train")
    missing = sorted(set(features) - set(train.columns))
    if missing:
        raise ValueError(f"Feature columns missing from training table: {missing}")
    forbidden = sorted(set(features) & FORBIDDEN_FEATURES)
    if forbidden:
        raise ValueError(f"Non-feature metadata/label columns in feature list: {forbidden}")

    overall = _auc_table(train, features)
    parts = ["# Shortcut check", "", "## Overall TRAIN split", "", _markdown_table(
        ["Feature", "Direction-independent AUC", "Flag"],
        [[name, f"{auc:.4f}", flag or "—"] for name, auc, flag in overall],
    )]

    archetypes = sorted(train.filter(pl.col("label") == 1).get_column("archetype").unique().to_list())
    # The negative class is the legitimate subscriber cohort, including all its days.
    negatives = train.filter((pl.col("label") == 0) & pl.col("archetype").is_in(["none", "legit"]))
    parts.extend(["", "## Per-archetype top features", ""])
    for archetype in archetypes:
        positives = train.filter((pl.col("label") == 1) & (pl.col("archetype") == archetype))
        cohort = pl.concat([positives, negatives], how="vertical_relaxed")
        ranked = _auc_table(cohort, features)[:10]
        parts.extend([
            f"### {archetype}", "",
            _markdown_table(
                ["Feature", "Direction-independent AUC", "Flag"],
                [[name, f"{auc:.4f}", flag or "—"] for name, auc, flag in ranked],
            ),
            "",
        ])

    # Profile attributes are subscriber-level; select one record per subscriber first.
    available_profile = [column for column in PROFILE_MEANS if column in train.columns]
    subscriber_rows = train.select(["subscriber_id", "archetype", *available_profile]).unique(
        subset=["subscriber_id"], keep="first"
    )
    stats = subscriber_rows.group_by("archetype").agg(
        pl.len().alias("Subscribers"),
        *[pl.col(column).cast(pl.Float64, strict=False).mean().alias(column) for column in available_profile],
    ).sort("archetype")
    rows = []
    for row in stats.iter_rows(named=True):
        archetype = "legit" if row["archetype"] in ("none", "legit") else row["archetype"]
        rows.append([archetype, row["Subscribers"], *[
            "—" if row.get(column) is None else f"{row[column]:.4f}" for column in available_profile
        ]])
    parts.extend([
        "## Mean subscriber-level profile values", "",
        _markdown_table(["Archetype", "Subscribers", *available_profile], rows),
        "", "All results are from simulated data.", "",
    ])
    return "\n".join(parts)


def main() -> None:
    with (OUT_DIR / "feature_columns.json").open() as handle:
        features = json.load(handle)
    table = pl.read_parquet(OUT_DIR / "training_table.parquet")
    report = build_report(table, features)
    output = OUT_DIR / "shortcut_check.md"
    output.write_text(report, encoding="utf-8")
    print(report)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
