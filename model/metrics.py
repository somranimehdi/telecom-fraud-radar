"""Pure account-level aggregation and evaluation helpers."""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import polars as pl


def aggregate_accounts(
    rows: pl.DataFrame,
    score_col: str = "score",
    target_col: str = "label",
    group_col: str = "subscriber_id",
) -> pl.DataFrame:
    """Collapse subscriber-day rows to one account using max score and any positive label."""
    aggregations = [
        pl.col(score_col).max().alias("score"),
        pl.col(target_col).max().alias("fraud"),
    ]
    if "archetype" in rows.columns:
        aggregations.append(pl.col("archetype").first().alias("archetype"))
    return rows.group_by(group_col).agg(aggregations).rename({group_col: "subscriber_id"})


def precision_recall_at_budget(
    accounts: pl.DataFrame,
    score_col: str = "score",
    budget: int | None = None,
    flagged_col: str | None = None,
) -> dict[str, float | int]:
    """Compute precision/recall for a fixed-size top-score selection or a flag column."""
    if flagged_col is not None:
        selected = accounts.filter(pl.col(flagged_col).cast(pl.Boolean))
    else:
        k = max(0, min(accounts.height, int(budget or 0)))
        selected = accounts.sort(score_col, descending=True).head(k)
    positives = int(accounts.get_column("fraud").sum()) if accounts.height else 0
    true_positives = int(selected.get_column("fraud").sum()) if selected.height else 0
    return {
        "flagged": selected.height,
        "true_positives": true_positives,
        "precision": true_positives / selected.height if selected.height else 0.0,
        "recall": true_positives / positives if positives else 0.0,
    }


def bootstrap_ci(
    accounts: pl.DataFrame,
    metric_fn: Callable[[pl.DataFrame], dict[str, float]],
    n_resamples: int = 300,
    seed: int = 42,
) -> dict[str, tuple[float, float]]:
    """Percentile 95% CIs from subscriber-level resampling with replacement."""
    if accounts.is_empty() or n_resamples < 1:
        return {}
    records = accounts.to_dicts()
    rng = np.random.default_rng(seed)
    samples: dict[str, list[float]] = {}
    for _ in range(n_resamples):
        indices = rng.integers(0, len(records), size=len(records))
        sample = pl.DataFrame([records[int(i)] for i in indices])
        for name, value in metric_fn(sample).items():
            samples.setdefault(name, []).append(float(value))
    return {
        name: (float(np.percentile(values, 2.5)), float(np.percentile(values, 97.5)))
        for name, values in samples.items()
    }
