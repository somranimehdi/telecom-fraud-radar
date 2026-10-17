import polars as pl

from model.metrics import aggregate_accounts, bootstrap_ci, precision_recall_at_budget


def test_account_aggregation_and_budget_metrics():
    rows = pl.DataFrame(
        {
            "subscriber_id": ["a", "a", "b", "c"],
            "score": [0.2, 0.9, 0.8, 0.1],
            "label": [0, 1, 0, 0],
            "archetype": ["sim_swap_takeover"] * 2 + ["none", "none"],
        }
    )
    accounts = aggregate_accounts(rows)
    assert accounts.height == 3
    account_a = accounts.filter(pl.col("subscriber_id") == "a").row(0, named=True)
    assert account_a["score"] == 0.9
    assert account_a["fraud"] == 1

    stats = precision_recall_at_budget(accounts, budget=1)
    assert stats == {"flagged": 1, "true_positives": 1, "precision": 1.0, "recall": 1.0}


def test_bootstrap_ci_is_subscriber_level_and_reproducible():
    accounts = pl.DataFrame(
        {
            "subscriber_id": ["a", "b", "c", "d"],
            "score": [0.9, 0.8, 0.2, 0.1],
            "fraud": [1, 1, 0, 0],
        }
    )

    def metrics(sample):
        result = precision_recall_at_budget(sample, flagged_col="selected")
        return {"precision": result["precision"], "recall": result["recall"]}

    marked = accounts.with_columns(pl.col("fraud").cast(pl.Boolean).alias("selected"))
    ci = bootstrap_ci(marked, metrics, n_resamples=30, seed=42)
    assert set(ci) == {"precision", "recall"}
    assert ci == bootstrap_ci(marked, metrics, n_resamples=30, seed=42)
