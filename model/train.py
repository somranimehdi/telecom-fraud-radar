"""Train and evaluate the Week 5 subscriber-day fraud model from local artifacts."""

from __future__ import annotations

import json
import math
from pathlib import Path

import joblib
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.model_selection import GroupKFold
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score

from model.metrics import aggregate_accounts, bootstrap_ci, precision_recall_at_budget


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "model" / "out"
EVAL_OUT = ROOT / "evaluation" / "out"
SEED = 42
DROP_FEATURES = ["failed_payment_count"]
ABLATION_FEATURES = ["hours_since_last_swap", "sim_swap_count", "activation_event_count"]
ARCHETYPES = ["sim_swap_takeover", "irsf", "subscription_fraud", "bundle_abuse"]
RULE_ARCHETYPES = {
    "SIM_SWAP_INTL_24H": "sim_swap_takeover",
    "IRSF_HIGH_RISK_BURST": "irsf",
    "NEW_ACCOUNT_HIGH_VALUE_PURCHASE": "subscription_fraud",
    "REPEATED_BUNDLE_PURCHASES": "bundle_abuse",
}


def estimator() -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(
        random_state=SEED, max_depth=5, learning_rate=0.08, max_iter=250,
        l2_regularization=1.0,
    )


def _matrix(frame: pl.DataFrame, features: list[str]) -> np.ndarray:
    return frame.select([pl.col(name).cast(pl.Float64, strict=False) for name in features]).to_numpy()


def _weights(y: np.ndarray) -> np.ndarray:
    positives = int(np.sum(y == 1))
    negatives = int(np.sum(y == 0))
    ratio = min(30.0, negatives / positives) if positives else 1.0
    return np.where(y == 1, ratio, 1.0)


def _cross_validated_predictions(train: pl.DataFrame, features: list[str]) -> tuple[list[float], pl.DataFrame]:
    x, y = _matrix(train, features), train.get_column("label").to_numpy().astype(int)
    groups = train.get_column("subscriber_id").to_numpy()
    cv = GroupKFold(n_splits=3)
    scores: list[float] = []
    oof = np.empty(len(y), dtype=np.float64)
    for train_idx, valid_idx in cv.split(x, y, groups):
        model = estimator()
        model.fit(x[train_idx], y[train_idx], sample_weight=_weights(y[train_idx]))
        validation_scores = model.predict_proba(x[valid_idx])[:, 1]
        oof[valid_idx] = validation_scores
        scores.append(float(average_precision_score(y[valid_idx], validation_scores)))
    columns = ["subscriber_id", "date", "label"]
    return scores, train.select(columns).with_columns(pl.Series("oof_score", oof))


def alert_threshold_from_oof(oof_scores: pl.DataFrame) -> float:
    """Lowest OOF score threshold attaining at least 0.5 row-level precision."""
    y = oof_scores.get_column("label").to_numpy().astype(int)
    scores = oof_scores.get_column("oof_score").to_numpy().astype(float)
    precision, _recall, thresholds = precision_recall_curve(y, scores)
    qualifying = thresholds[precision[:-1] >= 0.5]
    return float(qualifying.min()) if len(qualifying) else float(np.quantile(scores, 0.99))


def _account_frame(rows: pl.DataFrame, scores: np.ndarray) -> pl.DataFrame:
    return aggregate_accounts(rows.with_columns(pl.Series("score", scores)), score_col="score")


def _add_rules(accounts: pl.DataFrame, alerts: pl.DataFrame, test_ids: set[str]) -> tuple[pl.DataFrame, pl.DataFrame]:
    scoped = alerts.filter(pl.col("subscriber_id").cast(pl.String).is_in(sorted(test_ids)))
    ids = scoped.get_column("subscriber_id").cast(pl.String).unique().to_list()
    accounts = accounts.with_columns(pl.col("subscriber_id").is_in(ids).alias("rule_flag"))
    return accounts, scoped


def _metric_at_top(accounts: pl.DataFrame, budget: int, candidate_col: str | None = None) -> dict[str, float | int]:
    candidates = accounts if candidate_col is None else accounts.filter(pl.col(candidate_col))
    selected_ids = set(candidates.sort("score", descending=True).head(max(0, budget)).get_column("subscriber_id").to_list())
    selected = accounts.with_columns(pl.col("subscriber_id").is_in(selected_ids).alias("selected"))
    return precision_recall_at_budget(selected, flagged_col="selected")


def _per_archetype_recall(accounts: pl.DataFrame, selected_ids: set[str]) -> dict[str, float]:
    output = {}
    for archetype in ARCHETYPES:
        cohort = accounts.filter((pl.col("archetype") == archetype) & (pl.col("fraud") == 1))
        output[archetype] = (
            cohort.filter(pl.col("subscriber_id").is_in(selected_ids)).height / cohort.height
            if cohort.height else 0.0
        )
    return output


def _prediction_metrics(rows: pl.DataFrame, scores: np.ndarray) -> tuple[dict[str, object], pl.DataFrame]:
    y = rows.get_column("label").to_numpy().astype(int)
    metrics: dict[str, object] = {
        "roc_auc": float(roc_auc_score(y, scores)) if len(np.unique(y)) > 1 else None,
        "pr_auc": float(average_precision_score(y, scores)),
    }
    scored_rows = rows.with_columns(pl.Series("__score", scores))
    legit = scored_rows.filter((pl.col("label") == 0) & pl.col("archetype").is_in(["none", "legit"]))
    per_arch = {}
    for archetype in ARCHETYPES:
        positive = scored_rows.filter((pl.col("label") == 1) & (pl.col("archetype") == archetype))
        cohort = pl.concat([positive, legit], how="vertical_relaxed")
        cy = cohort.get_column("label").to_numpy().astype(int)
        cs = _matrix(cohort, ["__score"])
        per_arch[archetype] = {
            "positive_subscribers": positive.get_column("subscriber_id").n_unique(),
            "roc_auc": float(roc_auc_score(cy, cs[:, 0])) if len(np.unique(cy)) > 1 else None,
            "pr_auc": float(average_precision_score(cy, cs[:, 0])) if len(np.unique(cy)) > 1 else None,
        }
    metrics["per_archetype"] = per_arch
    return metrics, scored_rows


def _evaluate_account(rows: pl.DataFrame, scores: np.ndarray, alerts: pl.DataFrame) -> dict[str, object]:
    accounts = _account_frame(rows, scores)
    accounts, scoped_alerts = _add_rules(accounts, alerts, set(accounts.get_column("subscriber_id").to_list()))
    rule_count = int(accounts.get_column("rule_flag").sum())
    rule_metrics = precision_recall_at_budget(accounts, flagged_col="rule_flag")
    rules_per_arch = _per_archetype_recall(
        accounts, set(accounts.filter(pl.col("rule_flag")).get_column("subscriber_id").to_list())
    )
    model_metrics = _metric_at_top(accounts, rule_count)
    model_ids = set(accounts.sort("score", descending=True).head(rule_count).get_column("subscriber_id").to_list())
    model_per_arch = _per_archetype_recall(accounts, model_ids)

    combined = {}
    for fraction in (0.25, 0.50):
        keep = math.ceil(rule_count * fraction)
        combined[str(int(fraction * 100))] = _metric_at_top(accounts, keep, "rule_flag")

    by_rule = []
    for rule_id in sorted(scoped_alerts.get_column("rule_id").unique().to_list()):
        rule_ids = set(scoped_alerts.filter(pl.col("rule_id") == rule_id).get_column("subscriber_id").cast(pl.String).to_list())
        selected = accounts.with_columns(pl.col("subscriber_id").is_in(rule_ids).alias("flag"))
        stats = precision_recall_at_budget(selected, flagged_col="flag")
        matching_positive = selected.filter(
            pl.col("flag") & (pl.col("fraud") == 1) & (pl.col("archetype") == RULE_ARCHETYPES.get(rule_id, ""))
        ).height
        matched_precision = matching_positive / stats["flagged"] if stats["flagged"] else 0.0
        by_rule.append({"rule_id": rule_id, **stats, "precision": matched_precision,
                        "any_fraud_precision": stats["precision"]})

    precision_k = {}
    n = accounts.height
    for pct in (1, 2, 5):
        stats = _metric_at_top(accounts, max(1, math.ceil(n * pct / 100)))
        precision_k[str(pct)] = stats

    return {
        "accounts": accounts,
        "rules": rule_metrics,
        "rules_per_archetype": rules_per_arch,
        "model": model_metrics,
        "model_per_archetype": model_per_arch,
        "combined": combined,
        "per_rule": by_rule,
        "precision_at_pct": precision_k,
    }


def _bootstrap_metrics(accounts: pl.DataFrame) -> dict[str, dict[str, tuple[float, float]]]:
    def evaluate(sample: pl.DataFrame) -> dict[str, float]:
        budget = int(sample.get_column("rule_flag").sum())
        rule = precision_recall_at_budget(sample, flagged_col="rule_flag")
        model = _metric_at_top(sample, budget)
        rule_count = int(sample.get_column("rule_flag").sum())
        kept25 = _metric_at_top(sample, math.ceil(rule_count * 0.25), "rule_flag")
        kept50 = _metric_at_top(sample, math.ceil(rule_count * 0.50), "rule_flag")
        return {
            "rules_precision": float(rule["precision"]), "rules_recall": float(rule["recall"]),
            "model_precision": float(model["precision"]), "model_recall": float(model["recall"]),
            "combined25_precision": float(kept25["precision"]), "combined25_recall": float(kept25["recall"]),
            "combined50_precision": float(kept50["precision"]), "combined50_recall": float(kept50["recall"]),
        }
    return bootstrap_ci(accounts, evaluate, n_resamples=300, seed=SEED)


def _fmt(value: float | int | None) -> str:
    return "—" if value is None else (f"{value:.4f}" if isinstance(value, float) else str(value))


def _table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    lines += ["| " + " | ".join(map(str, row)) + " |" for row in rows]
    return "\n".join(lines)


def _report(main: dict[str, object], ablation: dict[str, object], row_metrics: dict[str, object],
            ablation_row_metrics: dict[str, object], ablation_cv: list[float], main_cv: list[float], importance: list[tuple[str, float]],
            cis: dict[str, dict[str, tuple[float, float]]], feature_dropped: list[str]) -> str:
    parts = ["# Model vs Rules", "", "## Training stability and row-level test metrics", "",
             f"TRAIN 3-fold GroupKFold PR-AUC: {', '.join(f'{v:.4f}' for v in main_cv)} (mean {np.mean(main_cv):.4f}).",
             f"Ablation GroupKFold PR-AUC: {', '.join(f'{v:.4f}' for v in ablation_cv)} (mean {np.mean(ablation_cv):.4f}).",
             f"Dropped constant feature(s): {', '.join(feature_dropped)}.", "",
             _table(["Test cohort", "Positive subscribers", "ROC-AUC", "PR-AUC"], [
                 ["Overall", "all test fraud accounts", _fmt(row_metrics["roc_auc"]), _fmt(row_metrics["pr_auc"])]
             ] + [[name, vals["positive_subscribers"], _fmt(vals["roc_auc"]), _fmt(vals["pr_auc"])]
                  for name, vals in row_metrics["per_archetype"].items()]),
             "", "## Ablation row-level test metrics", "",
             _table(["Test cohort", "Positive subscribers", "ROC-AUC", "PR-AUC"], [
                 ["Overall", "all test fraud accounts", _fmt(ablation_row_metrics["roc_auc"]), _fmt(ablation_row_metrics["pr_auc"])]
             ] + [[name, vals["positive_subscribers"], _fmt(vals["roc_auc"]), _fmt(vals["pr_auc"])]
                  for name, vals in ablation_row_metrics["per_archetype"].items()]),
             "", "## Account-level test comparison", "",
             _table(["Method", "Accounts flagged", "Precision", "Recall", "Precision 95% CI", "Recall 95% CI"], [
                 ["Rules", main["rules"]["flagged"], _fmt(main["rules"]["precision"]), _fmt(main["rules"]["recall"]),
                  _fmt(cis["rules_precision"][0]) + "–" + _fmt(cis["rules_precision"][1]), _fmt(cis["rules_recall"][0]) + "–" + _fmt(cis["rules_recall"][1])],
                 ["Model at rules alert budget", main["model"]["flagged"], _fmt(main["model"]["precision"]), _fmt(main["model"]["recall"]),
                  _fmt(cis["model_precision"][0]) + "–" + _fmt(cis["model_precision"][1]), _fmt(cis["model_recall"][0]) + "–" + _fmt(cis["model_recall"][1])],
                 ["Rules + model top 25%", main["combined"]["25"]["flagged"], _fmt(main["combined"]["25"]["precision"]), _fmt(main["combined"]["25"]["recall"]),
                  _fmt(cis["combined25_precision"][0]) + "–" + _fmt(cis["combined25_precision"][1]), _fmt(cis["combined25_recall"][0]) + "–" + _fmt(cis["combined25_recall"][1])],
                 ["Rules + model top 50%", main["combined"]["50"]["flagged"], _fmt(main["combined"]["50"]["precision"]), _fmt(main["combined"]["50"]["recall"]),
                  _fmt(cis["combined50_precision"][0]) + "–" + _fmt(cis["combined50_precision"][1]), _fmt(cis["combined50_recall"][0]) + "–" + _fmt(cis["combined50_recall"][1])],
             ]), "", "### Per-archetype account recall", "",
             _table(["Archetype", "Positive test subscribers", "Rules recall", "Model recall", "Ablation model recall"], [
                 [name, main["accounts"].filter((pl.col("archetype") == name) & (pl.col("fraud") == 1)).height,
                  _fmt(main["rules_per_archetype"][name]), _fmt(main["model_per_archetype"][name]), _fmt(ablation["model_per_archetype"][name])]
                 for name in ARCHETYPES]), "", "### Per-rule account precision", "",
             _table(["Rule", "Accounts flagged", "Precision", "Any-fraud precision"], [
                 [item["rule_id"], item["flagged"], _fmt(item["precision"]), _fmt(item["any_fraud_precision"])] for item in main["per_rule"]
             ]), "", "### Precision@K", "",
             _table(["Top share of test accounts", "Accounts flagged", "Precision", "Recall"], [
                 [f"{pct}%", vals["flagged"], _fmt(vals["precision"]), _fmt(vals["recall"])] for pct, vals in main["precision_at_pct"].items()
             ]), "", "### Ablation comparison", "",
             _table(["Measure", "Main model", "Ablated model", "Difference (ablated − main)"], [
                 [label, _fmt(main[section][portion][key] if portion else main[section][key]),
                  _fmt(ablation[section][portion][key] if portion else ablation[section][key]),
                  _fmt(float(ablation[section][portion][key] if portion else ablation[section][key]) -
                       float(main[section][portion][key] if portion else main[section][key]))]
                 for section, portion, key, label in [
                     ("model", None, "precision", "Precision at rules budget"), ("model", None, "recall", "Recall at rules budget"),
                     ("combined", "25", "precision", "Rules + model top 25% precision"), ("combined", "25", "recall", "Rules + model top 25% recall"),
                     ("combined", "50", "precision", "Rules + model top 50% precision"), ("combined", "50", "recall", "Rules + model top 50% recall"),
                 ]
             ]), "", "### Main-model permutation importance (average precision decrease)", "",
             _table(["Rank", "Feature", "Importance"], [[i + 1, f, f"{v:.6f}"] for i, (f, v) in enumerate(importance)]),
             "", "Results come from simulated data with injected patterns. SIM swap is easy because `hours_since_last_swap` is near-perfectly informative. Bundle abuse (6 test accounts) and subscription fraud (13) are small samples.", ""]
    # Explicitly show all archetype-specific recall comparisons for the combined strategy too.
    return "\n".join(parts)


def main() -> None:
    features = json.loads((OUT / "feature_columns.json").read_text())
    table = pl.read_parquet(OUT / "training_table.parquet")
    alerts = pl.read_parquet(OUT / "rule_alerts.parquet")
    dropped = [name for name in DROP_FEATURES if name in features]
    features = [name for name in features if name not in DROP_FEATURES]
    train = table.filter(pl.col("split") == "train")
    test = table.filter(pl.col("split") == "test")
    cv_scores, oof_scores = _cross_validated_predictions(train, features)
    alert_threshold = alert_threshold_from_oof(oof_scores)

    x_train, y_train = _matrix(train, features), train.get_column("label").to_numpy().astype(int)
    x_test, y_test = _matrix(test, features), test.get_column("label").to_numpy().astype(int)
    model = estimator()
    model.fit(x_train, y_train, sample_weight=_weights(y_train))
    row_scores = model.predict_proba(x_test)[:, 1]
    row_metrics, scored_rows = _prediction_metrics(test, row_scores)
    main_eval = _evaluate_account(test, row_scores, alerts)
    cis = _bootstrap_metrics(main_eval["accounts"])

    ablated_features = [name for name in features if name not in ABLATION_FEATURES]
    ablation_cv, _ = _cross_validated_predictions(train, ablated_features)
    ablation = estimator()
    ablation.fit(_matrix(train, ablated_features), y_train, sample_weight=_weights(y_train))
    ablation_scores = ablation.predict_proba(_matrix(test, ablated_features))[:, 1]
    ablation_row_metrics, _ = _prediction_metrics(test, ablation_scores)
    ablation_eval = _evaluate_account(test, ablation_scores, alerts)

    sample = scored_rows.sample(n=min(30_000, scored_rows.height), with_replacement=False, shuffle=True, seed=SEED)
    sample_x = _matrix(sample, features)
    sample_y = sample.get_column("label").to_numpy().astype(int)
    pi = permutation_importance(
        model, sample_x, sample_y, scoring="average_precision", n_repeats=3,
        random_state=SEED, n_jobs=-1,
    )
    importance = sorted(zip(features, pi.importances_mean.tolist()), key=lambda item: -item[1])[:10]

    joblib.dump({"model": model, "feature_columns": features, "random_state": SEED}, OUT / "model.joblib")
    oof_scores.write_parquet(OUT / "oof_scores.parquet")
    training_dates = train.get_column("date")
    metrics = {
        "model_version": "v1",
        "trained_on": "simulated data",
        "alert_threshold": alert_threshold,
        "training_date_range": {
            "start": str(training_dates.min()),
            "end": str(training_dates.max()),
        },
        "model": "HistGradientBoostingClassifier",
        "parameters": {"random_state": SEED, "max_depth": 5, "learning_rate": 0.08, "max_iter": 250, "l2_regularization": 1.0},
        "dropped_constant_features": dropped, "feature_columns": features,
        "train_groupkfold_pr_auc": cv_scores, "ablation_groupkfold_pr_auc": ablation_cv,
        "row_level_test": row_metrics,
        "account_test": {key: value for key, value in main_eval.items() if key != "accounts"},
        "ablation_account_test": {key: value for key, value in ablation_eval.items() if key != "accounts"},
        "bootstrap_95_ci": {key: list(value) for key, value in cis.items()},
        "permutation_importance_top10": [{"feature": f, "mean_ap_decrease": v} for f, v in importance],
    }
    (OUT / "model_metrics.json").write_text(json.dumps(metrics, indent=2, default=str) + "\n")
    report = _report(main_eval, ablation_eval, row_metrics, ablation_row_metrics,
                     ablation_cv, cv_scores, importance, cis, dropped)
    EVAL_OUT.mkdir(parents=True, exist_ok=True)
    (EVAL_OUT / "model_vs_rules.md").write_text(report, encoding="utf-8")
    print(report)
    print(f"Saved {OUT / 'model.joblib'}, {OUT / 'model_metrics.json'}, and {EVAL_OUT / 'model_vs_rules.md'}")


if __name__ == "__main__":
    main()
