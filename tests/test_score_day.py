from datetime import date

import polars as pl
import pytest

from scoring.score_day import build_sns_summary, score_day


class FeatureProbabilityModel:
    def predict_proba(self, values):
        scores = values[:, 0].astype(float)
        return __import__("numpy").column_stack([1.0 - scores, scores])


def sample_inputs():
    features = pl.DataFrame(
        {
            "subscriber_id": ["a", "b", "c", "d"],
            "date": [date(2026, 10, 16)] * 4,
            "signal": [0.3, 0.8, 0.9, 0.7],
        }
    )
    rules = pl.DataFrame({"subscriber_id": ["b", "b", "x"], "rule_id": ["RULE_A", "MODEL_GBM_V1", "RULE_B"]})
    return features, rules


def test_threshold_cap_corroboration_and_stable_alert_ids():
    features, rules = sample_inputs()
    model = FeatureProbabilityModel()
    kwargs = (model, ["signal"], 0.7, rules)

    uncapped = score_day(features, *kwargs, config={"max_model_alerts_per_day": 100})
    assert set(uncapped.get_column("subscriber_id")) == {"b", "c", "d"}
    severity = {row["subscriber_id"]: row["severity"] for row in uncapped.to_dicts()}
    assert severity == {"b": "HIGH", "c": "MEDIUM", "d": "MEDIUM"}
    assert all(row["rule_id"] == "MODEL_GBM_V1" for row in uncapped.to_dicts())
    assert all(row["fraud_type"] == "UNSPECIFIED" for row in uncapped.to_dicts())
    assert uncapped.filter(pl.col("subscriber_id") == "b").to_dicts()[0]["evidence"]["corroborating_rules"] == ["RULE_A"]

    capped = score_day(features, *kwargs, config={"max_model_alerts_per_day": 2})
    assert capped.get_column("subscriber_id").to_list() == ["c", "b"]
    again = score_day(features, *kwargs, config={"max_model_alerts_per_day": 2})
    assert capped.get_column("alert_id").to_list() == again.get_column("alert_id").to_list()


def test_missing_feature_fails_and_no_label_columns_are_required():
    features, rules = sample_inputs()
    alerts = score_day(features, FeatureProbabilityModel(), ["signal"], 0.85, rules)
    assert alerts.get_column("subscriber_id").to_list() == ["c"]
    with pytest.raises(ValueError, match="Missing model feature columns"):
        score_day(features.drop("signal"), FeatureProbabilityModel(), ["signal"], 0.5, rules)


def test_empty_alerts_have_no_sns_summary_message():
    assert build_sns_summary(pl.DataFrame(), "2026-10-16", "v1") is None


def test_write_alerts_evidence_with_decimal_is_serializable():
    """Evidence dicts containing Decimal must serialize to JSON."""
    from decimal import Decimal
    import json

    from scoring.score_day import _DecimalSafeEncoder

    evidence = {
        "score": Decimal("0.9998"),
        "threshold": Decimal("0.2562"),
        "model_version": "v1",
        "corroborating_rules": [],
    }
    encoded = json.dumps(evidence, cls=_DecimalSafeEncoder)
    decoded = json.loads(encoded)
    assert decoded["score"] == 0.9998
    assert decoded["threshold"] == 0.2562
