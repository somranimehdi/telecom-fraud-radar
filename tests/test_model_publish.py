from datetime import date

import polars as pl

from model.publish import FORBIDDEN_COLUMNS, build_daily_feature_frames
from model.train import alert_threshold_from_oof


def test_daily_exports_include_features_without_labels_or_split_metadata():
    table = pl.DataFrame(
        {
            "subscriber_id": ["b", "a"],
            "date": [date(2026, 1, 2), date(2026, 1, 1)],
            "call_count": [3, 1],
            "label": [1, 0],
            "is_fraud": [1, 0],
            "archetype": ["irsf", "none"],
            "injection_day": [0, -1],
            "notes": ["secret", "secret"],
            "split": ["train", "test"],
        }
    )
    days = build_daily_feature_frames(table, ["call_count"])
    assert list(days) == ["2026-01-01", "2026-01-02"]
    for frame in days.values():
        assert frame.columns == ["subscriber_id", "date", "call_count"]
        assert not (set(frame.columns) & FORBIDDEN_COLUMNS)


def test_alert_threshold_uses_oof_precision_or_99th_percentile_fallback():
    qualifying = pl.DataFrame({"label": [0, 0, 1, 1], "oof_score": [0.1, 0.2, 0.8, 0.9]})
    assert alert_threshold_from_oof(qualifying) == 0.1

    no_threshold = pl.DataFrame({"label": [0, 0, 0, 1], "oof_score": [0.9, 0.8, 0.7, 0.1]})
    assert alert_threshold_from_oof(no_threshold) == 0.897
