from datetime import date

import polars as pl

from scripts.build_live_features import build_features_for_date


def test_build_live_features_selects_one_day_and_model_columns_only():
    today = date(2026, 10, 17)
    frames = {
        "voice": pl.DataFrame(),
        "data": pl.DataFrame(),
        "revenue": pl.DataFrame(),
        "purchases": pl.DataFrame({
            "purchase_id": ["BUY1"], "subscriber_id": ["SUB000001"],
            "bundle_id": ["B1"], "price": [10.0],
            "timestamp": ["2026-10-17T10:00:00+00:00"],
        }),
        "activation": pl.DataFrame({
            "event_id": ["ACT1"], "subscriber_id": ["SUB000001"],
            "event_type": ["SWAPPED"], "timestamp": ["2026-10-17T09:00:00+00:00"],
        }),
    }
    profiles = pl.DataFrame({
        "subscriber_id": ["SUB000001", "SUB000002"],
        "segment": ["prepaid", "postpaid"],
    })
    expected_features = ["purchase_count", "spend", "sim_swap_count", "segment"]

    result = build_features_for_date(frames, profiles, ["SO", "PK", "LV"], today, expected_features)

    assert result.columns == ["subscriber_id", "date", *expected_features]
    assert result.get_column("date").unique().to_list() == [today]
    assert result.height == 2  # profile grid includes the subscriber with no events
    assert result.filter(pl.col("subscriber_id") == "SUB000001")["purchase_count"].item() == 1
    assert result.filter(pl.col("subscriber_id") == "SUB000001")["sim_swap_count"].item() == 1
    assert result.filter(pl.col("subscriber_id") == "SUB000002")["purchase_count"].item() == 0
