from datetime import date

import polars as pl
import pytest

from model.build_training_table import assemble_training_table, build_from_inputs, sim_swap_match_rate, subscriber_splits
from model.features import full_subscriber_date_grid, trailing_window_features, voice_features


def test_trailing_windows_do_not_include_future_days():
    daily = pl.DataFrame({
        "subscriber_id": ["s1"] * 6,
        "date": [date(2026, 1, d) for d in range(1, 7)],
        "intl_call_count": [0, 0, 0, 0, 0, 100],
        "high_risk_destination_call_count": [0, 0, 0, 0, 0, 100],
        "total_cost": [1.0, 1.0, 1.0, 1.0, 1.0, 100.0],
        "purchase_count": [0, 0, 0, 0, 0, 20],
    })
    result = trailing_window_features(daily)
    day4 = result.filter(pl.col("date") == date(2026, 1, 4)).row(0, named=True)
    assert day4["intl_call_count_3d_sum"] == 0
    assert day4["total_cost_7d_sum"] == 4.0
    assert day4["total_cost_to_prior_7d_avg_ratio"] == 1.0


def test_full_grid_crosses_every_subscriber_with_every_day():
    profiles = pl.DataFrame({"subscriber_id": ["s1", "s2"]})
    grid = full_subscriber_date_grid(profiles, date(2026, 1, 1), date(2026, 1, 3))
    assert grid.height == 6
    assert grid.group_by("subscriber_id").len().get_column("len").to_list() == [3, 3]


def test_target_is_zero_before_injection_and_features_exclude_label_metadata():
    daily = pl.DataFrame({
        "subscriber_id": ["s1"] * 3,
        "date": [date(2026, 1, d) for d in (1, 2, 3)],
        "call_count": [1, 2, 3],
    })
    labels = pl.DataFrame({
        "subscriber_id": ["s1"], "is_fraud": [1], "archetype": ["irsf"],
        "injection_day": [2], "notes": ["must never be accessed"],
    })
    table, features = assemble_training_table(daily, labels, date(2026, 1, 1))
    assert table.get_column("label").to_list() == [0, 0, 1]
    assert features == ["call_count"]
    assert not set(features) & {"is_fraud", "archetype", "injection_day", "notes", "label", "split"}
    assert table.get_column("split").unique().to_list() in (["train"], ["test"])


def test_voice_features_use_configured_high_risk_country_set():
    events = pl.DataFrame({
        "call_id": ["a", "b"], "subscriber_id": ["s1", "s1"],
        "timestamp": ["2026-01-01T01:00:00Z", "2026-01-01T13:00:00Z"],
        "country_code": ["SO", "GB"], "duration_sec": [5, 20], "cost": [2.0, 3.0],
    })
    row = voice_features(events, ["SO", "PK", "LV"]).row(0, named=True)
    assert row["call_count"] == 2
    assert row["intl_call_count"] == 2
    assert row["high_risk_destination_call_count"] == 1
    assert row["night_call_count"] == 1
    assert row["max_calls_in_any_hour"] == 1


def test_swap_alignment_rate_uses_origin_and_expected_injection_day():
    origin = date(2026, 1, 1)
    labels = pl.DataFrame({
        "subscriber_id": ["s1", "s2"], "is_fraud": [1, 1],
        "archetype": ["sim_swap_takeover", "sim_swap_takeover"],
        "injection_day": [2, 3], "notes": ["ignored", "ignored"],
    })
    activation = pl.DataFrame({
        "subscriber_id": ["s1", "s2"], "event_type": ["SWAPPED", "SWAPPED"],
        "timestamp": ["2026-01-03T03:17:00Z", "2026-01-04T03:17:00Z"],
    })
    assert sim_swap_match_rate(labels, activation, origin) == 1.0


def test_in_memory_training_build_joins_profile_and_event_features():
    stamp = "2026-01-01T03:17:00Z"
    frames = {
        "voice": pl.DataFrame({
            "call_id": ["c1", "c2"], "subscriber_id": ["s1", "s1"],
            "timestamp": [stamp, "2026-01-03T13:00:00Z"],
            "country_code": ["SO", "GB"], "duration_sec": [5, 20], "cost": [2.0, 3.0],
        }),
        "data": pl.DataFrame(schema={"session_id": pl.String, "subscriber_id": pl.String,
                                     "timestamp": pl.String, "bytes_used": pl.Int64, "cost": pl.Float64}),
        "revenue": pl.DataFrame(schema={"transaction_id": pl.String, "subscriber_id": pl.String,
                                         "timestamp": pl.String, "amount": pl.Float64, "status": pl.String}),
        "purchases": pl.DataFrame(schema={"purchase_id": pl.String, "subscriber_id": pl.String,
                                           "timestamp": pl.String, "price": pl.Float64, "bundle_id": pl.String}),
        "activation": pl.DataFrame({
            "event_id": ["a1"], "subscriber_id": ["s1"], "timestamp": [stamp], "event_type": ["SWAPPED"],
        }),
    }
    profiles = pl.DataFrame({
        "subscriber_id": ["s1"], "segment": ["consumer"], "kyc_status": ["verified"],
        "account_age_days": [100], "lines_per_identity": [1], "risk_score_baseline": [0.1],
    })
    labels = pl.DataFrame({
        "subscriber_id": ["s1"], "is_fraud": [1], "archetype": ["sim_swap_takeover"],
        "injection_day": [0], "notes": ["not a feature"],
    })
    table, feature_columns, origin, rate, skipped = build_from_inputs(
        frames, profiles, labels, ["SO", "PK", "LV"]
    )
    assert table.height == 3
    first = table.filter(pl.col("date") == date(2026, 1, 1)).row(0, named=True)
    missing_day = table.filter(pl.col("date") == date(2026, 1, 2)).row(0, named=True)
    assert first["label"] == 1
    assert first["high_risk_destination_call_count"] == 1
    assert first["hours_since_last_swap"] < 999
    assert missing_day["call_count"] == 0
    assert missing_day["intl_call_count"] == 0
    assert origin == date(2026, 1, 1)
    assert rate == 1.0
    assert skipped == {}
    assert "segment" in feature_columns


def test_stratified_subscriber_split_assigns_each_subscriber_once():
    labels = pl.DataFrame({
        "subscriber_id": [f"s{i}" for i in range(20)],
        "archetype": ["irsf"] * 10 + ["none"] * 10,
    })
    splits = subscriber_splits(labels, seed=42)
    assert len(splits) == 20
    assert set(splits.values()) == {"train", "test"}
    assert len(splits) == len(set(splits))


def test_training_build_fails_clearly_when_swap_alignment_is_below_threshold():
    frames = {
        source: pl.DataFrame({"subscriber_id": ["s1"], "timestamp": ["2026-01-01T00:00:00Z"]})
        for source in ("voice", "data", "revenue", "purchases", "activation")
    }
    frames["activation"] = frames["activation"].with_columns(pl.lit("ACTIVATED").alias("event_type"))
    profiles = pl.DataFrame({"subscriber_id": ["s1"]})
    labels = pl.DataFrame({
        "subscriber_id": ["s1"], "is_fraud": [1], "archetype": ["sim_swap_takeover"],
        "injection_day": [0], "notes": ["ignored"],
    })
    with pytest.raises(ValueError, match=r"match rate 0\.0%.*below 80%"):
        build_from_inputs(frames, profiles, labels, ["SO", "PK", "LV"])
