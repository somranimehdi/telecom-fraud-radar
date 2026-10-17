"""Pure Polars feature transforms shared by training and online scoring."""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable

import polars as pl

PROFILE_EXCLUDED = {"subscriber_id", "msisdn", "imsi", "last_updated", "is_fraud",
                    "archetype", "injection_day", "notes", "label", "split"}

def with_event_date(events: pl.DataFrame) -> pl.DataFrame:
    """Derive the event calendar date from timestamp, independent of S3 paths."""
    if "timestamp" not in events.columns:
        return events.with_columns(pl.lit(None, dtype=pl.Date).alias("date"))
    if events.schema["timestamp"] == pl.String:
        parsed = pl.col("timestamp").str.to_datetime(time_zone="UTC", strict=False)
        return events.with_columns(parsed.alias("timestamp"), parsed.dt.date().alias("date"))
    if events.schema["timestamp"] == pl.Date:
        return events.with_columns(pl.col("timestamp").alias("date"))
    return events.with_columns(pl.col("timestamp").dt.date().alias("date"))


def _base_events(events: pl.DataFrame) -> pl.DataFrame:
    if "subscriber_id" not in events.columns:
        return pl.DataFrame(schema={"subscriber_id": pl.String, "date": pl.Date})
    frame = with_event_date(events)
    if "date" not in frame.columns:
        return pl.DataFrame(schema={"subscriber_id": pl.String, "date": pl.Date})
    return frame.filter(pl.col("subscriber_id").is_not_null() & pl.col("date").is_not_null())


def _dedupe(events: pl.DataFrame, id_field: str) -> pl.DataFrame:
    return events.unique(subset=[id_field], keep="last", maintain_order=True) if id_field in events.columns else events


def voice_features(events: pl.DataFrame, high_risk_countries: Iterable[str]) -> pl.DataFrame:
    """Aggregate voice CDRs into subscriber-day features."""
    frame = _dedupe(_base_events(events), "call_id")
    keys = ["subscriber_id", "date"]
    expressions = []
    if "timestamp" in frame.columns:
        expressions.append(pl.len().cast(pl.Int64).alias("call_count"))
        hour = pl.col("timestamp").dt.hour()
        expressions.append(((hour >= 22) | (hour < 6)).cast(pl.Int64).sum().alias("night_call_count"))
    if "country_code" in frame.columns:
        country = pl.col("country_code").cast(pl.String).str.to_uppercase()
        expressions.extend([
            (country != "TN").cast(pl.Int64).sum().alias("intl_call_count"),
            country.is_in([str(c).upper() for c in high_risk_countries]).cast(pl.Int64).sum()
                .alias("high_risk_destination_call_count"),
            country.n_unique().cast(pl.Int64).alias("distinct_destination_countries"),
        ])
    if "duration_sec" in frame.columns:
        duration = pl.col("duration_sec").cast(pl.Float64)
        expressions.extend([
            (duration < 10).cast(pl.Int64).sum().alias("short_call_count"),
            duration.sum().alias("total_duration_sec"),
            duration.mean().alias("avg_call_duration"),
        ])
    if "cost" in frame.columns:
        expressions.append(pl.col("cost").cast(pl.Float64).sum().alias("total_cost"))
    if not expressions:
        return frame.select(keys)
    result = frame.group_by(keys).agg(expressions)
    if "timestamp" in frame.columns:
        hourly = (
            frame.with_columns(pl.col("timestamp").dt.truncate("1h").alias("_event_hour"))
            .group_by([*keys, "_event_hour"]).agg(pl.len().alias("_hour_calls"))
            .group_by(keys).agg(pl.col("_hour_calls").max().cast(pl.Int64).alias("max_calls_in_any_hour"))
        )
        result = result.join(hourly, on=keys, how="left")
    return result


def data_features(events: pl.DataFrame) -> pl.DataFrame:
    frame = _dedupe(_base_events(events), "session_id")
    keys = ["subscriber_id", "date"]
    expressions = []
    if "timestamp" in frame.columns:
        expressions.append(pl.len().cast(pl.Int64).alias("session_count"))
    if "bytes_used" in frame.columns:
        expressions.append(pl.col("bytes_used").cast(pl.Int64).sum().alias("bytes"))
    if "cost" in frame.columns:
        expressions.append(pl.col("cost").cast(pl.Float64).sum().alias("cost"))
    return frame.group_by(keys).agg(expressions) if expressions else frame.select(keys)


def revenue_features(events: pl.DataFrame) -> pl.DataFrame:
    frame = _dedupe(_base_events(events), "transaction_id")
    keys = ["subscriber_id", "date"]
    expressions = []
    if "timestamp" in frame.columns:
        expressions.append(pl.len().cast(pl.Int64).alias("transaction_count"))
    if "amount" in frame.columns:
        expressions.append(pl.col("amount").cast(pl.Float64).sum().alias("net_amount"))
    if "status" in frame.columns:
        expressions.append((pl.col("status").cast(pl.String).str.to_lowercase() == "failed")
                           .cast(pl.Int64).sum().alias("failed_payment_count"))
    return frame.group_by(keys).agg(expressions) if expressions else frame.select(keys)


def purchase_features(events: pl.DataFrame) -> pl.DataFrame:
    frame = _dedupe(_base_events(events), "purchase_id")
    keys = ["subscriber_id", "date"]
    expressions = []
    if "timestamp" in frame.columns:
        expressions.append(pl.len().cast(pl.Int64).alias("purchase_count"))
    if "price" in frame.columns:
        expressions.append(pl.col("price").cast(pl.Float64).sum().alias("spend"))
    if "bundle_id" in frame.columns:
        expressions.append(pl.col("bundle_id").n_unique().cast(pl.Int64).alias("distinct_bundles"))
    return frame.group_by(keys).agg(expressions) if expressions else frame.select(keys)


def activation_features(events: pl.DataFrame) -> pl.DataFrame:
    frame = _dedupe(_base_events(events), "event_id")
    keys = ["subscriber_id", "date"]
    expressions = []
    if "timestamp" in frame.columns:
        expressions.append(pl.len().cast(pl.Int64).alias("activation_event_count"))
    if "event_type" in frame.columns:
        expressions.append((pl.col("event_type").cast(pl.String).str.to_uppercase() == "SWAPPED")
                           .cast(pl.Int64).sum().alias("sim_swap_count"))
    return frame.group_by(keys).agg(expressions) if expressions else frame.select(keys)


def full_subscriber_date_grid(subscribers: pl.DataFrame, first_date: date, last_date: date) -> pl.DataFrame:
    """Return every subscriber crossed with every calendar date in the range."""
    ids = subscribers.select(pl.col("subscriber_id").cast(pl.String).unique()).sort("subscriber_id")
    dates = pl.DataFrame({"date": pl.date_range(first_date, last_date, interval="1d", eager=True)})
    return ids.join(dates, how="cross")


def hours_since_last_swap(grid: pl.DataFrame, activation: pl.DataFrame) -> pl.DataFrame:
    """Add end-of-day hours since the latest swap, capped at 999."""
    frame = with_event_date(activation)
    if frame.is_empty() or not {"subscriber_id", "timestamp", "event_type"}.issubset(frame.columns):
        return grid.with_columns(pl.lit(999.0).alias("hours_since_last_swap"))
    swaps: dict[str, list[datetime]] = {}
    for row in frame.filter(pl.col("event_type").cast(pl.String).str.to_uppercase() == "SWAPPED").iter_rows(named=True):
        ts = row["timestamp"]
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
        if not isinstance(ts, datetime):
            continue
        ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        swaps.setdefault(str(row["subscriber_id"]), []).append(ts.astimezone(timezone.utc))
    for values in swaps.values():
        values.sort()
    hours = []
    for sid, day in grid.select("subscriber_id", "date").iter_rows():
        values = swaps.get(str(sid), [])
        cutoff = datetime.combine(day, time.max, tzinfo=timezone.utc)
        position = bisect_right(values, cutoff) - 1
        hours.append(min(999.0, (cutoff - values[position]).total_seconds() / 3600) if position >= 0 else 999.0)
    return grid.with_columns(pl.Series("hours_since_last_swap", hours, dtype=pl.Float64))


TRAILING_BASES = {
    "intl_call_count": "intl_call_count",
    "high_risk_destination_call_count": "high_risk_destination_call_count",
    "total_cost": "total_cost",
    "purchase_count": "purchase_count",
}


def trailing_window_features(daily: pl.DataFrame) -> pl.DataFrame:
    """Add inclusive trailing 3/7-day sums and cost ratio without future rows."""
    result = daily.sort(["subscriber_id", "date"])
    expressions = []
    for column in TRAILING_BASES:
        if column in result.columns:
            for window in (3, 7):
                rolling = _rolling_expr(pl.col(column).fill_null(0), window, "sum")
                expressions.append(rolling.over("subscriber_id").alias(f"{column}_{window}d_sum"))
    if "total_cost" in result.columns:
        prior_avg = _rolling_expr(pl.col("total_cost").shift(1), 7, "mean").over("subscriber_id")
        expressions.append(
            pl.when(prior_avg.is_null() | (prior_avg == 0)).then(0.0)
            .otherwise(pl.col("total_cost") / prior_avg)
            .alias("total_cost_to_prior_7d_avg_ratio")
        )
    return result.with_columns(expressions) if expressions else result


def _rolling_expr(expression: pl.Expr, window: int, operation: str) -> pl.Expr:
    """Support the min_periods/min_samples spelling across supported Polars 1.x."""
    method = expression.rolling_sum if operation == "sum" else expression.rolling_mean
    try:
        return method(window_size=window, min_samples=1)
    except TypeError:
        return method(window_size=window, min_periods=1)


def profile_features(profiles: pl.DataFrame) -> pl.DataFrame:
    """Encode profile categoricals and retain usable scalar profile columns."""
    excluded = PROFILE_EXCLUDED
    result = profiles.select(pl.col("subscriber_id").cast(pl.String))
    for column in profiles.columns:
        if column in excluded:
            continue
        series = profiles.get_column(column)
        if series.dtype == pl.String or series.dtype == pl.Categorical or str(series.dtype).startswith("Enum"):
            values = sorted(str(value) for value in series.drop_nulls().unique().to_list())
            mapping = {value: code for code, value in enumerate(values)}
            encoded = [mapping.get(str(value), -1) if value is not None else -1 for value in series.to_list()]
            result = result.with_columns(pl.Series(column, encoded, dtype=pl.Int32))
        elif series.dtype.is_numeric():
            result = result.with_columns(series.cast(pl.Float64).fill_null(0).alias(column))
        else:
            continue
    return result


def skipped_features(frames: dict[str, pl.DataFrame], profiles: pl.DataFrame | None = None) -> dict[str, str]:
    """Explain requested feature fields that cannot be computed from inputs."""
    requirements = {
        "voice": {
            "call_count": {"timestamp"}, "intl_call_count": {"country_code"},
            "high_risk_destination_call_count": {"country_code"},
            "distinct_destination_countries": {"country_code"}, "night_call_count": {"timestamp"},
            "max_calls_in_any_hour": {"timestamp"}, "short_call_count": {"duration_sec"},
            "total_duration_sec": {"duration_sec"}, "total_cost": {"cost"}, "avg_call_duration": {"duration_sec"},
        },
        "data": {"session_count": {"timestamp"}, "bytes": {"bytes_used"}, "cost": {"cost"}},
        "revenue": {"transaction_count": {"timestamp"}, "net_amount": {"amount"}, "failed_payment_count": {"status"}},
        "purchases": {"purchase_count": {"timestamp"}, "spend": {"price"}, "distinct_bundles": {"bundle_id"}},
        "activation": {"activation_event_count": {"timestamp"}, "sim_swap_count": {"event_type"},
                       "hours_since_last_swap": {"timestamp", "event_type"}},
    }
    result = {}
    for source, features in requirements.items():
        columns = set(frames.get(source, pl.DataFrame()).columns)
        for feature, needed in features.items():
            missing = sorted(needed - columns)
            if missing:
                result[feature] = f"{source} source lacks required column(s): {', '.join(missing)}"
    derived_bases = list(TRAILING_BASES)
    available = {
        feature for source_features in requirements.values() for feature in source_features
    } - set(result)
    for base in derived_bases:
        if base not in available:
            result[f"{base}_3d_sum"] = f"requires unavailable daily feature {base}"
            result[f"{base}_7d_sum"] = f"requires unavailable daily feature {base}"
    if "total_cost" not in available:
        result["total_cost_to_prior_7d_avg_ratio"] = "requires unavailable daily feature total_cost"
    if profiles is not None:
        used_profile_columns = set(profile_features(profiles).columns)
        for column in profiles.columns:
            if column in used_profile_columns or column in PROFILE_EXCLUDED:
                continue
            result[f"profile.{column}"] = f"unsupported profile dtype: {profiles.schema[column]}"
    return result


def build_daily_features(
    voice: pl.DataFrame,
    data: pl.DataFrame,
    revenue: pl.DataFrame,
    purchases: pl.DataFrame,
    activation: pl.DataFrame,
    profiles: pl.DataFrame,
    high_risk_countries: Iterable[str],
) -> pl.DataFrame:
    """Aggregate source events, form the full grid, and join profile features."""
    event_frames = [voice, data, revenue, purchases, activation]
    dated_frames = [with_event_date(frame) for frame in event_frames]
    event_dates = [value for frame in dated_frames for value in frame.get_column("date").drop_nulls().to_list()]
    if not event_dates:
        raise ValueError("no event timestamps found across Silver sources")
    grid = full_subscriber_date_grid(profiles, min(event_dates), max(event_dates))
    aggregates = [
        voice_features(voice, high_risk_countries), data_features(data),
        revenue_features(revenue), purchase_features(purchases), activation_features(activation),
    ]
    for aggregate in aggregates:
        if aggregate.width > 2:
            grid = grid.join(aggregate, on=["subscriber_id", "date"], how="left")
    if {"timestamp", "event_type"}.issubset(activation.columns):
        grid = hours_since_last_swap(grid, activation)
    profile_frame = profile_features(profiles)
    grid = grid.join(profile_frame, on="subscriber_id", how="left")
    numeric = [column for column in grid.columns if column not in {"subscriber_id", "date"}]
    grid = grid.with_columns([pl.col(column).fill_null(0) for column in numeric])
    return trailing_window_features(grid).sort(["subscriber_id", "date"])
