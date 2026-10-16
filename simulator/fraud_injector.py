"""
Simulator — Fraud Injector
----------------------------
Takes legit subscriber activity from baseline.py and mutates it to inject
one of four fraud archetypes. Returns BOTH the modified events AND the
labels — but they must never be combined by the caller.

Design principle: sealed envelope.
  - The subscriber's fraud status is decided here.
  - The events are modified to *look* fraud-like, probabilistically.
  - The label is returned separately and written only to labels.parquet.
  - The model must reconstruct the label from behavior alone.

Archetypes (weights from config.fraud.archetype_weights):
  1. sim_swap_takeover  — swap + burst of intl calls within 24h
  2. irsf                — burst of short calls to high-IRSF countries
  3. subscription_fraud  — new unverified account, high activity, dormancy
  4. bundle_abuse        — buy cheap bundle, consume 10-50x allowance, repeat

Every archetype is probabilistic — no pattern happens with 100% certainty.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from random import Random
from typing import Any

import numpy as np

from simulator import constants as C
from simulator.baseline import SubscriberProfile, DailyActivity


# ─── Data Classes ───────────────────────────────────────────────────────────

@dataclass
class FraudLabel:
    """Ground truth for one fraud subscriber. Written to labels.parquet only."""
    subscriber_id: str
    is_fraud: int                    # 1 or 0
    archetype: str                   # one of the 4 names, or "none"
    injection_day: int               # day index (0-based) when fraud starts
    notes: str = ""                  # free text for debugging


@dataclass
class FraudAssignment:
    """One fraud subscriber's plan — created before event generation."""
    subscriber_id: str
    archetype: str
    injection_day: int               # day index when the pattern begins
    params: dict[str, Any]           # archetype-specific knobs


# ─── Fraud Assignment ───────────────────────────────────────────────────────

def assign_fraud_subscribers(
    subscribers: list[SubscriberProfile],
    cfg: dict,
    seed: int,
) -> tuple[dict[str, FraudAssignment], list[FraudLabel]]:
    """
    Select ~fraud_rate * len(subscribers) subscribers as fraud.
    Each is assigned one archetype by weighted choice.
    Returns:
      - assignments: {subscriber_id -> FraudAssignment} (only fraud subs)
      - labels: FraudLabel rows for ALL subscribers (fraud AND legit)
    """
    rng = Random(seed)
    np_rng = np.random.default_rng(seed)

    total_days = cfg["population"]["days"]
    fraud_rate = cfg["fraud"]["subscriber_fraud_rate"]
    archetype_weights = cfg["fraud"]["archetype_weights"]

    n_fraud = int(round(len(subscribers) * fraud_rate))
    fraud_sub_ids = set(rng.sample([s.subscriber_id for s in subscribers], n_fraud))

    archetype_names = list(archetype_weights.keys())
    archetype_probs = np.array(list(archetype_weights.values()), dtype=float)
    archetype_probs /= archetype_probs.sum()

    assignments: dict[str, FraudAssignment] = {}
    labels: list[FraudLabel] = []

    for sub in subscribers:
        if sub.subscriber_id not in fraud_sub_ids:
            labels.append(FraudLabel(
                subscriber_id=sub.subscriber_id,
                is_fraud=0,
                archetype="none",
                injection_day=-1,
                notes="",
            ))
            continue

        archetype = archetype_names[int(np_rng.choice(len(archetype_names), p=archetype_probs))]

        # Injection day: fraud doesn't start on day 0 (telemetry needs history).
        injection_day = int(np_rng.integers(low=3, high=max(4, total_days - 3)))

        # Archetype-specific knobs
        params: dict[str, Any] = {}
        if archetype == "sim_swap_takeover":
            params["intl_call_count"] = int(np_rng.integers(3, 16))
            params["has_unusual_calls"] = bool(rng.random() < 0.80)   # 20% hide
        elif archetype == "irsf":
            params["burst_count"] = int(np_rng.integers(20, 81))
            params["burst_window_hours"] = int(np_rng.integers(1, 4))
            params["duration_range_sec"] = (30, 90)
        elif archetype == "subscription_fraud":
            params["activity_multiplier"] = float(np_rng.uniform(2.0, 4.0))
            params["large_purchase_amount"] = Decimal("90.00")
            params["dormant_after_day"] = injection_day + int(np_rng.integers(5, 16))
        elif archetype == "bundle_abuse":
            params["consumption_multiplier"] = float(np_rng.uniform(10.0, 50.0))
            params["repeat_purchases"] = int(np_rng.integers(1, 9))
            params["bundle_id"] = rng.choice(
                ["BND_DATA_1GB_7D", "BND_YOUTH_SOCIAL_FREE_7D"]
            )

        assignments[sub.subscriber_id] = FraudAssignment(
            subscriber_id=sub.subscriber_id,
            archetype=archetype,
            injection_day=injection_day,
            params=params,
        )
        labels.append(FraudLabel(
            subscriber_id=sub.subscriber_id,
            is_fraud=1,
            archetype=archetype,
            injection_day=injection_day,
            notes="",
        ))

    return assignments, labels


# ─── Archetype Injectors ────────────────────────────────────────────────────
# Each mutates one DailyActivity and returns it. The subscription profile
# is available for archetypes that need account context.

def _inject_sim_swap_takeover(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    assignment: FraudAssignment,
    day_index: int,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """On the swap day, add a SWAPPED activation event + burst of intl calls."""
    if day_index != assignment.injection_day:
        return activity

    swap_time = activity.day.replace(hour=3, minute=17, second=0, microsecond=0, tzinfo=timezone.utc)

    # 1. Inject the SWAPPED activation event (stored separately in run.py;
    #    we return it as a synthetic "activation_event" attached to the activity)
    setattr(activity, "activation_event", {
        "event_id": f"ACT{rng.randint(10**9, 10**10)}",
        "subscriber_id": subscriber.subscriber_id,
        "msisdn": subscriber.msisdn,
        "imsi": subscriber.imsi,
        "event_type": "SWAPPED",
        "timestamp": swap_time,
        "channel": rng.choice(["retail", "online", "call_center", "dealer", "app"]),
    })

    # 2. Add a burst of intl calls within the next 6-24h (80% of the time)
    if assignment.params["has_unusual_calls"]:
        n_calls = assignment.params["intl_call_count"]
        window_start = swap_time + timedelta(hours=2)
        for _ in range(n_calls):
            offset_sec = rng.randint(0, 3600 * 6)     # within 6h window
            ts = window_start + timedelta(seconds=offset_sec)
            # Pick a high-IRSF-risk country more often than random
            high_risk = [c for c in C.INTERNATIONAL_COUNTRIES if c.irsf_risk >= 0.4]
            country = rng.choice(high_risk if rng.random() < 0.7 else C.INTERNATIONAL_COUNTRIES)
            activity.voice_calls.append({
                "call_id": f"CALL{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
                "subscriber_id": subscriber.subscriber_id,
                "destination_number": f"{country.calling_code}{rng.randint(1_000_000, 99_999_999)}",
                "duration_sec": rng.randint(20, 120),
                "cost": Decimal(f"{0.005 * rng.randint(20, 120) * country.cost_multiplier:.4f}"),
                "timestamp": ts,
                "country_code": country.code,
            })

    return activity


def _inject_irsf(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    assignment: FraudAssignment,
    day_index: int,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """On injection day, add a burst of short calls to high-IRSF countries."""
    if day_index != assignment.injection_day:
        return activity

    n_calls = assignment.params["burst_count"]
    window_hours = assignment.params["burst_window_hours"]
    dur_min, dur_max = assignment.params["duration_range_sec"]

    burst_start = activity.day.replace(
        hour=int(np_rng.integers(8, 20)),
        minute=int(np_rng.integers(0, 60)),
        second=0, microsecond=0, tzinfo=timezone.utc,
    )

    high_risk = [c for c in C.INTERNATIONAL_COUNTRIES if c.irsf_risk >= 0.4]
    for _ in range(n_calls):
        offset_sec = rng.randint(0, 3600 * window_hours)
        ts = burst_start + timedelta(seconds=offset_sec)
        country = rng.choice(high_risk)
        dur = rng.randint(dur_min, dur_max)
        activity.voice_calls.append({
            "call_id": f"CALL{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
            "subscriber_id": subscriber.subscriber_id,
            "destination_number": f"{country.calling_code}{rng.randint(1_000_000, 99_999_999)}",
            "duration_sec": dur,
            "cost": Decimal(f"{0.005 * dur * country.cost_multiplier:.4f}"),
            "timestamp": ts,
            "country_code": country.code,
        })

    return activity


def _inject_subscription_fraud(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    assignment: FraudAssignment,
    day_index: int,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """
    Multiply activity during the fraud window, then go dormant.
    Only applies to new / unverified accounts (checked in run.py).
    """
    if day_index < assignment.injection_day:
        return activity
    if day_index > assignment.params["dormant_after_day"]:
        # Dormant: strip all activity
        activity.voice_calls = []
        activity.data_sessions = []
        activity.revenue_txs = []
        activity.bundle_purchases = []
        return activity

    # Multiplier on voice calls and data sessions
    mult = assignment.params["activity_multiplier"]
    extra_calls = int(len(activity.voice_calls) * (mult - 1))
    extra_sessions = int(len(activity.data_sessions) * (mult - 1))

    for _ in range(extra_calls):
        ts = activity.day.replace(
            hour=int(np_rng.integers(0, 24)),
            minute=int(np_rng.integers(0, 60)),
            second=0, microsecond=0, tzinfo=timezone.utc,
        )
        # Mostly international, moderate duration
        country = rng.choice(C.INTERNATIONAL_COUNTRIES)
        activity.voice_calls.append({
            "call_id": f"CALL{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
            "subscriber_id": subscriber.subscriber_id,
            "destination_number": f"{country.calling_code}{rng.randint(1_000_000, 99_999_999)}",
            "duration_sec": rng.randint(60, 400),
            "cost": Decimal(f"{0.005 * 200 * country.cost_multiplier:.4f}"),
            "timestamp": ts,
            "country_code": country.code,
        })

    for _ in range(extra_sessions):
        ts = activity.day.replace(
            hour=int(np_rng.integers(0, 24)),
            minute=int(np_rng.integers(0, 60)),
            second=0, microsecond=0, tzinfo=timezone.utc,
        )
        activity.data_sessions.append({
            "session_id": f"SESS{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
            "subscriber_id": subscriber.subscriber_id,
            "url_domain": "example.com",
            "bytes_used": int(np_rng.integers(1_000_000, 100_000_000)),
            "cost": Decimal(f"{0.002 * 100:.4f}"),
            "timestamp": ts,
            "country_code": C.DOMESTIC_COUNTRY.code,
        })

    # Big purchase on a random day mid-window
    if day_index == assignment.injection_day + 3:
        ts = activity.day.replace(hour=14, minute=22, tzinfo=timezone.utc)
        activity.bundle_purchases.append({
            "purchase_id": f"PUR{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
            "subscriber_id": subscriber.subscriber_id,
            "bundle_id": "BND_BUSINESS_50GB_30D",
            "channel": "online",
            "price": assignment.params["large_purchase_amount"],
            "timestamp": ts,
        })

    return activity


def _inject_bundle_abuse(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    assignment: FraudAssignment,
    day_index: int,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """Buy cheap bundle, consume 10-50x allowance, sometimes repeat same day."""
    if day_index != assignment.injection_day:
        return activity

    bundle_id = assignment.params["bundle_id"]
    bundle = next(b for b in C.BUNDLE_CATALOG if b.bundle_id == bundle_id)
    mult = assignment.params["consumption_multiplier"]
    repeats = assignment.params["repeat_purchases"]

    # Purchase event (once, plus repeats if any)
    purchase_time = activity.day.replace(hour=10, minute=5, tzinfo=timezone.utc)
    for i in range(repeats):
        ts = purchase_time + timedelta(minutes=i * 3)
        activity.bundle_purchases.append({
            "purchase_id": f"PUR{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
            "subscriber_id": subscriber.subscriber_id,
            "bundle_id": bundle_id,
            "channel": "ussd",
            "price": bundle.price,
            "timestamp": ts,
        })

    # Consumption burst — huge data sessions well beyond the allowance
    if bundle.allowance_type in ("data", "combo"):
        allowance_mb = max(bundle.allowance_amount, 1024)
        total_mb = allowance_mb * mult
        n_sessions = int(np_rng.integers(5, 20))
        per_session_mb = total_mb / n_sessions
        for _ in range(n_sessions):
            ts = purchase_time + timedelta(
                hours=int(np_rng.integers(0, 24)),
                minutes=int(np_rng.integers(0, 60)),
            )
            activity.data_sessions.append({
                "session_id": f"SESS{int(ts.timestamp() * 1000)}{rng.randint(1000, 9999)}",
                "subscriber_id": subscriber.subscriber_id,
                "url_domain": "cdn.example",
                "bytes_used": int(per_session_mb * 1024 * 1024),
                "cost": Decimal("0.0000"),       # bundle consumed, not billed
                "timestamp": ts,
                "country_code": C.DOMESTIC_COUNTRY.code,
            })

    return activity


# ─── Dispatcher ─────────────────────────────────────────────────────────────

_ARCHETYPE_FUNCS = {
    "sim_swap_takeover": _inject_sim_swap_takeover,
    "irsf": _inject_irsf,
    "subscription_fraud": _inject_subscription_fraud,
    "bundle_abuse": _inject_bundle_abuse,
}


def inject_fraud_for_day(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    assignment: FraudAssignment | None,
    day_index: int,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """
    If this subscriber has a fraud assignment, apply the archetype-specific
    mutation for this day. Otherwise, return the activity unchanged.
    """
    if assignment is None:
        return activity

    func = _ARCHETYPE_FUNCS.get(assignment.archetype)
    if func is None:
        return activity

    return func(activity, subscriber, assignment, day_index, rng, np_rng)