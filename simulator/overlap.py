"""
Simulator — Overlap (Hard Cases)
---------------------------------
Deliberately introduces ambiguity into the dataset:

  1. LEGIT-LOOKS-FRAUD: legitimate subscribers whose behavior mimics
     common fraud patterns (travelers, heavy streamers, call centers).
     Label stays 0.

  2. FRAUD-LOOKS-LEGIT: fraud subscribers whose behavior is subdued —
     low-and-slow fraud, delayed utilization, spread-out bursts.
     Label stays 1.

The purpose is to prevent the model from learning a single-feature
threshold. If a legit traveler calls Somalia 20 times, the model cannot
just learn "intl calls > 15 → fraud". It must look at interactions.

This module mutates activities. It does not change labels.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from random import Random
from typing import Any

import numpy as np

from simulator import constants as C
from simulator.baseline import SubscriberProfile, DailyActivity
from simulator.fraud_injector import FraudAssignment, FraudLabel


# ─── Assignment ─────────────────────────────────────────────────────────────

def assign_overlap_subscribers(
    subscribers: list[SubscriberProfile],
    labels: list[FraudLabel],
    cfg: dict,
    seed: int,
) -> tuple[set[str], set[str]]:
    """
    Choose which subscribers get overlap treatment.

    Returns:
      - legit_fraud_like : set of subscriber_ids that are LEGIT but look fraud-like
      - fraud_legit_like : set of subscriber_ids that are FRAUD but look legit-like
    """
    rng = Random(seed + 1)   # Different seed stream from fraud assignment

    legit_ids = [l.subscriber_id for l in labels if l.is_fraud == 0]
    fraud_ids = [l.subscriber_id for l in labels if l.is_fraud == 1]

    n_legit_like_fraud = int(round(len(legit_ids) * cfg["overlap"]["legit_looking_fraud_rate"]))
    n_fraud_like_legit = int(round(len(fraud_ids) * cfg["overlap"]["fraud_looking_legit_rate"]))

    legit_fraud_like = set(rng.sample(legit_ids, min(n_legit_like_fraud, len(legit_ids))))
    fraud_legit_like = set(rng.sample(fraud_ids, min(n_fraud_like_legit, len(fraud_ids))))

    return legit_fraud_like, fraud_legit_like


# ─── Legit-looks-fraud ──────────────────────────────────────────────────────

def _legit_traveler(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """Business traveler: 5-15 intl calls in a day, scattered across hours."""
    n_extra = int(np_rng.integers(5, 16))
    for _ in range(n_extra):
        ts = activity.day.replace(
            hour=int(np_rng.integers(6, 22)),
            minute=int(np_rng.integers(0, 60)),
            second=0, microsecond=0, tzinfo=timezone.utc,
        )
        country = rng.choice(C.INTERNATIONAL_COUNTRIES)
        dur = rng.randint(60, 400)
        activity.voice_calls.append({
            "call_id": f"CALL{int(ts.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
            "subscriber_id": subscriber.subscriber_id,
            "destination_number": f"{country.calling_code}{rng.randint(1_000_000, 99_999_999)}",
            "duration_sec": dur,
            "cost": Decimal(f"{0.005 * dur * country.cost_multiplier:.4f}"),
            "timestamp": ts,
            "country_code": country.code,
        })
    return activity


def _legit_streamer(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """Heavy streamer: adds large data sessions (multiple GB)."""
    n_extra = int(np_rng.integers(3, 8))
    for _ in range(n_extra):
        ts = activity.day.replace(
            hour=int(np_rng.integers(18, 24)),
            minute=int(np_rng.integers(0, 60)),
            second=0, microsecond=0, tzinfo=timezone.utc,
        )
        activity.data_sessions.append({
            "session_id": f"SESS{int(ts.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
            "subscriber_id": subscriber.subscriber_id,
            "url_domain": "streaming.example",
            "bytes_used": int(np_rng.integers(500_000_000, 3_000_000_000)),  # 0.5–3 GB
            "cost": Decimal("0.0000"),
            "timestamp": ts,
            "country_code": C.DOMESTIC_COUNTRY.code,
        })
    return activity


def _legit_call_center(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """Call center: burst of short, mostly domestic calls during work hours."""
    n_extra = int(np_rng.integers(30, 60))
    for _ in range(n_extra):
        ts = activity.day.replace(
            hour=int(np_rng.integers(9, 18)),
            minute=int(np_rng.integers(0, 60)),
            second=0, microsecond=0, tzinfo=timezone.utc,
        )
        activity.voice_calls.append({
            "call_id": f"CALL{int(ts.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
            "subscriber_id": subscriber.subscriber_id,
            "destination_number": f"+216{rng.randint(20_000_000, 99_999_999)}",
            "duration_sec": rng.randint(30, 180),
            "cost": Decimal(f"{0.005 * 100 * 1.0:.4f}"),
            "timestamp": ts,
            "country_code": C.DOMESTIC_COUNTRY.code,
        })
    return activity


def apply_legit_looks_fraud(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """
    Apply a legit-looks-fraud pattern to one day's activity.
    Each call picks one of three flavors at random.
    """
    flavor = rng.choices(
        ["traveler", "streamer", "call_center"],
        weights=[0.4, 0.35, 0.25],
    )[0]

    if flavor == "traveler":
        activity = _legit_traveler(activity, subscriber, rng, np_rng)
    elif flavor == "streamer":
        activity = _legit_streamer(activity, subscriber, rng, np_rng)
    else:
        activity = _legit_call_center(activity, subscriber, rng, np_rng)

    setattr(activity, "overlap_flavor", f"legit_{flavor}")
    return activity


# ─── Fraud-looks-legit ──────────────────────────────────────────────────────

def apply_fraud_looks_legit(
    activity: DailyActivity,
    subscriber: SubscriberProfile,
    assignment: FraudAssignment,
    day_index: int,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """
    Dampen fraud patterns so they're harder to detect.

    Strategy depends on archetype:
      - sim_swap_takeover: attacker swaps SIM but doesn't use it immediately.
      - irsf: spread the burst over multiple days (fewer calls per day).
      - subscription_fraud: reduce activity multiplier.
      - bundle_abuse: lower consumption multiplier.
    """
    if day_index != assignment.injection_day:
        return activity

    archetype = assignment.archetype

    if archetype == "sim_swap_takeover":
        # Remove 80% of the intl calls that were injected.
        keep = rng.sample(
            activity.voice_calls,
            k=max(0, int(len(activity.voice_calls) * 0.2)),
        )
        activity.voice_calls = keep
        # Also increase duration — real fraud calls look more legit.
        for call in activity.voice_calls:
            call["duration_sec"] = call["duration_sec"] * 3
        setattr(activity, "overlap_flavor", "fraud_low_and_slow")

    elif archetype == "irsf":
        # Keep only 20% of burst calls; they'll be spread over multiple days
        # by keeping injection_day calls minimal.
        keep = rng.sample(
            activity.voice_calls,
            k=max(0, int(len(activity.voice_calls) * 0.2)),
        )
        activity.voice_calls = keep
        setattr(activity, "overlap_flavor", "fraud_spread_burst")

    elif archetype == "subscription_fraud":
        # Cut activity back — closer to a normal new account.
        keep = rng.sample(
            activity.voice_calls,
            k=max(0, int(len(activity.voice_calls) * 0.3)),
        )
        activity.voice_calls = keep
        setattr(activity, "overlap_flavor", "fraud_mild_activity")

    elif archetype == "bundle_abuse":
        # Reduce consumption multiplier effect by removing some sessions.
        keep = rng.sample(
            activity.data_sessions,
            k=max(0, int(len(activity.data_sessions) * 0.3)),
        )
        activity.data_sessions = keep
        setattr(activity, "overlap_flavor", "fraud_low_consumption")

    return activity