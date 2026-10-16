"""
Simulator — Baseline (Legitimate Behavior)
--------------------------------------------
Generates realistic LEGITIMATE subscriber behavior. Fraud and overlap
modules consume this output.

Design:
  - Counts (calls, sessions, top-ups) are Poisson-distributed.
  - Durations and amounts are log-normal (heavy-tailed).
  - Activity is multiplied by hour-of-day and day-of-week seasonality.
  - Destinations are weighted (mostly domestic).
  - Reference tables (subscriber_profile, bundle_catalog) are produced once.

Nothing in this module knows about fraud. Fraud is injected separately.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from random import Random
from typing import Any

import numpy as np

from simulator import constants as C


# ─── Data Classes ───────────────────────────────────────────────────────────

@dataclass
class SubscriberProfile:
    """One subscriber's identity + account info. Matches Source 8 contract."""
    subscriber_id: str
    msisdn: str
    imsi: str
    segment: str
    kyc_status: str
    account_age_days: int
    lines_per_identity: int
    risk_score_baseline: Decimal
    created_at: datetime


@dataclass
class DailyActivity:
    """One subscriber's behavior on one day — purely legit, before fraud."""
    subscriber_id: str
    day: datetime
    voice_calls: list[dict[str, Any]] = field(default_factory=list)
    data_sessions: list[dict[str, Any]] = field(default_factory=list)
    revenue_txs: list[dict[str, Any]] = field(default_factory=list)
    bundle_purchases: list[dict[str, Any]] = field(default_factory=list)


# ─── Subscriber Generation ──────────────────────────────────────────────────

def _weighted_choice(rng: np.random.Generator, weights: dict[str, float]) -> str:
    keys = list(weights.keys())
    probs = np.array([weights[k] for k in keys], dtype=float)
    probs /= probs.sum()
    return keys[int(rng.choice(len(keys), p=probs))]


def _make_msisdn(rng: Random) -> str:
    """Tunisian mobile: +216 + 2-digit prefix + 6-digit number."""
    prefix = rng.choice(C.MSISDN_PREFIXES)
    suffix = f"{rng.randint(0, 999_999):06d}"
    return f"+216{prefix}{suffix}"


def _make_imsi(rng: Random) -> str:
    """15-digit IMSI: 5-digit Tunisian prefix + 10-digit MSIN."""
    prefix = rng.choice(C.IMSI_PREFIXES)
    msin = f"{rng.randint(0, 9_999_999_999):010d}"
    return f"{prefix}{msin}"


def generate_subscribers(
    n: int,
    start_date: datetime,
    seed: int,
) -> list[SubscriberProfile]:
    """Generate `n` legitimate subscriber profiles."""
    rng = Random(seed)
    np_rng = np.random.default_rng(seed)
    subscribers: list[SubscriberProfile] = []

    for i in range(n):
        sub_id = f"SUB{i:06d}"
        segment = _weighted_choice(np_rng, C.SEGMENT_WEIGHTS)
        kyc_status = _weighted_choice(np_rng, C.KYC_STATUS_WEIGHTS)

        # Account age: most subscribers are established, some are new.
        # Log-normal-ish: exp(6.5) ≈ 665 days median, long tail both ways.
        account_age = int(np.clip(np_rng.lognormal(mean=6.0, sigma=1.0), 0, 5000))

        # lines_per_identity: mostly 1, business/enterprise higher.
        if segment == "business":
            lines = int(np_rng.integers(2, 30))
        else:
            lines = int(np_rng.choice([1, 1, 1, 1, 2, 2, 3], p=[0.5, 0.2, 0.1, 0.05, 0.08, 0.05, 0.02]))

        # Baseline risk: new + unverified = higher risk.
        base_risk = 0.10
        if kyc_status in ("pending", "unverified"):
            base_risk += 0.20
        if account_age < 30:
            base_risk += 0.15
        if lines > 5:
            base_risk += 0.05
        # Add noise so the score isn't deterministic.
        risk = float(np.clip(base_risk + np_rng.normal(0, 0.05), 0.0, 1.0))

        created_at = start_date - timedelta(days=account_age)

        subscribers.append(
            SubscriberProfile(
                subscriber_id=sub_id,
                msisdn=_make_msisdn(rng),
                imsi=_make_imsi(rng),
                segment=segment,
                kyc_status=kyc_status,
                account_age_days=account_age,
                lines_per_identity=lines,
                risk_score_baseline=Decimal(f"{risk:.4f}"),
                created_at=created_at,
            )
        )

    return subscribers


# ─── Seasonality ────────────────────────────────────────────────────────────

def _hour_weight(cfg: dict, hour: int) -> float:
    if not cfg["seasonality"]["enabled"]:
        return 1.0
    return cfg["seasonality"]["hour_weights"][hour]


def _day_weight(cfg: dict, weekday: int) -> float:
    """weekday: 0=Monday .. 6=Sunday"""
    if not cfg["seasonality"]["enabled"]:
        return 1.0
    return cfg["seasonality"]["day_weights"][weekday]


def _pick_hour(np_rng: np.random.Generator, cfg: dict) -> int:
    """Draw an hour-of-day biased by seasonality weights."""
    weights = np.array(cfg["seasonality"]["hour_weights"], dtype=float)
    if not cfg["seasonality"]["enabled"]:
        weights = np.ones(24)
    weights /= weights.sum()
    return int(np_rng.choice(24, p=weights))


# ─── Daily Activity Generation ──────────────────────────────────────────────

def _make_voice_call(
    subscriber: SubscriberProfile,
    timestamp: datetime,
    cfg: dict,
    np_rng: np.random.Generator,
    rng: Random,
) -> dict[str, Any]:
    """One voice CDR, legit baseline (no fraud)."""
    # Duration: log-normal, mu/sigma from config
    dur = int(np.clip(np_rng.lognormal(cfg["baseline"]["voice_duration_mu"],
                                       cfg["baseline"]["voice_duration_sigma"]),
                      1, 86400))

    # Domestic vs international
    is_intl = rng.random() < (1 - cfg["destinations"]["domestic_share"])
    if is_intl:
        country = rng.choice(C.INTERNATIONAL_COUNTRIES)
        dest = f"{country.calling_code}{rng.randint(1_000_000, 99_999_999)}"
        cc = country.code
        cost_mult = country.cost_multiplier
    else:
        dest = f"+216{rng.randint(20_000_000, 99_999_999)}"
        cc = C.DOMESTIC_COUNTRY.code
        cost_mult = C.DOMESTIC_COUNTRY.cost_multiplier

    # Cost: base rate * duration * country multiplier, in TND
    base_per_sec = 0.005      # ~0.30 TND/min domestic
    cost = Decimal(f"{base_per_sec * dur * cost_mult:.4f}")

    return {
        "call_id": f"CALL{int(timestamp.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
        "subscriber_id": subscriber.subscriber_id,
        "destination_number": dest,
        "duration_sec": dur,
        "cost": cost,
        "timestamp": timestamp,
        "country_code": cc,
    }


def _make_data_session(
    subscriber: SubscriberProfile,
    timestamp: datetime,
    cfg: dict,
    np_rng: np.random.Generator,
    rng: Random,
) -> dict[str, Any]:
    """One data CDR, legit baseline."""
    # Bytes: log-normal in MB, then to bytes
    mb = float(np.clip(np_rng.lognormal(cfg["baseline"]["data_bytes_mu"],
                                        cfg["baseline"]["data_bytes_sigma"]),
                       0.01, 50_000))
    bytes_used = int(mb * 1024 * 1024)

    is_intl = rng.random() < 0.05
    if is_intl:
        country = rng.choice(C.INTERNATIONAL_COUNTRIES)
        cc = country.code
        cost_mult = country.cost_multiplier
    else:
        cc = C.DOMESTIC_COUNTRY.code
        cost_mult = C.DOMESTIC_COUNTRY.cost_multiplier

    base_per_mb = 0.002
    cost = Decimal(f"{base_per_mb * mb * cost_mult:.4f}")

    return {
        "session_id": f"SESS{int(timestamp.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
        "subscriber_id": subscriber.subscriber_id,
        "url_domain": rng.choice(["example.com", "news.tn", "social.app", "video.site", None]),
        "bytes_used": bytes_used,
        "cost": cost,
        "timestamp": timestamp,
        "country_code": cc,
    }


def _make_revenue_tx(
    subscriber: SubscriberProfile,
    timestamp: datetime,
    cfg: dict,
    np_rng: np.random.Generator,
    rng: Random,
) -> dict[str, Any]:
    """One revenue transaction (top-up or invoice payment)."""
    if subscriber.segment == "business":
        ptype = rng.choice(["invoice_payment", "adjustment"])
    else:
        ptype = rng.choice(["topup", "topup", "topup", "adjustment", "refund"])

    # Amount: log-normal
    amt = float(np.clip(np_rng.lognormal(cfg["baseline"]["revenue_amount_mu"],
                                         cfg["baseline"]["revenue_amount_sigma"]),
                        0.5, 5000))
    if ptype == "refund":
        amt = -amt
    amount = Decimal(f"{amt:.2f}")

    return {
        "transaction_id": f"TXN{int(timestamp.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
        "subscriber_id": subscriber.subscriber_id,
        "payment_type": ptype,
        "amount": amount,
        "currency": "TND",
        "timestamp": timestamp,
        "status": rng.choice(["completed", "completed", "completed", "pending", "failed"]),
    }


def _make_bundle_purchase(
    subscriber: SubscriberProfile,
    timestamp: datetime,
    cfg: dict,
    rng: Random,
) -> dict[str, Any]:
    """One bundle purchase. Uses constants catalog."""
    # Prefer bundles matching the subscriber's segment; sometimes others.
    matching = [b for b in C.BUNDLE_CATALOG if b.segment == subscriber.segment]
    pool = matching if matching and rng.random() < 0.75 else C.BUNDLE_CATALOG
    bundle = rng.choice(pool)

    channel_weights = cfg["channels"]["purchase_channel_weights"]
    channel = rng.choices(list(channel_weights.keys()),
                          weights=list(channel_weights.values()))[0]

    return {
        "purchase_id": f"PUR{int(timestamp.timestamp() * 1000)}{rng.randint(10**9, 10**10 - 1)}",
        "subscriber_id": subscriber.subscriber_id,
        "bundle_id": bundle.bundle_id,
        "channel": channel,
        "price": bundle.price,
        "timestamp": timestamp,
    }


def generate_daily_activity(
    subscriber: SubscriberProfile,
    day: datetime,
    cfg: dict,
    rng: Random,
    np_rng: np.random.Generator,
) -> DailyActivity:
    """Generate one subscriber's legit behavior for a single day."""
    activity = DailyActivity(subscriber_id=subscriber.subscriber_id, day=day)

    day_w = _day_weight(cfg, day.weekday())

    # Voice calls — Poisson with hour seasonality
    n_calls = int(np_rng.poisson(cfg["baseline"]["voice_calls_per_day_mean"] * day_w))
    for _ in range(n_calls):
        hour = _pick_hour(np_rng, cfg)
        minute = int(np_rng.integers(0, 60))
        ts = day.replace(hour=hour, minute=minute, second=0, microsecond=0, tzinfo=timezone.utc)
        activity.voice_calls.append(_make_voice_call(subscriber, ts, cfg, np_rng, rng))

    # Data sessions
    n_sessions = int(np_rng.poisson(cfg["baseline"]["data_sessions_per_day_mean"] * day_w))
    for _ in range(n_sessions):
        hour = _pick_hour(np_rng, cfg)
        ts = day.replace(hour=hour, minute=int(np_rng.integers(0, 60)),
                         second=0, microsecond=0, tzinfo=timezone.utc)
        activity.data_sessions.append(_make_data_session(subscriber, ts, cfg, np_rng, rng))

    # Revenue transactions
    n_txs = int(np_rng.poisson(cfg["baseline"]["revenue_tx_per_day_mean"] * day_w))
    for _ in range(n_txs):
        hour = _pick_hour(np_rng, cfg)
        ts = day.replace(hour=hour, minute=int(np_rng.integers(0, 60)),
                         second=0, microsecond=0, tzinfo=timezone.utc)
        activity.revenue_txs.append(_make_revenue_tx(subscriber, ts, cfg, np_rng, rng))

    # Bundle purchases (very rare)
    n_purchases = int(np_rng.poisson(cfg["baseline"]["bundle_purchases_per_day_mean"] * day_w))
    for _ in range(n_purchases):
        hour = _pick_hour(np_rng, cfg)
        ts = day.replace(hour=hour, minute=int(np_rng.integers(0, 60)),
                         second=0, microsecond=0, tzinfo=timezone.utc)
        activity.bundle_purchases.append(_make_bundle_purchase(subscriber, ts, cfg, rng))

    return activity