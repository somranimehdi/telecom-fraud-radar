"""
Simulator Constants
-------------------
Static data used across the simulator. Not user-tunable — see config.yaml
for knobs. This file defines the *shape of reality* the simulator imitates:

  - Which countries appear as destinations (with cost + IRSF risk)
  - Which bundle products exist in the catalog
  - Which subscriber segments exist
  - Which KYC statuses exist
  - Realistic IMSI / MSISDN prefixes for a Tunisian telco
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


# ─── Destination Countries ──────────────────────────────────────────────────
# Used by VoiceCDR / DataCDR generators. Config references the codes;
# this table supplies cost multipliers and IRSF risk scores.

@dataclass(frozen=True)
class Country:
    code: str                     # ISO 3166-1 alpha-2
    name: str
    calling_code: str             # international dialing prefix
    cost_multiplier: float        # relative to domestic
    irsf_risk: float              # 0.0–1.0; higher = classic IRSF destination


DOMESTIC_COUNTRY = Country(
    code="TN",
    name="Tunisia",
    calling_code="+216",
    cost_multiplier=1.0,
    irsf_risk=0.0,
)

INTERNATIONAL_COUNTRIES: list[Country] = [
    Country(code="GB", name="United Kingdom", calling_code="+44",  cost_multiplier=3.0,  irsf_risk=0.05),
    Country(code="FR", name="France",         calling_code="+33",  cost_multiplier=2.5,  irsf_risk=0.03),
    Country(code="US", name="United States",  calling_code="+1",   cost_multiplier=4.0,  irsf_risk=0.10),
    Country(code="SO", name="Somalia",        calling_code="+252", cost_multiplier=15.0, irsf_risk=0.75),
    Country(code="PK", name="Pakistan",       calling_code="+92",  cost_multiplier=8.0,  irsf_risk=0.40),
    Country(code="LV", name="Latvia",         calling_code="+371", cost_multiplier=12.0, irsf_risk=0.85),
]


# ─── Bundle Catalog Seeds ───────────────────────────────────────────────────
# The bundles the telco sells. Loaded into DynamoDB `bundle_catalog`
# and referenced by `bundle_id` in the purchase stream.

@dataclass(frozen=True)
class BundleSeed:
    bundle_id: str
    name: str
    price: Decimal
    allowance_type: str           # "voice" | "data" | "sms" | "combo"
    allowance_amount: int         # minutes / MB / SMS count
    validity_days: int
    segment: str                  # "consumer" | "business" | "youth" | "roaming"


BUNDLE_CATALOG: list[BundleSeed] = [
    BundleSeed(
        bundle_id="BND_DATA_1GB_7D",
        name="Data 1GB - 7 Days",
        price=Decimal("5.00"),
        allowance_type="data",
        allowance_amount=1024,
        validity_days=7,
        segment="consumer",
    ),
    BundleSeed(
        bundle_id="BND_DATA_10GB_30D",
        name="Data 10GB - 30 Days",
        price=Decimal("25.00"),
        allowance_type="data",
        allowance_amount=10240,
        validity_days=30,
        segment="consumer",
    ),
    BundleSeed(
        bundle_id="BND_VOICE_500MIN_7D",
        name="Voice 500 min - 7 Days",
        price=Decimal("5.00"),
        allowance_type="voice",
        allowance_amount=500,
        validity_days=7,
        segment="consumer",
    ),
    BundleSeed(
        bundle_id="BND_VOICE_2000MIN_30D",
        name="Voice 2000 min - 30 Days",
        price=Decimal("20.00"),
        allowance_type="voice",
        allowance_amount=2000,
        validity_days=30,
        segment="consumer",
    ),
    BundleSeed(
        bundle_id="BND_COMBO_5GB_500MIN_30D",
        name="Combo 5GB + 500 min - 30 Days",
        price=Decimal("30.00"),
        allowance_type="combo",
        allowance_amount=5632,     # 5GB (5120 MB) + 500 min
        validity_days=30,
        segment="consumer",
    ),
    BundleSeed(
        bundle_id="BND_BUSINESS_50GB_30D",
        name="Business Data 50GB - 30 Days",
        price=Decimal("90.00"),
        allowance_type="data",
        allowance_amount=51200,
        validity_days=30,
        segment="business",
    ),
    BundleSeed(
        bundle_id="BND_YOUTH_SOCIAL_FREE_7D",
        name="Youth Social Free - 7 Days",
        price=Decimal("0.00"),
        allowance_type="combo",
        allowance_amount=0,          # add-on: unlocks social apps, no base allowance
        validity_days=7,
        segment="youth",
    ),
    BundleSeed(
        bundle_id="BND_ROAMING_EU_5GB_7D",
        name="EU Roaming 5GB - 7 Days",
        price=Decimal("35.00"),
        allowance_type="data",
        allowance_amount=5120,
        validity_days=7,
        segment="roaming",
    ),
]


# ─── Subscriber Segments ────────────────────────────────────────────────────
# Weighted distribution — most subscribers are consumers.

SEGMENT_WEIGHTS: dict[str, float] = {
    "consumer":   0.75,
    "business":   0.10,
    "youth":      0.10,
    "roaming":    0.05,
}


# ─── KYC Statuses ───────────────────────────────────────────────────────────
# Weighted distribution — most accounts are verified.

KYC_STATUS_WEIGHTS: dict[str, float] = {
    "verified":   0.80,
    "pending":    0.12,
    "unverified": 0.06,
    "rejected":   0.02,
}


# ─── Tunisian IMSI Prefixes ─────────────────────────────────────────────────
# MCC-MNC prefixes for Tunisian operators. Realistic touch.
# IMSI is MCC(3) + MNC(2-3) + MSIN(9-10) = 14-16 digits total.

IMSI_PREFIXES: list[str] = [
    "60501",   # Orange Tunisia (MCC 605, MNC 01)
    "60502",   # Tunisie Telecom
    "60503",   # Ooredoo Tunisia
]


# ─── MSISDN Format ──────────────────────────────────────────────────────────
# Tunisian mobile numbers: +216 2X / 4X / 5X / 9X followed by 6 digits.

MSISDN_PREFIXES: list[str] = ["20", "21", "22", "23", "24", "25",
                              "40", "41", "42", "50", "51", "52",
                              "90", "91", "92", "93", "94", "95", "96", "97", "98"]


# ─── Channel Names ──────────────────────────────────────────────────────────
# Kept as plain strings here; the enum lives in contracts/.
# The simulator uses these to draw weighted samples.

ACTIVATION_CHANNELS: list[str] = ["retail", "online", "call_center", "dealer", "app"]

PURCHASE_CHANNELS: list[str] = ["retail", "online", "call_center", "dealer", "app", "ussd"]


# ─── Fraud Archetype Names ──────────────────────────────────────────────────
# Must match the keys in config.yaml `fraud.archetype_weights`.

FRAUD_ARCHETYPES: list[str] = [
    "sim_swap_takeover",
    "irsf",
    "subscription_fraud",
    "bundle_abuse",
]