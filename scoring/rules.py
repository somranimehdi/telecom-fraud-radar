"""Explainable Week 4 fraud rules over validated Silver records.

These produce candidate alerts for analyst review. Bundle-consumption abuse
cannot be measured until the deferred bundle-usage source is added; the
available purchase stream supports a repeat-purchase proxy only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import yaml

from transform.gold import SILVER_SOURCES, _read_silver


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value is None:
        return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _deduplicate(rows: list[dict], id_field: str) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for row in rows:
        event_id = str(row.get(id_field, ""))
        if event_id and event_id not in seen:
            seen.add(event_id)
            unique.append(row)
    return unique


def _scan(table) -> list[dict]:
    items: list[dict] = []
    response = table.scan()
    while True:
        items.extend(response.get("Items", []))
        key = response.get("LastEvaluatedKey")
        if not key:
            return items
        response = table.scan(ExclusiveStartKey=key)


def _alert(rule_id: str, subscriber_id: str, anchor: str, fraud_type: str,
           severity: str, score: str, evidence: dict) -> dict:
    identity = f"{rule_id}|{subscriber_id}|{anchor}"
    alert_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return {
        "alert_id": alert_id,
        "subscriber_id": subscriber_id,
        "rule_id": rule_id,
        "fraud_type": fraud_type,
        "severity": severity,
        "score": Decimal(score),
        "status": "CANDIDATE",
        "event_timestamp": anchor,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "evidence": json.dumps(evidence, sort_keys=True, default=str),
    }


def generate_candidates(s3, bucket: str, profiles: dict[str, dict], rules_cfg: dict) -> list[dict]:
    activation = _deduplicate(
        _read_silver(s3, bucket, SILVER_SOURCES["activation"]).to_dicts(), "event_id"
    )
    voice = _deduplicate(
        _read_silver(s3, bucket, SILVER_SOURCES["voice"]).to_dicts(), "call_id"
    )
    purchases = _deduplicate(
        _read_silver(s3, bucket, SILVER_SOURCES["purchases"]).to_dicts(), "purchase_id"
    )
    revenue = _deduplicate(
        _read_silver(s3, bucket, SILVER_SOURCES["revenue"]).to_dicts(), "transaction_id"
    )
    alerts: list[dict] = []

    voice_by_subscriber: dict[str, list[dict]] = {}
    for row in voice:
        row["_ts"] = _parse_timestamp(row.get("timestamp"))
        if row["_ts"]:
            voice_by_subscriber.setdefault(str(row["subscriber_id"]), []).append(row)
    for rows in voice_by_subscriber.values():
        rows.sort(key=lambda row: row["_ts"])

    # Rule 1: a SIM swap followed by international calls within 24 hours.
    for event in activation:
        if event.get("event_type") != "SWAPPED":
            continue
        swapped_at = _parse_timestamp(event.get("timestamp"))
        sid = str(event.get("subscriber_id", ""))
        if not swapped_at or not sid:
            continue
        end = swapped_at + timedelta(hours=24)
        calls = [
            call for call in voice_by_subscriber.get(sid, [])
            if call["_ts"] >= swapped_at and call["_ts"] < end
            and str(call.get("country_code", "")).upper() != "TN"
        ]
        if calls:
            alerts.append(_alert(
                "SIM_SWAP_INTL_24H", sid, swapped_at.isoformat(), "sim_swap_takeover",
                "HIGH", "0.90", {
                    "activation_event_id": event.get("event_id"),
                    "international_calls_24h": len(calls),
                    "international_cost_24h": str(sum(Decimal(str(c.get("cost", 0))) for c in calls)),
                },
            ))

    # Rule 2: concentrated calls to the configured high-risk destinations.
    high_risk = {str(code).upper() for code in rules_cfg["irsf_high_risk_countries"]}
    min_calls = int(rules_cfg["irsf_min_calls_24h"])
    for sid, calls in voice_by_subscriber.items():
        risk_calls = [call for call in calls if str(call.get("country_code", "")).upper() in high_risk]
        right = 0
        best: dict[str, tuple[int, Decimal, str]] = {}
        for left, call in enumerate(risk_calls):
            right = max(right, left)
            while right < len(risk_calls) and risk_calls[right]["_ts"] < call["_ts"] + timedelta(hours=24):
                right += 1
            count = right - left
            if count >= min_calls:
                day = call["_ts"].date().isoformat()
                cost = sum(Decimal(str(item.get("cost", 0))) for item in risk_calls[left:right])
                candidate = (count, cost, call["_ts"].isoformat())
                if day not in best or count > best[day][0]:
                    best[day] = candidate
        for count, cost, anchor in best.values():
            alerts.append(_alert(
                "IRSF_HIGH_RISK_BURST", sid, anchor, "irsf", "CRITICAL", "0.95",
                {"high_risk_call_count_24h": count, "high_risk_cost_24h": str(cost),
                 "destinations": sorted(high_risk)},
            ))

    # Build completed-revenue timeline to evaluate the seven days before a purchase.
    paid_by_subscriber: dict[str, list[datetime]] = {}
    for row in revenue:
        if row.get("status") == "completed":
            ts = _parse_timestamp(row.get("timestamp"))
            if ts:
                paid_by_subscriber.setdefault(str(row["subscriber_id"]), []).append(ts)

    # Rule 3: high-value purchases on new / non-verified accounts with no recent payment.
    age_limit = int(rules_cfg["new_account_max_days"])
    price_limit = Decimal(str(rules_cfg["high_value_purchase_min_tnd"]))
    for purchase in purchases:
        sid = str(purchase.get("subscriber_id", ""))
        profile = profiles.get(sid, {})
        ts = _parse_timestamp(purchase.get("timestamp"))
        try:
            price = Decimal(str(purchase.get("price", "0")))
            age = int(profile.get("account_age_days", -1))
        except (ValueError, ArithmeticError):
            continue
        if not ts or price < price_limit or not profile:
            continue
        risky_profile = age >= 0 and age <= age_limit or profile.get("kyc_status") != "verified"
        recent_payment = any(ts - timedelta(days=7) <= paid_at <= ts
                             for paid_at in paid_by_subscriber.get(sid, []))
        if risky_profile and not recent_payment:
            alerts.append(_alert(
                "NEW_ACCOUNT_HIGH_VALUE_PURCHASE", sid, ts.isoformat(), "subscription_fraud",
                "HIGH", "0.80", {
                    "purchase_id": purchase.get("purchase_id"), "bundle_id": purchase.get("bundle_id"),
                    "purchase_price_tnd": str(price), "account_age_days": age,
                    "kyc_status": profile.get("kyc_status"), "completed_payment_in_previous_7d": False,
                },
            ))

    # Rule 4: repeated purchases of the same bundle in a rolling 24-hour window.
    purchases_by_key: dict[tuple[str, str], list[dict]] = {}
    for row in purchases:
        row["_ts"] = _parse_timestamp(row.get("timestamp"))
        if row["_ts"]:
            key = (str(row.get("subscriber_id", "")), str(row.get("bundle_id", "")))
            purchases_by_key.setdefault(key, []).append(row)
    min_purchases = int(rules_cfg["repeated_bundle_min_purchases_24h"])
    for (sid, bundle_id), rows in purchases_by_key.items():
        rows.sort(key=lambda row: row["_ts"])
        right = 0
        best: tuple[int, Decimal, str] | None = None
        for left, row in enumerate(rows):
            right = max(right, left)
            while right < len(rows) and rows[right]["_ts"] < row["_ts"] + timedelta(hours=24):
                right += 1
            count = right - left
            if count >= min_purchases and (best is None or count > best[0]):
                spend = sum(Decimal(str(item.get("price", 0))) for item in rows[left:right])
                best = (count, spend, row["_ts"].isoformat())
        if best:
            count, spend, anchor = best
            alerts.append(_alert(
                "REPEATED_BUNDLE_PURCHASES", sid, anchor, "bundle_abuse", "MEDIUM", "0.70",
                {"bundle_id": bundle_id, "purchase_count_24h": count, "spend_24h": str(spend),
                 "limitation": "bundle usage is not ingested; this is a repeat-purchase proxy"},
            ))

    # Deterministic alert IDs make reruns safe and updates idempotent.
    return list({item["alert_id"]: item for item in alerts}.values())


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate explainable fraud rules against Silver data.")
    parser.add_argument("--config", default="simulator/config.yaml")
    parser.add_argument("--rules-config", default="scoring/rules_config.yaml")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()
    with Path(args.config).open() as handle:
        cfg = yaml.safe_load(handle)
    with Path(args.rules_config).open() as handle:
        rules_cfg = yaml.safe_load(handle)["rules"]

    common = {
        "endpoint_url": args.endpoint,
        "region_name": cfg.get("region", "us-east-1"),
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    s3 = boto3.client("s3", **common)
    ddb = boto3.resource("dynamodb", **common)
    profiles = {
        str(item["subscriber_id"]): item
        for item in _scan(ddb.Table(cfg["paths"]["dynamodb_subscribers"]))
    }
    candidates = generate_candidates(s3, cfg["paths"]["s3_bucket"], profiles, rules_cfg)
    table = ddb.Table("fraud_alerts")
    for candidate in candidates:
        table.put_item(Item=candidate)
    print(f"Evaluated Silver sources; upserted {len(candidates):,} candidate alert(s) to fraud_alerts")
    by_rule: dict[str, int] = {}
    for candidate in candidates:
        by_rule[candidate["rule_id"]] = by_rule.get(candidate["rule_id"], 0) + 1
    for rule_id, count in sorted(by_rule.items()):
        print(f"  {rule_id}: {count}")


if __name__ == "__main__":
    main()
