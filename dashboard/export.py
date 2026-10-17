"""Export alert and quality-gate state from LocalStack for the dashboard."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3


BUCKET = "telco-fraud-bronze"
QUALITY_PREFIX = "quality_metrics/"


class _DecimalSafeEncoder(json.JSONEncoder):
    """Convert DynamoDB Decimal values to JSON numbers."""

    def default(self, obj: Any):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _clients():
    common = {
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return boto3.resource("dynamodb", **common), boto3.client("s3", **common)


def _scan_alerts(table) -> list[dict]:
    items = []
    response = table.scan()
    while True:
        items.extend(response.get("Items", []))
        key = response.get("LastEvaluatedKey")
        if not key:
            return items
        response = table.scan(ExclusiveStartKey=key)


def _alert_view(item: dict) -> dict:
    return {
        "alert_id": str(item.get("alert_id", "")),
        "subscriber_id": str(item.get("subscriber_id", "")),
        "rule_id": str(item.get("rule_id", "")),
        "score": float(item["score"]) if item.get("score") is not None else None,
        "severity": str(item.get("severity", "UNKNOWN")),
        "event_timestamp": str(item.get("event_timestamp", "")),
        "created_at": str(item.get("created_at", "")),
        "status": str(item.get("status", "UNKNOWN")),
    }


def _latest_quality(s3) -> dict:
    token = None
    candidates = []
    while True:
        request = {"Bucket": BUCKET, "Prefix": QUALITY_PREFIX}
        if token:
            request["ContinuationToken"] = token
        response = s3.list_objects_v2(**request)
        candidates.extend(
            item for item in response.get("Contents", [])
            if item["Key"].rsplit("/", 1)[-1].startswith("gate_")
            and item["Key"].endswith(".json")
        )
        token = response.get("NextContinuationToken")
        if not token:
            break
    if not candidates:
        raise FileNotFoundError(f"No gate_*.json files found under s3://{BUCKET}/{QUALITY_PREFIX}")
    latest = max(candidates, key=lambda item: item["LastModified"])
    response = s3.get_object(Bucket=BUCKET, Key=latest["Key"])
    source = json.loads(response["Body"].read())
    return {**source, "generated_at": datetime.now(timezone.utc).isoformat()}


def export(output_dir: Path = Path("dashboard"), dynamodb=None, s3=None) -> tuple[Path, Path]:
    if dynamodb is None or s3 is None:
        default_dynamodb, default_s3 = _clients()
        dynamodb = dynamodb or default_dynamodb
        s3 = s3 or default_s3

    raw_alerts = _scan_alerts(dynamodb.Table("fraud_alerts"))
    alerts = [_alert_view(item) for item in raw_alerts]
    now = datetime.now(timezone.utc)
    counts_by_rule: dict[str, int] = {}
    counts_by_severity: dict[str, int] = {}
    counts_by_status: dict[str, int] = {}
    counts_by_date: dict[str, int] = {}
    cutoff = (now.date() - timedelta(days=29)).isoformat()
    for alert in alerts:
        for mapping, key in (
            (counts_by_rule, alert["rule_id"]),
            (counts_by_severity, alert["severity"]),
            (counts_by_status, alert["status"]),
        ):
            mapping[key] = mapping.get(key, 0) + 1
        day = alert["event_timestamp"][:10]
        if day and cutoff <= day <= now.date().isoformat():
            counts_by_date[day] = counts_by_date.get(day, 0) + 1

    score_sorted = sorted(alerts, key=lambda row: row["score"] if row["score"] is not None else float("-inf"), reverse=True)
    recent_sorted = sorted(alerts, key=lambda row: row["created_at"], reverse=True)
    alert_payload = {
        "generated_at": now.isoformat(),
        "total_alerts": len(alerts),
        "model_alerts_today": sum(
            row["rule_id"] == "MODEL_GBM_V1" and row["event_timestamp"][:10] == now.date().isoformat()
            for row in alerts
        ),
        "by_rule_id": counts_by_rule,
        "by_severity": counts_by_severity,
        "by_status": counts_by_status,
        "by_event_date": dict(sorted(counts_by_date.items())),
        "top_10_by_score": score_sorted[:10],
        "recent_alerts": recent_sorted[:50],
    }

    try:
        quality_payload = _latest_quality(s3)
    except Exception as exc:
        print(f"WARNING: quality metrics unavailable: {type(exc).__name__}: {exc}")
        quality_payload = {
            "generated_at": now.isoformat(),
            "sources": [],
            "overall_quarantine_rate": None,
        }

    output_dir.mkdir(parents=True, exist_ok=True)
    alerts_path = output_dir / "alerts.json"
    quality_path = output_dir / "quality.json"
    alerts_path.write_text(json.dumps(alert_payload, indent=2, cls=_DecimalSafeEncoder) + "\n", encoding="utf-8")
    quality_path.write_text(json.dumps(quality_payload, indent=2, cls=_DecimalSafeEncoder) + "\n", encoding="utf-8")
    print(f"Wrote {alerts_path} ({alerts_path.stat().st_size} bytes); keys={list(alert_payload)}")
    print(f"Wrote {quality_path} ({quality_path.stat().st_size} bytes); keys={list(quality_payload)}")
    return alerts_path, quality_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export LocalStack alert and quality state as dashboard JSON.")
    parser.add_argument("--output-dir", default="dashboard")
    args = parser.parse_args()
    export(Path(args.output_dir))


if __name__ == "__main__":
    main()
