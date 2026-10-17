"""Score one published model-feature day and optionally persist/notify alerts."""

from __future__ import annotations

import argparse
import io
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import joblib
import numpy as np
import polars as pl


class _DecimalSafeEncoder(json.JSONEncoder):
    """Serializes Decimal to float so json.dumps works on DynamoDB-sourced evidence dicts."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


BUCKET = "telco-fraud-bronze"
MODEL_PREFIX = "models/fraud_gbm/v1/"
FEATURE_PREFIX = "gold/model_features/"
ALERT_TABLE = "fraud_alerts"
SNS_TOPIC_NAME = "fraud-alerts-topic"
MODEL_RULE_ID = "MODEL_GBM_V1"
_MODEL_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


def _as_date_string(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def score_day(
    features_df: pl.DataFrame,
    model,
    feature_columns: list[str],
    threshold: float,
    rule_hits_df: pl.DataFrame,
    config: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Pure one-day scoring core; it performs no reads, writes, or AWS calls."""
    config = config or {}
    missing = [column for column in feature_columns if column not in features_df.columns]
    if missing:
        raise ValueError(f"Missing model feature columns: {missing}")
    required = {"subscriber_id", "date"}
    if not required.issubset(features_df.columns):
        raise ValueError(f"Features must include {sorted(required)}")
    if features_df.is_empty():
        return pl.DataFrame()

    x = features_df.select(feature_columns).to_numpy()
    scores = model.predict_proba(x)[:, 1]
    scored = features_df.select("subscriber_id", "date").with_columns(pl.Series("score", scores))
    # Gold daily data is expected to be one row per account; keep a single
    # deterministic record if an upstream retry left duplicate rows.
    scored = scored.sort(["subscriber_id", "score"], descending=[False, True]).unique(
        subset=["subscriber_id"], keep="first", maintain_order=True
    )
    max_alerts = max(0, int(config.get("max_model_alerts_per_day", 100)))
    thresholded = scored.filter(pl.col("score") >= float(threshold)).sort(
        ["score", "subscriber_id"], descending=[True, False]
    ).head(max_alerts)

    hits: dict[str, set[str]] = {}
    if {"subscriber_id", "rule_id"}.issubset(rule_hits_df.columns):
        for row in rule_hits_df.select("subscriber_id", "rule_id").iter_rows(named=True):
            sid, rule_id = str(row["subscriber_id"]), str(row["rule_id"])
            if rule_id != MODEL_RULE_ID:
                hits.setdefault(sid, set()).add(rule_id)

    now = datetime.now(timezone.utc).isoformat()
    model_version = str(config.get("model_version", "v1"))
    alerts = []
    for row in thresholded.iter_rows(named=True):
        sid = str(row["subscriber_id"])
        day = _as_date_string(row["date"])
        score = float(row["score"])
        corroborating_rules = sorted(hits.get(sid, set()))
        alerts.append({
            "alert_id": f"{sid}#{day}#{MODEL_RULE_ID}",
            "subscriber_id": sid,
            "rule_id": MODEL_RULE_ID,
            "fraud_type": "UNSPECIFIED",
            "score": score,
            "severity": "HIGH" if corroborating_rules else "MEDIUM",
            "status": "OPEN",
            "event_timestamp": day,
            "created_at": now,
            "evidence": {
                "score": score,
                "threshold": float(threshold),
                "model_version": model_version,
                "corroborating_rules": corroborating_rules,
            },
        })
    return pl.DataFrame(alerts, strict=False) if alerts else pl.DataFrame()


def build_sns_summary(alerts_df: pl.DataFrame, day: str, model_version: str) -> str | None:
    """Build the single SNS JSON message for one run, or None when no alerts exist."""
    if alerts_df.is_empty():
        return None
    records = alerts_df.sort(["score", "subscriber_id"], descending=[True, False]).head(10)
    corroborated = sum(bool(row.get("evidence", {}).get("corroborating_rules", [])) for row in alerts_df.to_dicts())
    payload = {
        "date": day,
        "model_version": model_version,
        "model_alerts": alerts_df.height,
        "corroborated": corroborated,
        "top_accounts": [
            {"subscriber_id": row["subscriber_id"], "score": float(row["score"])}
            for row in records.iter_rows(named=True)
        ],
    }
    return json.dumps(payload, sort_keys=True)


def _aws_common() -> dict[str, str]:
    return {
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }


def _s3_client():
    return boto3.client("s3", **_aws_common())


def load_model(bucket: str = BUCKET, prefix: str = MODEL_PREFIX) -> dict[str, Any]:
    cache_key = (bucket, prefix)
    if cache_key in _MODEL_CACHE:
        return _MODEL_CACHE[cache_key]
    s3 = _s3_client()
    normalized = prefix.rstrip("/") + "/"
    model = joblib.load(io.BytesIO(s3.get_object(Bucket=bucket, Key=normalized + "model.joblib")["Body"].read()))
    published_features = json.loads(s3.get_object(Bucket=bucket, Key=normalized + "feature_columns.json")["Body"].read())
    metrics = json.loads(s3.get_object(Bucket=bucket, Key=normalized + "model_metrics.json")["Body"].read())
    # The model bundle/metrics identify the exact fitted inputs. The table's
    # original feature list can also include a constant feature dropped by the
    # trainer; require the published list to contain every fitted input.
    bundled_features = model.get("feature_columns") if isinstance(model, dict) else None
    metric_features = metrics.get("feature_columns")
    feature_columns = bundled_features or metric_features or published_features
    if not set(feature_columns).issubset(set(published_features)):
        raise ValueError("Fitted model features are absent from feature_columns.json")
    if bundled_features and metric_features and bundled_features != metric_features:
        raise ValueError("Model bundle and model_metrics.json feature lists disagree")
    loaded = {
        "model": model["model"] if isinstance(model, dict) and "model" in model else model,
        "feature_columns": feature_columns,
        "threshold": float(metrics["alert_threshold"]),
        "model_version": str(metrics.get("model_version", "v1")),
    }
    _MODEL_CACHE[cache_key] = loaded
    return loaded


def _feature_key(day: str) -> str:
    year, month, dom = day.split("-")
    return f"{FEATURE_PREFIX}year={year}/month={month}/day={dom}/features.parquet"


def load_features(day: str, bucket: str = BUCKET) -> pl.DataFrame:
    body = _s3_client().get_object(Bucket=bucket, Key=_feature_key(day))["Body"].read()
    return pl.read_parquet(io.BytesIO(body))


def latest_feature_date(bucket: str = BUCKET) -> str:
    s3 = _s3_client()
    token = None
    dates: list[str] = []
    while True:
        request = {"Bucket": bucket, "Prefix": FEATURE_PREFIX}
        if token:
            request["ContinuationToken"] = token
        response = s3.list_objects_v2(**request)
        for item in response.get("Contents", []):
            key = item["Key"]
            try:
                year = key.split("year=", 1)[1][:4]
                month = key.split("month=", 1)[1][:2]
                dom = key.split("day=", 1)[1][:2]
                dates.append(date.fromisoformat(f"{year}-{month}-{dom}").isoformat())
            except (IndexError, ValueError):
                continue
        token = response.get("NextContinuationToken")
        if not token:
            break
    if not dates:
        raise ValueError(f"No daily model feature partitions found under s3://{bucket}/{FEATURE_PREFIX}")
    return max(dates)


def load_rule_hits(day: str) -> pl.DataFrame:
    table = boto3.resource("dynamodb", **_aws_common()).Table(ALERT_TABLE)
    items: list[dict] = []
    response = table.scan()
    while True:
        items.extend(response.get("Items", []))
        key = response.get("LastEvaluatedKey")
        if not key:
            break
        response = table.scan(ExclusiveStartKey=key)
    filtered = [
        {"subscriber_id": str(item.get("subscriber_id", "")), "rule_id": str(item.get("rule_id", ""))}
        for item in items
        if str(item.get("event_timestamp", "")).startswith(day)
        and str(item.get("rule_id", "")) != MODEL_RULE_ID
    ]
    return pl.DataFrame(filtered) if filtered else pl.DataFrame(schema={"subscriber_id": pl.String, "rule_id": pl.String})


def write_alerts(alerts_df: pl.DataFrame) -> int:
    if alerts_df.is_empty():
        return 0
    table = boto3.resource("dynamodb", **_aws_common()).Table(ALERT_TABLE)
    for alert in alerts_df.to_dicts():
        item = dict(alert)
        item["score"] = Decimal(str(item["score"]))
        evidence = dict(item["evidence"])
        evidence["score"] = Decimal(str(evidence["score"]))
        evidence["threshold"] = Decimal(str(evidence["threshold"]))
        item["evidence"] = json.dumps(
            evidence, sort_keys=True, cls=_DecimalSafeEncoder
        )
        table.put_item(Item=item)
    return alerts_df.height


def publish_summary(message: str | None, day: str) -> bool:
    if message is None:
        return False
    sns = boto3.client("sns", **_aws_common())
    token = None
    topic_arn = None
    while True:
        request = {} if token is None else {"NextToken": token}
        response = sns.list_topics(**request)
        for topic in response.get("Topics", []):
            arn = topic["TopicArn"]
            if arn.rsplit(":", 1)[-1] == SNS_TOPIC_NAME:
                topic_arn = arn
                break
        if topic_arn:
            break
        token = response.get("NextToken")
        if not token:
            break
    if topic_arn is None:
        raise ValueError(f"SNS topic {SNS_TOPIC_NAME!r} was not found")
    sns.publish(TopicArn=topic_arn, Subject=f"Fraud model alerts - {day}", Message=message)
    return True


def run(day: str | None = None, dry_run: bool = False) -> dict[str, Any]:
    day = day or latest_feature_date()
    loaded = load_model()
    features = load_features(day)
    hits = load_rule_hits(day)
    alerts = score_day(
        features, loaded["model"], loaded["feature_columns"], loaded["threshold"], hits,
        {"model_version": loaded["model_version"], "max_model_alerts_per_day": 100},
    )
    message = build_sns_summary(alerts, day, loaded["model_version"])
    if dry_run:
        print(alerts)
        print(message if message is not None else "No SNS message: no model alerts.")
    else:
        write_alerts(alerts)
        publish_summary(message, day)
    return {
        "date": day,
        "model_version": loaded["model_version"],
        "model_alerts": alerts.height,
        "corroborated": sum(
            bool(row.get("evidence", {}).get("corroborating_rules", [])) for row in alerts.to_dicts()
        ),
        "dry_run": dry_run,
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return run(event.get("date"), bool(event.get("dry_run", False)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Score one day of published model features.")
    parser.add_argument("--date", help="Day to score (YYYY-MM-DD); defaults to latest available day")
    parser.add_argument("--dry-run", action="store_true", help="Print alerts and summary without writes")
    args = parser.parse_args()
    print(json.dumps(run(args.date, args.dry_run), sort_keys=True))


if __name__ == "__main__":
    main()
