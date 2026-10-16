"""
Quality Gate — Metrics & Alerting
-----------------------------------
Two responsibilities:

  1. Write the GateReport as JSON to S3 (`quality_metrics/`).
     This is the persistent record of every gate run.

  2. Publish an SNS alert if any source exceeds the quarantine threshold.

Design:
  - Metrics are always written (success or failure of alerting).
  - SNS alerts are best-effort: a failure to publish is logged, not fatal.
  - Alert threshold comes from config; default 5%.
  - Alert payload includes the full gate report — enough context for the
    on-call engineer to know what happened without opening the dashboard.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from quality_gate.io import write_metrics_to_s3
from quality_gate.models import GateReport


# ─── Metrics Persistence ────────────────────────────────────────────────────

def write_gate_metrics(
    s3,
    bucket: str,
    metrics_prefix: str,
    report: GateReport,
    timestamp: datetime | None = None,
) -> str:
    """
    Write the GateReport to S3 as JSON.
    Returns the S3 key written.
    """
    ts = timestamp or datetime.now(timezone.utc)
    payload = report.to_dict()
    key = write_metrics_to_s3(
        s3, bucket, metrics_prefix, payload, run_id=report.run_id, timestamp=ts,
    )
    return key


# ─── SNS Alerting ───────────────────────────────────────────────────────────

def publish_quality_alert(
    sns,
    topic_arn: str,
    report: GateReport,
    verbose: bool = True,
) -> bool:
    """
    Publish a quality alert to SNS if any source is over threshold.
    Returns True if an alert was published, False otherwise.
    """
    over = report.sources_over_threshold()
    if not over:
        if verbose:
            print(f"  [metrics] no source over threshold ({report.quarantine_threshold:.2%})")
        return False

    subject = f"[QUALITY ALERT] {len(over)} source(s) over threshold"
    message = {
        "alert_type": "quality_gate_threshold_exceeded",
        "run_id": report.run_id,
        "finished_at": report.finished_at,
        "overall_quarantine_rate": round(report.overall_quarantine_rate, 6),
        "threshold": report.quarantine_threshold,
        "sources_over_threshold": over,
        "sources": [
            {
                "source": r.source,
                "total_rows": r.total_rows,
                "failed_rows": r.failed_rows,
                "quarantine_rate": round(r.quarantine_rate, 6),
                "failures_by_check": r.failures_by_check,
            }
            for r in report.source_reports
        ],
    }

    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],   # SNS subject limit is 100 chars
            Message=json.dumps(message, indent=2, default=str),
        )
        if verbose:
            print(f"  [metrics] ✅ SNS alert published: {subject}")
        return True
    except Exception as exc:
        if verbose:
            print(f"  [metrics] ⚠️  SNS alert failed: {type(exc).__name__}: {exc}")
        return False


# ─── Combined Emit ──────────────────────────────────────────────────────────

def emit_metrics_and_alert(
    s3,
    sns,
    bucket: str,
    metrics_prefix: str,
    topic_arn: str,
    report: GateReport,
    timestamp: datetime | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    """
    Write metrics + publish alert in one call. Returns a summary dict.
    """
    ts = timestamp or datetime.now(timezone.utc)

    metrics_key = write_gate_metrics(
        s3, bucket, metrics_prefix, report, timestamp=ts,
    )
    if verbose:
        print(f"  [metrics] wrote → s3://{bucket}/{metrics_key}")

    alert_published = publish_quality_alert(
        sns, topic_arn, report, verbose=verbose,
    )

    return {
        "metrics_key": metrics_key,
        "alert_published": alert_published,
        "sources_over_threshold": report.sources_over_threshold(),
    }