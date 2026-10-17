"""
Quality Gate — Metrics & Alerting
-----------------------------------
Two responsibilities:

  1. Write the GateReport as JSON to S3 (`quality_metrics/`).
     This is the persistent record of every gate run.

  2. Publish an SNS alert if any source is:
       a. Over the quarantine threshold, OR
       b. Stale (no recent data).

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

def _build_alert_payload(report: GateReport) -> dict:
    """
    Build the SNS message body from the GateReport.
    Includes both quarantine and freshness info per source.
    """
    over_quarantine = report.sources_over_threshold()
    stale_sources = [r.source for r in report.source_reports if not r.freshness_ok]

    return {
        "alert_type": "quality_gate",
        "run_id": report.run_id,
        "finished_at": report.finished_at,
        "overall_quarantine_rate": round(report.overall_quarantine_rate, 6),
        "quarantine_threshold": report.quarantine_threshold,
        "sources_over_quarantine_threshold": over_quarantine,
        "stale_sources": stale_sources,
        "sources": [
            {
                "source": r.source,
                "total_rows": r.total_rows,
                "failed_rows": r.failed_rows,
                "quarantine_rate": round(r.quarantine_rate, 6),
                "failures_by_check": r.failures_by_check,
                "freshness_ok": r.freshness_ok,
                "freshness_days_behind": r.freshness_days_behind,
                "future_timestamp_rows": r.future_timestamp_rows,
                "null_timestamp_rows": r.null_timestamp_rows,
            }
            for r in report.source_reports
        ],
    }


def publish_quality_alert(
    sns,
    topic_arn: str,
    report: GateReport,
    verbose: bool = True,
) -> bool:
    """
    Publish a quality alert to SNS if any source is over the quarantine
    threshold OR stale. Returns True if an alert was published.
    """
    over_quarantine = report.sources_over_threshold()
    stale_sources = [r.source for r in report.source_reports if not r.freshness_ok]

    if not over_quarantine and not stale_sources:
        if verbose:
            print(f"  [metrics] no source over threshold ({report.quarantine_threshold:.2%})")
            print(f"  [metrics] all sources fresh")
        return False

    # Build a human-readable subject summarizing what's wrong
    parts = []
    if over_quarantine:
        parts.append(f"{len(over_quarantine)} over threshold")
    if stale_sources:
        parts.append(f"{len(stale_sources)} stale")
    subject = f"[QUALITY ALERT] {', '.join(parts)}"

    message = _build_alert_payload(report)

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
        "stale_sources": [r.source for r in report.source_reports if not r.freshness_ok],
    }
