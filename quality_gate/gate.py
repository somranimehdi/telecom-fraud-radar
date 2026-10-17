"""
Quality Gate — All-Source Orchestrator
----------------------------------------
Wires validate_source() + emit_metrics_and_alert() into a single entry
point that processes every source in one run.

Design:
  - Sequential. No parallelism needed at this scale.
  - Fail-safe per source: one source's failure doesn't block others.
  - Freshness check runs per source (warns if data is stale).
  - Returns a GateReport with per-source details.
  - Emits metrics + alert at the end (once, for the whole run).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from quality_gate.io import (
    load_all_bundle_ids,
    load_all_subscriber_ids,
    read_source,
)
from quality_gate.metrics import emit_metrics_and_alert
from quality_gate.models import GateReport
from quality_gate.runner import EmptySourceError, SOURCE_PATHS, validate_source


DEFAULT_SOURCES: list[str] = ["voice", "data", "revenue", "activation", "purchases"]


# ─── Freshness ──────────────────────────────────────────────────────────────

def _check_source_freshness(
    s3,
    bucket: str,
    raw_prefix: str,
    reference_date: date | datetime | None = None,
    max_days_behind: int = 2,
) -> tuple[bool, int | None]:
    """Compare the newest row timestamp date with the reference date."""
    if reference_date is None:
        reference_date = datetime.now(timezone.utc).date()
    elif isinstance(reference_date, datetime):
        reference_date = reference_date.date()

    frame, file_count = read_source(s3, bucket, raw_prefix, file_type="auto")
    if not file_count or frame.is_empty() or "timestamp" not in frame.columns:
        return (False, None)
    event_dates: list[date] = []
    for value in frame.get_column("timestamp").to_list():
        if isinstance(value, datetime):
            event_dates.append(value.date())
        elif isinstance(value, date):
            event_dates.append(value)
        elif value is not None:
            try:
                event_dates.append(datetime.fromisoformat(
                    str(value).replace("Z", "+00:00")
                ).date())
            except ValueError:
                continue
    if not event_dates:
        return (False, None)
    days_behind = (reference_date - max(event_dates)).days
    return (days_behind <= max_days_behind, days_behind)


# ─── Reference Data Loading ─────────────────────────────────────────────────

def _load_reference_data(
    dynamodb,
    cfg: dict,
    verbose: bool = True,
) -> dict:
    """
    Load reference tables once per run for referential checks.
    Returns a dict with `bundle_ids` and `subscriber_ids` sets.
    """
    reference = {"bundle_ids": set(), "subscriber_ids": set()}
    if dynamodb is None:
        if verbose:
            print("  ⚠️  No DynamoDB client — referential checks disabled")
        return reference

    try:
        reference["bundle_ids"] = load_all_bundle_ids(
            dynamodb, cfg["paths"]["dynamodb_bundles"]
        )
        reference["subscriber_ids"] = load_all_subscriber_ids(
            dynamodb, cfg["paths"]["dynamodb_subscribers"]
        )
        if verbose:
            print(
                f"  Loaded {len(reference['bundle_ids'])} bundle_ids, "
                f"{len(reference['subscriber_ids'])} subscriber_ids from reference tables"
            )
    except Exception as exc:
        if verbose:
            print(f"  ⚠️  Failed to load reference data: {type(exc).__name__}: {exc}")
            print(f"      Referential checks will be skipped.")

    return reference


# ─── Public API ─────────────────────────────────────────────────────────────

def run_gate(
    s3,
    sns,
    cfg: dict,
    dynamodb=None,
    sources: list[str] | None = None,
    verbose: bool = True,
    allow_empty: bool = False,
    as_of: date | None = None,
) -> GateReport:
    """
    Run the quality gate on all (or specified) sources.

    Args:
      s3:         boto3 S3 client
      sns:        boto3 SNS client
      cfg:        parsed config.yaml dict
      dynamodb:   boto3 DynamoDB resource (for referential checks)
      sources:    optional list of sources; defaults to DEFAULT_SOURCES
      verbose:    if True, prints per-source progress

    Returns:
      GateReport — the full run summary (also written to S3 as metrics).
    """
    sources = sources or DEFAULT_SOURCES
    bucket = cfg["paths"]["s3_bucket"]
    metrics_prefix = cfg.get("quality_gate", {}).get("metrics_prefix", "quality_metrics")
    quarantine_threshold = cfg.get("quality_gate", {}).get("quarantine_threshold", 0.05)
    freshness_max_days = cfg.get("quality_gate", {}).get("freshness_max_days_behind", 2)
    topic_arn = f"arn:aws:sns:{cfg.get('region', 'us-east-1')}:000000000000:fraud-alerts-topic"

    run_id = "gate_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = GateReport(run_id=run_id, quarantine_threshold=quarantine_threshold)

    # ── Banner ──────────────────────────────────────────────────────────────
    if verbose:
        print("═" * 65)
        print(f"  Quality Gate — Run {run_id}")
        print(f"  Sources: {', '.join(sources)}")
        print(f"  Bucket : {bucket}")
        print(f"  Threshold: {quarantine_threshold:.2%}")
        print(f"  Freshness max days behind: {freshness_max_days}")
        print(f"  Freshness as of: {as_of or datetime.now(timezone.utc).date()}")
        print("═" * 65)

    # ── Load reference data ─────────────────────────────────────────────────
    reference = _load_reference_data(dynamodb, cfg, verbose=verbose)

    # ── Filter out unknown sources ──────────────────────────────────────────
    valid_sources = [s for s in sources if s in SOURCE_PATHS]
    for s in sources:
        if s not in SOURCE_PATHS:
            print(f"  ⚠️  Skipping unknown source: {s}")

    # ── Process each source ─────────────────────────────────────────────────
    for src in valid_sources:
        try:
            src_report = validate_source(
                s3=s3,
                bucket=bucket,
                source=src,
                run_id=run_id,
                reference=reference,
                verbose=verbose,
                allow_empty=allow_empty,
            )

            # Freshness check
            paths = SOURCE_PATHS[src]
            fresh_ok, days_behind = _check_source_freshness(
                s3, bucket, paths.raw_prefix, reference_date=as_of,
                max_days_behind=freshness_max_days,
            )
            src_report.freshness_ok = fresh_ok
            src_report.freshness_days_behind = days_behind

            if verbose and not src_report.freshness_ok:
                behind_str = f"{days_behind} days behind" if days_behind is not None else "no data"
                print(
                    f"  ⚠️  [{src}] STALE — {behind_str} "
                    f"(max: {freshness_max_days})"
                )

            report.add_source_report(src_report)
        except EmptySourceError:
            raise
        except Exception as exc:
            if verbose:
                print(f"  ❌ [{src}] failed: {type(exc).__name__}: {exc}")
            # Continue with other sources — partial success is OK

    report.finish()

    # ── Emit metrics + alert ────────────────────────────────────────────────
    if verbose:
        print()
        print("─" * 65)
        print("  Emitting metrics")
        print("─" * 65)

    try:
        emit_metrics_and_alert(
            s3=s3,
            sns=sns,
            bucket=bucket,
            metrics_prefix=metrics_prefix,
            topic_arn=topic_arn,
            report=report,
            verbose=verbose,
        )
    except Exception as exc:
        if verbose:
            print(f"  ⚠️  metrics/alert failed: {type(exc).__name__}: {exc}")

    # ── Final summary ───────────────────────────────────────────────────────
    if verbose:
        print()
        print("═" * 65)
        print("  Gate Summary")
        print("═" * 65)
        print(f"  Total rows  : {report.total_rows:,}")
        print(f"  Passed      : {report.total_passed:,}")
        print(f"  Quarantined : {report.total_failed:,}")
        print(f"  Rate        : {report.overall_quarantine_rate:.4%}")

        over = report.sources_over_threshold()
        stale = [r.source for r in report.source_reports if not r.freshness_ok]

        if over:
            print(f"  ⚠️  Over quarantine threshold: {over}")
        else:
            print(f"  ✅ All sources within quarantine threshold")

        if stale:
            print(f"  ⚠️  Stale sources: {stale}")
        else:
            print(f"  ✅ All sources fresh")

        print("═" * 65)

    return report
