"""
Quality Gate — Runner
-----------------------
Orchestrates validation for one source:

  1. Read raw data from S3 (Parquet or JSONL).
  2. For each row:
       a. check_schema
       b. check_duplicate (only if schema passed)
       c. check_range (only if schema and duplicate passed)
  3. Collect passed rows and quarantined rows.
  4. Write silver + quarantine to S3.
  5. Return a SourceReport.

Design:
  - Sequential checks: fail-fast. A bad schema row doesn't get checked for
    duplicates (its ID may be missing anyway).
  - Reason is preserved verbatim from the failing check.
  - Per-source config drives file type, prefixes, contract.
  - Silent on empty sources (returns an empty report).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import polars as pl

from quality_gate.checks import (
    SOURCE_REGISTRY,
    CheckContext,
    check_schema,
    check_duplicate,
    check_range,
)
from quality_gate.io import (
    read_source,
    write_parquet_to_s3,
    write_quarantine_to_s3,
)
from quality_gate.models import QuarantineRow, SourceReport


# ─── Source Configuration ───────────────────────────────────────────────────
# Where each source lives in S3, and how its files are formatted.
# Silver and quarantine mirror the raw prefixes with a different root.

@dataclass(frozen=True)
class SourcePaths:
    raw_prefix: str
    silver_prefix: str
    quarantine_prefix: str
    file_type: str            # "parquet" or "jsonl"


SOURCE_PATHS: dict[str, SourcePaths] = {
    "activation": SourcePaths(
        raw_prefix="raw/activation/",
        silver_prefix="silver/activation/",
        quarantine_prefix="quarantine/activation/",
        file_type="jsonl",
    ),
    "purchases": SourcePaths(
        raw_prefix="raw/bundles/purchases/",
        silver_prefix="silver/bundles/purchases/",
        quarantine_prefix="quarantine/bundles/purchases/",
        file_type="jsonl",
    ),
    "voice": SourcePaths(
        raw_prefix="raw/usage/voice/",
        silver_prefix="silver/usage/voice/",
        quarantine_prefix="quarantine/usage/voice/",
        file_type="parquet",
    ),
    "data": SourcePaths(
        raw_prefix="raw/usage/data/",
        silver_prefix="silver/usage/data/",
        quarantine_prefix="quarantine/usage/data/",
        file_type="parquet",
    ),
    "revenue": SourcePaths(
        raw_prefix="raw/revenue/prepaid/",
        silver_prefix="silver/revenue/prepaid/",
        quarantine_prefix="quarantine/revenue/prepaid/",
        file_type="parquet",
    ),
}


# ─── Row-Level Orchestration ────────────────────────────────────────────────

def _validate_row(row: dict, context: CheckContext) -> tuple[str, str, str]:
    """
    Run the three checks in sequence. Returns (status, reason, check_name).

    status: "passed" | "failed"
    reason: "" on pass, non-empty on fail
    check_name: "" on pass, name of failing check on fail
    """
    # 1. Schema
    r = check_schema(row, context)
    if not r.passed:
        return ("failed", r.reason, r.check_name)

    # 2. Duplicate (mutates context.seen_ids)
    r = check_duplicate(row, context)
    if not r.passed:
        return ("failed", r.reason, r.check_name)

    # 3. Range
    r = check_range(row, context)
    if not r.passed:
        return ("failed", r.reason, r.check_name)

    return ("passed", "", "")


# ─── Per-Source Runner ──────────────────────────────────────────────────────

def validate_source(
    s3,
    bucket: str,
    source: str,
    run_id: str,
    timestamp: datetime | None = None,
    verbose: bool = True,
) -> SourceReport:
    """
    Validate one source end-to-end. Returns a SourceReport.

    Steps:
      1. Read all raw files for this source.
      2. Validate row-by-row.
      3. Write silver + quarantine.
      4. Return report.
    """
    if source not in SOURCE_PATHS:
        raise ValueError(f"unknown source: {source}")
    if source not in SOURCE_REGISTRY:
        raise ValueError(f"source {source} not in registry")

    paths = SOURCE_PATHS[source]
    report = SourceReport(source=source)

    # 1. Read raw
    df, file_count = read_source(
        s3, bucket, paths.raw_prefix, file_type=paths.file_type
    )
    report.total_rows = df.height
    report.silver_prefix = paths.silver_prefix
    report.quarantine_prefix = paths.quarantine_prefix

    if verbose:
        print(f"  [{source}] read {file_count} file(s), {df.height:,} rows")

    if df.is_empty():
        report.finish()
        return report

    # 2. Validate each row
    context = CheckContext(source=source)
    passed_rows: list[dict] = []
    quarantine_rows: list[dict] = []

    for raw_row in df.iter_rows(named=True):
        # Convert Polars types (Decimal, datetime) to native Python for Pydantic
        row = {k: _to_python(v) for k, v in raw_row.items()}

        status, reason, check_name = _validate_row(row, context)
        if status == "passed":
            passed_rows.append(row)
            report.record_pass()
        else:
            quarantine_rows.append(asdict(QuarantineRow(
                row=row,
                reason=reason,
                check_name=check_name,
                source=source,
            )))
            report.record_failure(check_name, reason)

    # 3. Write outputs
    ts = timestamp or datetime.now(timezone.utc)

    if passed_rows:
        silver_df = pl.DataFrame(passed_rows, infer_schema_length=None)
        silver_key = write_parquet_to_s3(
            s3, bucket, paths.silver_prefix, silver_df,
            run_id=run_id, filename="data.parquet", timestamp=ts,
        )
        if verbose:
            print(f"  [{source}] silver   → {silver_key} ({len(passed_rows):,} rows)")

    if quarantine_rows:
        quarantine_key = write_quarantine_to_s3(
            s3, bucket, paths.quarantine_prefix, quarantine_rows,
            run_id=run_id, timestamp=ts,
        )
        if verbose:
            print(f"  [{source}] quarantine → {quarantine_key} ({len(quarantine_rows):,} rows)")

    # 4. Finalize report
    report.finish()
    if verbose:
        print(f"  [{source}] done — rate {report.quarantine_rate:.4%}")

    return report


# ─── Type Conversion ────────────────────────────────────────────────────────

def _to_python(value: Any) -> Any:
    """
    Convert Polars-native types to plain Python types so Pydantic can
    validate them.

    Handles:
      - Decimal → keep as Decimal (Pydantic accepts Decimal directly)
      - datetime → keep as datetime
      - Everything else passes through.
    """
    # Polars already returns Decimal and datetime as Python objects in
    # iter_rows(named=True). This function is a hook for future needs.
    return value