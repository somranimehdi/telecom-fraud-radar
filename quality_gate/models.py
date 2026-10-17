"""
Quality Gate — Internal Models
--------------------------------
Data structures used *inside* the quality gate. These are not the Pydantic
contracts for raw data — those live in `contracts/`. These define what the
gate's outputs look like:

  - ValidationResult: passed/failed + reason for one row
  - SourceReport: aggregate stats for one source's run
  - GateReport: full run across all sources
  - QuarantineRow: the shape of a quarantined row (data + reason + metadata)

Why separate from contracts?
  - Contracts describe incoming data.
  - These describe the gate's internal bookkeeping.
  - Mixing them would couple validation logic to contracts in fragile ways.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


# ─── Per-Row Result ─────────────────────────────────────────────────────────

CheckStatus = Literal["passed", "failed"]


@dataclass
class ValidationResult:
    """
    The outcome of validating a single row.
    Every row produced by the gate has one of these.
    """
    status: CheckStatus
    reason: str = ""              # empty when passed; human-readable when failed
    check_name: str = ""          # which check failed ("schema", "duplicate", "range", "referential")
    row: dict[str, Any] = field(default_factory=dict)   # the original row

    @property
    def passed(self) -> bool:
        return self.status == "passed"


# ─── Per-Source Report ──────────────────────────────────────────────────────

@dataclass
class QuarantineRow:
    """
    A row that failed validation. Wrapped with metadata so downstream tools
    (Athena, dashboards) can filter and audit without re-parsing.
    """
    row: dict[str, Any]
    reason: str
    check_name: str
    source: str                       # e.g., "voice", "activation"
    quarantined_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


@dataclass
class SourceReport:
    """
    Summary statistics for one source's validation run.
    Produced by the runner, consumed by metrics + reports.
    """
    source: str
    total_rows: int = 0
    passed_rows: int = 0
    failed_rows: int = 0

    # Breakdown of failures by check name (schema, duplicate, range, ...)
    failures_by_check: dict[str, int] = field(default_factory=dict)

    # Breakdown of failures by reason (free-form string)
    failures_by_reason: dict[str, int] = field(default_factory=dict)

    # Timing
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    finished_at: str = ""
    # Freshness — filled in by the gate (not by validate_source)
    freshness_ok: bool = True
    freshness_days_behind: int | None = None
    future_timestamp_rows: int = 0
    null_timestamp_rows: int = 0
    # S3 locations of outputs
    silver_prefix: str = ""
    quarantine_prefix: str = ""

    @property
    def quarantine_rate(self) -> float:
        """Fraction of rows quarantined. 0.0–1.0."""
        if self.total_rows == 0:
            return 0.0
        return self.failed_rows / self.total_rows

    def record_failure(self, check_name: str, reason: str) -> None:
        """Increment failure counters for one failed row."""
        self.failed_rows += 1
        self.failures_by_check[check_name] = self.failures_by_check.get(check_name, 0) + 1
        # Truncate reason to first 120 chars so metrics don't balloon
        key = reason[:120]
        self.failures_by_reason[key] = self.failures_by_reason.get(key, 0) + 1

    def record_pass(self) -> None:
        self.passed_rows += 1

    def finish(self) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        """Serialize for writing to S3 as JSON metrics."""
        return {
            "source": self.source,
            "total_rows": self.total_rows,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "quarantine_rate": round(self.quarantine_rate, 6),
            "failures_by_check": dict(self.failures_by_check),
            "failures_by_reason": dict(self.failures_by_reason),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "silver_prefix": self.silver_prefix,
            "quarantine_prefix": self.quarantine_prefix,
             "freshness_ok": self.freshness_ok,
            "freshness_days_behind": self.freshness_days_behind,
            "future_timestamp_rows": self.future_timestamp_rows,
            "null_timestamp_rows": self.null_timestamp_rows,

        }


# ─── Full-Run Report ────────────────────────────────────────────────────────

@dataclass
class GateReport:
    """
    Aggregated report across all sources processed in one gate run.
    """
    run_id: str                              # e.g., "20261016T120000Z" or a UUID
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    finished_at: str = ""
    source_reports: list[SourceReport] = field(default_factory=list)

    # Alert thresholds — see config or constants
    quarantine_threshold: float = 0.05       # 5% default — over this triggers SNS

    def add_source_report(self, report: SourceReport) -> None:
        self.source_reports.append(report)

    def finish(self) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()

    @property
    def total_rows(self) -> int:
        return sum(r.total_rows for r in self.source_reports)

    @property
    def total_passed(self) -> int:
        return sum(r.passed_rows for r in self.source_reports)

    @property
    def total_failed(self) -> int:
        return sum(r.failed_rows for r in self.source_reports)

    @property
    def overall_quarantine_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return self.total_failed / self.total_rows

    def sources_over_threshold(self) -> list[str]:
        """Sources whose quarantine rate exceeds the alert threshold."""
        return [
            r.source for r in self.source_reports
            if r.quarantine_rate > self.quarantine_threshold
        ]

    def to_dict(self) -> dict[str, Any]:
        """Serialize for metrics/logging."""
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total_rows": self.total_rows,
            "total_passed": self.total_passed,
            "total_failed": self.total_failed,
            "overall_quarantine_rate": round(self.overall_quarantine_rate, 6),
            "quarantine_threshold": self.quarantine_threshold,
            "sources_over_threshold": self.sources_over_threshold(),
            "sources": [r.to_dict() for r in self.source_reports],
        }
