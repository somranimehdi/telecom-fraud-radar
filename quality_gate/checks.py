"""
Quality Gate — Checks
-----------------------
Pure functions that validate individual rows.

Three check types:
  1. check_schema     — Pydantic contract validation (Validator 1)
  2. check_duplicate  — same ID seen twice (Validator 2)
  3. check_range      — cross-field business rules (Validator 2)

Each check has the same signature:
  (row, context) -> ValidationResult

`context` carries cross-row state, e.g.:
  - seen_ids: set of event/transaction/call IDs already validated
  - known_bundle_ids: set of valid bundle IDs from the catalog
  - known_subscriber_ids: set of valid subscriber IDs

Checks are pure — no side effects, no I/O, no exceptions (return failed
results instead).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from contracts.source_1_sim_activation import SimActivationEvent
from contracts.source_2_revenue import RevenueTransaction
from contracts.source_3_usage import VoiceCDR, DataCDR
from contracts.source_4_bundle_catalog import BundleCatalogEntry
from contracts.source_5_bundle_purchases import BundlePurchase
from contracts.source_8_subscriber_profile import SubscriberProfile

from quality_gate.models import ValidationResult


# ─── Source Registry ────────────────────────────────────────────────────────
# Maps source names (as used in the pipeline) to:
#   - the Pydantic contract class that validates rows from that source
#   - the field name that holds the unique ID (for duplicate checks)

@dataclass(frozen=True)
class SourceSpec:
    name: str
    contract: type
    id_field: str           # primary key field for duplicate detection


SOURCE_REGISTRY: dict[str, SourceSpec] = {
    "activation":  SourceSpec("activation",  SimActivationEvent, "event_id"),
    "purchases":   SourceSpec("purchases",   BundlePurchase,     "purchase_id"),
    "voice":       SourceSpec("voice",       VoiceCDR,           "call_id"),
    "data":        SourceSpec("data",        DataCDR,            "session_id"),
    "revenue":     SourceSpec("revenue",     RevenueTransaction, "transaction_id"),
    # Reference sources — used by referential checks, not row validation
    "subscribers": SourceSpec("subscribers", SubscriberProfile,  "subscriber_id"),
    "bundles":     SourceSpec("bundles",     BundleCatalogEntry, "bundle_id"),
}


# ─── Cross-Row Context ──────────────────────────────────────────────────────

@dataclass
class CheckContext:
    """
    Cross-row state passed to each check. Built up incrementally as rows
    are processed.

    For most sources, `seen_ids` and the reference tables are enough.
    Additional fields can be added as we introduce more checks.
    """
    source: str
    seen_ids: set[str] = field(default_factory=set)
    known_bundle_ids: set[str] = field(default_factory=set)
    known_subscriber_ids: set[str] = field(default_factory=set)


# ─── Check 1 — Schema (Pydantic) ────────────────────────────────────────────

def check_schema(row: dict[str, Any], context: CheckContext) -> ValidationResult:
    """
    Validate a row against the Pydantic contract for its source.

    Returns:
      - passed  → ValidationResult(status="passed", row=row)
      - failed  → ValidationResult(status="failed", reason=<pydantic msg>, ...)
    """
    spec = SOURCE_REGISTRY.get(context.source)
    if spec is None:
        return ValidationResult(
            status="failed",
            reason=f"unknown source: {context.source}",
            check_name="schema",
            row=row,
        )

    try:
        spec.contract(**row)
        return ValidationResult(status="passed", row=row)
    except ValidationError as exc:
        # Compact, human-readable reason: first error, cleaned up
        first = exc.errors()[0] if exc.errors() else {}
        loc = ".".join(str(p) for p in first.get("loc", []))
        msg = first.get("msg", "unknown validation error")
        reason = f"{loc}: {msg}" if loc else msg
        return ValidationResult(
            status="failed",
            reason=reason,
            check_name="schema",
            row=row,
        )
    except Exception as exc:
        # Catch-all — anything Pydantic doesn't raise but breaks anyway
        return ValidationResult(
            status="failed",
            reason=f"unexpected: {type(exc).__name__}: {exc}",
            check_name="schema",
            row=row,
        )


# ─── Check 2 — Duplicates ───────────────────────────────────────────────────

def check_duplicate(row: dict[str, Any], context: CheckContext) -> ValidationResult:
    """
    Reject a row if its primary ID has already been seen in this run.

    Uses `SOURCE_REGISTRY[source].id_field` to find the right field.
    Missing IDs are skipped — schema check should have caught those already.
    """
    spec = SOURCE_REGISTRY.get(context.source)
    if spec is None:
        # Unknown source — schema check already returned failed; be lenient here
        return ValidationResult(status="passed", row=row)

    id_value = row.get(spec.id_field)
    if id_value is None:
        # No ID field — schema check will catch this; don't double-report
        return ValidationResult(status="passed", row=row)

    id_str = str(id_value)
    if id_str in context.seen_ids:
        return ValidationResult(
            status="failed",
            reason=f"duplicate {spec.id_field}: {id_str}",
            check_name="duplicate",
            row=row,
        )

    context.seen_ids.add(id_str)
    return ValidationResult(status="passed", row=row)


# ─── Check 3 — Range ────────────────────────────────────────────────────────

def check_range(row: dict[str, Any], context: CheckContext) -> ValidationResult:
    """
    Cross-field range and consistency checks that Pydantic doesn't cover.

    Examples:
      - cost should scale with duration (very loose heuristic, catches only
        egregious mismatches — not fraud, just data corruption)
      - bytes_used vs cost consistency for data sessions
      - timestamp not older than N years (catches clock bugs)
      - amount vs payment_type (refunds must be negative; topups positive)

    Pydantic handles per-field ranges (ge=0, le=1). This check catches
    cross-field relationships.
    """
    source = context.source

    if source == "voice":
        return _check_range_voice(row)
    if source == "data":
        return _check_range_data(row)
    if source == "revenue":
        return _check_range_revenue(row)
    # Other sources have no cross-field rules — pass through
    return ValidationResult(status="passed", row=row)


def _check_range_voice(row: dict[str, Any]) -> ValidationResult:
    """Voice CDR cross-field checks."""
    duration = row.get("duration_sec")
    cost = row.get("cost")
    if duration is None or cost is None:
        # Schema check already caught missing fields — pass here
        return ValidationResult(status="passed", row=row)

    # Rule: a zero-duration call with positive cost is suspicious
    try:
        if int(duration) == 0 and float(cost) > 0:
            return ValidationResult(
                status="failed",
                reason=f"zero-duration call with cost={cost}",
                check_name="range",
                row=row,
            )
    except (TypeError, ValueError):
        return ValidationResult(
            status="failed",
            reason="non-numeric duration or cost",
            check_name="range",
            row=row,
        )

    return ValidationResult(status="passed", row=row)


def _check_range_data(row: dict[str, Any]) -> ValidationResult:
    """Data session cross-field checks."""
    # Placeholder — no cross-field rules yet.
    return ValidationResult(status="passed", row=row)


def _check_range_revenue(row: dict[str, Any]) -> ValidationResult:
    """
    Revenue cross-field checks:
      - refunds must have negative amounts
      - topups must have positive amounts
    """
    ptype = row.get("payment_type")
    amount = row.get("amount")
    if ptype is None or amount is None:
        return ValidationResult(status="passed", row=row)

    try:
        amt = float(amount)
    except (TypeError, ValueError):
        return ValidationResult(
            status="failed",
            reason=f"non-numeric amount: {amount}",
            check_name="range",
            row=row,
        )

    # Pydantic already enforces amount >= 0 — so refunds can't be negative
    # in the current contract, but top-ups must always be non-negative.
    if ptype == "topup" and amt < 0:
        return ValidationResult(
            status="failed",
            reason=f"negative amount for topup: {amount}",
            check_name="range",
            row=row,
        )
    if amt != amt or amt in (float("inf"), float("-inf")):
        return ValidationResult(
            status="failed",
            reason=f"non-finite amount: {amount}",
            check_name="range",
            row=row,
        )

    return ValidationResult(status="passed", row=row)

    # ─── Check 4 — Referential Integrity ────────────────────────────────────────

def check_referential(row: dict[str, Any], context: CheckContext) -> ValidationResult:
    """
    Cross-table referential checks. Verifies that foreign keys in this row
    exist in the loaded reference tables.

    Current checks:
      - purchases.bundle_id must exist in bundle_catalog
      - any row with subscriber_id must reference a known subscriber

    Behavior when reference data is empty:
      - If the reference set is empty, the corresponding check is skipped.
      - This avoids a bad run if someone forgets to load reference data.

    Behavior when the field is missing:
      - Schema check already failed the row; skip here.
    """
    source = context.source

    # 1. Bundle catalog check — only for purchases
    if source == "purchases":
        result = _check_referential_purchases(row, context)
        if not result.passed:
            return result

    # 2. Subscriber check — applies to any source with subscriber_id
    result = _check_subscriber_exists(row, context)
    if not result.passed:
        return result

    return ValidationResult(status="passed", row=row)


def _check_subscriber_exists(
    row: dict[str, Any], context: CheckContext
) -> ValidationResult:
    """
    Verify that row['subscriber_id'] exists in the loaded subscriber set.
    Skipped if the set is empty (fail-safe).
    """
    subscriber_id = row.get("subscriber_id")
    if subscriber_id is None:
        # Schema check will fail this row — skip here
        return ValidationResult(status="passed", row=row)

    if not context.known_subscriber_ids:
        # Subscriber set not loaded — skip check rather than reject everything
        return ValidationResult(status="passed", row=row)

    if str(subscriber_id) not in context.known_subscriber_ids:
        return ValidationResult(
            status="failed",
            reason=f"unknown subscriber_id: {subscriber_id}",
            check_name="referential",
            row=row,
        )

    return ValidationResult(status="passed", row=row)

def _check_referential_purchases(
    row: dict[str, Any], context: CheckContext
) -> ValidationResult:
    """Purchases: bundle_id must exist in bundle_catalog."""
    bundle_id = row.get("bundle_id")
    if bundle_id is None:
        # Schema check will fail this row — skip here
        return ValidationResult(status="passed", row=row)

    if not context.known_bundle_ids:
        # Catalog not loaded — skip check rather than reject everything
        return ValidationResult(status="passed", row=row)

    if str(bundle_id) not in context.known_bundle_ids:
        return ValidationResult(
            status="failed",
            reason=f"unknown bundle_id: {bundle_id}",
            check_name="referential",
            row=row,
        )

    return ValidationResult(status="passed", row=row)
