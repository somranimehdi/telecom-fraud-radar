"""
Simulator — Orchestrator
-------------------------
Runs the full pipeline:

  1. Load config
  2. Generate subscriber profiles
  3. Assign fraud + overlap
  4. For each day × subscriber:
       generate baseline activity
       → apply fraud injection (if fraud subscriber)
       → apply overlap (if in overlap set)
  5. Collect all events
  6. Write labels (sealed envelope) + config snapshot
  7. [Step 8] Hand events to writers for Kinesis / S3 / DynamoDB upload

The final step (write to AWS) is intentionally separated so we can verify
the data generator in isolation before wiring AWS IO.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from random import Random

import numpy as np
import polars as pl
import yaml

from simulator.baseline import (
    SubscriberProfile,
    DailyActivity,
    generate_subscribers,
    generate_daily_activity,
)
from simulator.fraud_injector import (
    FraudAssignment,
    FraudLabel,
    assign_fraud_subscribers,
    inject_fraud_for_day,
)
from simulator.overlap import (
    assign_overlap_subscribers,
    apply_legit_looks_fraud,
    apply_fraud_looks_legit,
)
from simulator.labels import write_labels, label_summary


# ─── Config Loading ─────────────────────────────────────────────────────────

def load_config(path: str | Path = "simulator/config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# ─── Event Collection ───────────────────────────────────────────────────────

class EventCollector:
    """Accumulates events across the full simulation run."""

    def __init__(self):
        self.activation_events: list[dict] = []
        self.bundle_purchases: list[dict] = []
        self.voice_cdrs: list[dict] = []
        self.data_cdrs: list[dict] = []
        self.revenue_txs: list[dict] = []
        self.injected_defects: list[dict] = []

    def add_activity(self, activity: DailyActivity) -> None:
        self.voice_cdrs.extend(activity.voice_calls)
        self.data_cdrs.extend(activity.data_sessions)
        self.revenue_txs.extend(activity.revenue_txs)
        self.bundle_purchases.extend(activity.bundle_purchases)

        # Baseline activations and fraud-induced SIM swaps share the same
        # event collection, but are generated independently.
        act_event = activity.activation_event
        if act_event:
            self.activation_events.append(act_event)

    def summary(self) -> dict:
        return {
            "activation_events": len(self.activation_events),
            "bundle_purchases": len(self.bundle_purchases),
            "voice_cdrs": len(self.voice_cdrs),
            "data_cdrs": len(self.data_cdrs),
            "revenue_txs": len(self.revenue_txs),
            "total_events": (
                len(self.activation_events)
                + len(self.bundle_purchases)
                + len(self.voice_cdrs)
                + len(self.data_cdrs)
                + len(self.revenue_txs)
            ),
        }


DEFAULT_DEFECT_RATE = 0.015


def simulation_start_date(config: dict, end_yesterday: bool = False,
                          today: date | None = None) -> date:
    """Resolve the first simulated day, optionally ending the window yesterday."""
    if end_yesterday:
        end_date = (today or date.today()) - timedelta(days=1)
        return end_date - timedelta(days=int(config["population"]["days"]) - 1)
    return date.fromisoformat(config["population"]["start_date"])


def inject_defects(collector: EventCollector, labels: list[FraudLabel], rate: float,
                   seed: int = 42) -> list[dict]:
    """Inject deterministic raw-row defects without altering fraud subscribers."""
    if not 0 <= rate <= 1:
        raise ValueError("defect rate must be between 0 and 1")
    collector.injected_defects = []
    if rate == 0:
        return collector.injected_defects

    fraud_ids = {label.subscriber_id for label in labels if label.is_fraud == 1}
    sources = {
        "voice": (collector.voice_cdrs, "call_id", ["subscriber_id", "timestamp", "duration_sec", "cost"]),
        "data": (collector.data_cdrs, "session_id", ["subscriber_id", "timestamp", "bytes_used", "cost"]),
        "revenue": (collector.revenue_txs, "transaction_id", ["subscriber_id", "timestamp", "amount"]),
        "activation": (collector.activation_events, "event_id", ["subscriber_id", "timestamp", "event_type"]),
        "purchases": (collector.bundle_purchases, "purchase_id", ["subscriber_id", "timestamp", "price"]),
    }
    rng = Random(seed)
    defect_kinds = ["duplicate_id", "negative_value", "future_timestamp", "null_required"]
    for source, (rows, id_field, required_fields) in sources.items():
        eligible = [i for i, row in enumerate(rows) if str(row.get("subscriber_id")) not in fraud_ids]
        if not eligible:
            continue
        count = min(len(eligible), max(1, math.ceil(len(rows) * rate)))
        rng.shuffle(eligible)
        chosen = eligible[:count]
        # Cycle defect kinds in a seeded order, preserving an even distribution.
        kinds = defect_kinds[:]
        rng.shuffle(kinds)
        for offset, index in enumerate(chosen):
            kind = kinds[offset % len(kinds)]
            row = rows[index]
            row_id = str(row.get(id_field, ""))
            if kind == "duplicate_id":
                rows.append(dict(row))
                row_index = len(rows) - 1
            elif kind == "negative_value":
                field_name = next((f for f in ("duration_sec", "amount", "cost", "price") if f in row), None)
                if field_name is None:
                    # Activation rows have no amount/duration field; substitute a
                    # duplicate so each source still receives a detectable defect.
                    rows.append(dict(row))
                    kind = "duplicate_id"
                    row_index = len(rows) - 1
                else:
                    row[field_name] = -abs(row[field_name]) if row[field_name] is not None else -1
                    row_index = index
            elif kind == "future_timestamp":
                row["timestamp"] = datetime.now(timezone.utc) + timedelta(days=365)
                row_index = index
            else:
                null_field = next((f for f in required_fields if f != id_field), required_fields[0])
                row[null_field] = None
                row_index = index
            collector.injected_defects.append({
                "source": source, "row_id": row_id, "defect_type": kind, "row_index": row_index,
            })
    return collector.injected_defects


# ─── Main Pipeline ──────────────────────────────────────────────────────────

def run_simulation(
    config_path: str | Path = "simulator/config.yaml",
    write_outputs: bool = True,
    defect_rate: float = DEFAULT_DEFECT_RATE,
    end_yesterday: bool = False,
    config: dict | None = None,
) -> tuple[EventCollector, list[FraudLabel], list[SubscriberProfile]]:
    """
    Run the full simulator.

    Args:
      config_path: path to config.yaml
      write_outputs: if True, write labels + config snapshot to simulator/out/

    Returns:
      (collector, labels) — events in memory + ground truth
    """
    t0 = time.time()
    cfg = config if config is not None else load_config(config_path)

    seed = cfg["seed"]
    n_subs = cfg["population"]["num_subscribers"]
    n_days = cfg["population"]["days"]
    start_date = datetime.combine(
        simulation_start_date(cfg, end_yesterday), datetime.min.time(), tzinfo=timezone.utc
    )
    if end_yesterday:
        cfg["population"]["start_date"] = start_date.date().isoformat()

    print(f"─── Simulator Run ──────────────────────────────")
    print(f"  Seed         : {seed}")
    print(f"  Subscribers  : {n_subs:,}")
    print(f"  Days         : {n_days}")
    print(f"  Start date   : {start_date.date()}")
    print()

    # 1. Generate subscriber profiles
    print("[1/5] Generating subscriber profiles...")
    subscribers = generate_subscribers(
        n_subs, start_date, seed=seed, simulation_days=n_days
    )
    print(f"      → {len(subscribers):,} profiles")

    # 2. Assign fraud
    print("[2/5] Assigning fraud subscribers...")
    assignments, labels = assign_fraud_subscribers(subscribers, cfg, seed=seed)
    n_fraud = sum(1 for l in labels if l.is_fraud == 1)
    print(f"      → {n_fraud:,} fraud subscribers ({100 * n_fraud / n_subs:.2f}%)")

    # 3. Assign overlap
    print("[3/5] Assigning overlap subscribers...")
    legit_fraud_like, fraud_legit_like = assign_overlap_subscribers(
        subscribers, labels, cfg, seed=seed
    )
    print(f"      → {len(legit_fraud_like):,} legit-looks-fraud")
    print(f"      → {len(fraud_legit_like):,} fraud-looks-legit")

    # 4. Generate daily activity
    print(f"[4/5] Generating {n_days} days of activity...")
    collector = EventCollector()
    rng = Random(seed)
    np_rng = np.random.default_rng(seed)

    for day_index in range(n_days):
        day = start_date + timedelta(days=day_index)
        for sub in subscribers:
            activity = generate_daily_activity(sub, day, cfg, rng, np_rng)

            # Occasional legitimate SIM replacements for upgrades, loss, or repair.
            if (
                activity.activation_event is None
                and sub.activation_date is not None
                and sub.activation_date.date() != day.date()
                and rng.random() < 0.003 / 30
            ):
                swap_hour = int(np_rng.integers(0, 24))
                swap_ts = day.replace(
                    hour=swap_hour, minute=int(np_rng.integers(0, 60)),
                    second=0, microsecond=0, tzinfo=timezone.utc,
                )
                activity.activation_event = {
                    "event_id": f"ACT{rng.randint(10**9, 10**10)}",
                    "subscriber_id": sub.subscriber_id,
                    "msisdn": sub.msisdn,
                    "imsi": sub.imsi,
                    "event_type": "SWAPPED",
                    "timestamp": swap_ts,
                    "channel": rng.choice(["retail", "call_center", "dealer"]),
                }

            # Apply fraud if this subscriber has an assignment
            assignment = assignments.get(sub.subscriber_id)
            if assignment is not None:
                activity = inject_fraud_for_day(
                    activity, sub, assignment, day_index, rng, np_rng
                )
                # Overlap: dampen fraud if this subscriber is in the hiding set
                if sub.subscriber_id in fraud_legit_like:
                    activity = apply_fraud_looks_legit(
                        activity, sub, assignment, day_index, rng, np_rng
                    )
            elif sub.subscriber_id in legit_fraud_like:
                # Legit subscriber who mimics fraud
                activity = apply_legit_looks_fraud(activity, sub, rng, np_rng)

            collector.add_activity(activity)

        # Progress indicator (every 5 days)
        if (day_index + 1) % 5 == 0 or day_index == n_days - 1:
            elapsed = time.time() - t0
            print(f"      → day {day_index + 1:>2}/{n_days}  "
                  f"({collector.summary()['total_events']:,} events, "
                  f"{elapsed:.1f}s)")

    # Inject raw defects after all fraud patterns have been generated.
    injected_defects = inject_defects(collector, labels, defect_rate, seed=seed)

    # 5. Write labels + config snapshot
    if write_outputs:
        print("[5/5] Writing labels + config snapshot...")
        labels_path = write_labels(labels, cfg, config_path=config_path)
        print(f"      → {labels_path}")
        defects_path = Path("simulator/out/injected_defects.parquet")
        defects_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([{k: item[k] for k in ("source", "row_id", "defect_type")}
                      for item in injected_defects],
                     schema={"source": pl.String, "row_id": pl.String, "defect_type": pl.String})\
            .write_parquet(defects_path)
        print(f"      → {defects_path} ({len(injected_defects):,} defects)")

    elapsed = time.time() - t0
    print()
    print(f"─── Run Complete ({elapsed:.1f}s) ─────────────────────")
    summary = collector.summary()
    for key, val in summary.items():
        print(f"  {key:20s}: {val:>10,}")
    print()

    return collector, labels, subscribers

# ─── CLI Entry Point ────────────────────────────────────────────────────────

# ─── CLI Entry Point ────────────────────────────────────────────────────────

def _defect_rate_arg(value: str) -> float:
    try:
        rate = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("defect rate must be a number between 0 and 1") from exc
    if not 0 <= rate <= 1:
        raise argparse.ArgumentTypeError("defect rate must be between 0 and 1")
    return rate


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate simulated telecom fraud events.")
    parser.add_argument("--config", default="simulator/config.yaml",
                        help="Path to simulator config YAML")
    parser.add_argument("--defect-rate", type=_defect_rate_arg, default=DEFAULT_DEFECT_RATE,
                        help="Share of raw rows to defect (0 disables injection)")
    parser.add_argument("--end-yesterday", action="store_true",
                        help="Shift the simulation window to end yesterday")
    return parser.parse_args(argv)


RAW_PREFIXES = {
    "activation": "raw/activation/",
    "purchases": "raw/bundles/purchases/",
    "voice": "raw/usage/voice/",
    "data": "raw/usage/data/",
    "revenue": "raw/revenue/prepaid/",
}


def verify_s3_outputs(s3, bucket: str, write_summary: dict, write_to_s3: bool) -> dict:
    """Print current raw-prefix object counts and fail if this run wrote none."""
    written_keys = [
        key for name, value in write_summary.items() if name.startswith("s3_")
        for key in (value.values() if isinstance(value, dict) else [value])
        if isinstance(key, str) and key
    ]
    counts = {}
    for source, prefix in RAW_PREFIXES.items():
        objects = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(
                item for item in page.get("Contents", [])
                if not item["Key"].endswith("/") and item.get("Size", 0) > 0
            )
        newest = max((item.get("LastModified") for item in objects if item.get("LastModified")), default=None)
        counts[source] = {"count": len(objects), "newest_last_modified": newest}
        print(f"  {source:10s}: {len(objects):,} objects; newest LastModified={newest}")
    if write_to_s3 and not written_keys:
        raise SystemExit("S3 output is enabled, but this run wrote no raw objects.")
    return counts


if __name__ == "__main__":
    from simulator.writers import make_clients, write_all

    args = _parse_args()
    cfg = load_config(args.config)
    collector, labels, subscribers = run_simulation(
        config_path=args.config,
        config=cfg,
        defect_rate=args.defect_rate,
        end_yesterday=args.end_yesterday,
    )

    # ── Hand in-memory events to LocalStack (Kinesis, S3, DynamoDB) ──
    clients = make_clients(region=cfg.get("region", "us-east-1"))
    print()
    print("─── Uploading to LocalStack ───────────────────")
    write_summary = write_all(collector, subscribers, cfg, clients=clients)
    print()
    print("Upload summary:")
    print(json.dumps(write_summary, indent=2, default=str))
    print("S3 raw-prefix verification:")
    verify_s3_outputs(
        clients["s3"], cfg["paths"]["s3_bucket"], write_summary,
        write_to_s3=bool(cfg["outputs"].get("write_to_s3", False)),
    )

    # Print label summary
    from simulator.labels import load_labels
    df = load_labels()
    print()
    print("Label summary:")
    print(json.dumps(label_summary(df), indent=2))
