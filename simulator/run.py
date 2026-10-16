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

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

import numpy as np
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


# ─── Main Pipeline ──────────────────────────────────────────────────────────

def run_simulation(
    config_path: str | Path = "simulator/config.yaml",
    write_outputs: bool = True,
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
    cfg = load_config(config_path)

    seed = cfg["seed"]
    n_subs = cfg["population"]["num_subscribers"]
    n_days = cfg["population"]["days"]
    start_date = datetime.fromisoformat(cfg["population"]["start_date"]).replace(
        tzinfo=timezone.utc
    )

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

    # 5. Write labels + config snapshot
    if write_outputs:
        print("[5/5] Writing labels + config snapshot...")
        labels_path = write_labels(labels, cfg, config_path=config_path)
        print(f"      → {labels_path}")

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

if __name__ == "__main__":
    from simulator.writers import write_all
    from simulator.run import load_config

    collector, labels, subscribers = run_simulation()

    # ── Hand in-memory events to LocalStack (Kinesis, S3, DynamoDB) ──
    cfg = load_config("simulator/config.yaml")
    print()
    print("─── Uploading to LocalStack ───────────────────")
    write_summary = write_all(collector, subscribers, cfg)
    print()
    print("Upload summary:")
    import json
    print(json.dumps(write_summary, indent=2, default=str))

    # Print label summary
    from simulator.labels import load_labels
    df = load_labels()
    print()
    print("Label summary:")
    print(json.dumps(label_summary(df), indent=2))
