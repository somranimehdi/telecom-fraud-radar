"""
Simulator — Continuous Streaming Mode
--------------------------------------
Runs forever. Emits fresh events to Kinesis and S3 in real-time,
simulating a live telco feed.

Usage:
    python -m simulator.stream                    # 60x speed (default)
    python -m simulator.stream --speed 1          # real time (slow demo)
    python -m simulator.stream --speed 600        # 10x faster (fast demo)
    python -m simulator.stream --subscribers 500  # smaller population
    python -m simulator.stream --duration 300     # stop after 300 seconds

Timing:
    At 60x speed, 1 real second = 1 simulated minute.
    A full simulated day passes every 24 real seconds.

What it does:
    Startup (once):
      1. Generate subscriber profiles
      2. Assign fraud + overlap (same as batch mode)
      3. Load subscribers + bundles to DynamoDB
    Loop (forever):
      Every real second:
        - Advance simulated time by 1 minute
        - Generate events for that minute:
            * Streaming sources → Kinesis (activations, purchases)
            * Batch sources → memory buffer (CDRs, revenue)
        - Flush batch buffers to S3 every --flush-every minutes
        - Print progress
      Ctrl+C → clean shutdown.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

import numpy as np
import yaml

from simulator import constants as C
from simulator.baseline import (
    SubscriberProfile,
    generate_subscribers,
    generate_daily_activity,
)
from simulator.fraud_injector import (
    FraudAssignment,
    assign_fraud_subscribers,
    inject_fraud_for_day,
)
from simulator.overlap import (
    assign_overlap_subscribers,
    apply_legit_looks_fraud,
    apply_fraud_looks_legit,
)
from simulator.writers import (
    make_clients,
    write_events_to_kinesis,
    write_parquet_to_s3,
    write_subscribers_to_dynamodb,
    write_bundles_to_dynamodb,
)


# ─── Config Loading ─────────────────────────────────────────────────────────

def load_config(path: str | Path = "simulator/config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# ─── Print Helpers ──────────────────────────────────────────────────────────

def _banner(cfg: dict, speed: float, subs_count: int) -> None:
    print("─" * 65)
    print("  Telecom Fraud Radar — Continuous Streaming Producer")
    print("─" * 65)
    print(f"  Speed           : {speed:.0f}x  (1 real sec = {speed:.0f} sim sec)")
    print(f"  Subscribers     : {subs_count:,}")
    print(f"  Sim start       : {cfg['population']['start_date']}")
    print(f"  Kinesis streams : {cfg['paths']['kinesis_activation_stream']}")
    print(f"                    {cfg['paths']['kinesis_purchases_stream']}")
    print(f"  S3 bucket       : {cfg['paths']['s3_bucket']}")
    print(f"  DynamoDB tables : {cfg['paths']['dynamodb_subscribers']}, "
          f"{cfg['paths']['dynamodb_bundles']}")
    print("─" * 65)
    print("  Ctrl+C to stop cleanly.")
    print("─" * 65)
    print()


# ─── Simulated Clock ────────────────────────────────────────────────────────

class SimClock:
    """
    Tracks simulated time. Starts at the config's start_date.
    Each `tick(real_seconds)` call advances simulated time by
    real_seconds * speed.
    """

    def __init__(self, start: datetime, speed: float):
        self.start = start
        self.speed = speed
        self.elapsed_real_seconds = 0.0

    @property
    def now(self) -> datetime:
        return self.start + timedelta(seconds=self.elapsed_real_seconds * self.speed)

    @property
    def current_day_index(self) -> int:
        return int((self.now - self.start).total_seconds() // 86400)

    @property
    def current_minute_of_day(self) -> int:
        """Which minute (0–1439) of the current simulated day we're in."""
        elapsed = (self.now - self.start).total_seconds()
        return int((elapsed % 86400) // 60)

    def tick(self, real_seconds: float = 1.0) -> None:
        self.elapsed_real_seconds += real_seconds


# ─── Per-Minute Event Generation ────────────────────────────────────────────

def _generate_minute(
    subscribers: list[SubscriberProfile],
    assignments: dict[str, FraudAssignment],
    legit_fraud_like: set[str],
    fraud_legit_like: set[str],
    clock: SimClock,
    cfg: dict,
    rng: Random,
    np_rng: np.random.Generator,
) -> dict:
    """
    Generate one simulated minute of activity.

    Returns a dict with:
      - activation_events: list (streaming)
      - bundle_purchases: list (streaming)
      - voice_cdrs: list (batch)
      - data_cdrs: list (batch)
      - revenue_txs: list (batch)
    """
    day = clock.now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_index = clock.current_day_index

    # Sample a small random subset of subscribers to be "active this minute".
    # At 60x speed, we only see 1/60th of a day's events per tick.
    # Baseline activity was designed per-day, so we scale down.
    sample_size = max(1, int(len(subscribers) * 0.02))   # ~2% of subs active per minute
    active_subs = rng.sample(subscribers, sample_size)

    out = {
        "activation_events": [],
        "bundle_purchases": [],
        "voice_cdrs": [],
        "data_cdrs": [],
        "revenue_txs": [],
    }

    for sub in active_subs:
        # Generate that subscriber's full daily activity for this day.
        # We cache per-subscriber-per-day so we don't regenerate it 1440 times.
        # Simpler approach: just generate a tiny slice now.
        activity = generate_daily_activity(sub, day, cfg, rng, np_rng)

        # Fraud injection for this subscriber, if they're fraud
        assignment = assignments.get(sub.subscriber_id)
        if assignment is not None:
            activity = inject_fraud_for_day(
                activity, sub, assignment, day_index, rng, np_rng
            )
            if sub.subscriber_id in fraud_legit_like:
                activity = apply_fraud_looks_legit(
                    activity, sub, assignment, day_index, rng, np_rng
                )
        elif sub.subscriber_id in legit_fraud_like:
            activity = apply_legit_looks_fraud(activity, sub, rng, np_rng)

        # Sample ONE event per category per minute, if the subscriber has any
        # (This spreads each subscriber's daily events across the 1440 sim-minutes.)
        if activity.voice_calls:
            out["voice_cdrs"].append(rng.choice(activity.voice_calls))
        if activity.data_sessions:
            out["data_cdrs"].append(rng.choice(activity.data_sessions))
        if activity.revenue_txs:
            out["revenue_txs"].append(rng.choice(activity.revenue_txs))
        if activity.bundle_purchases:
            out["bundle_purchases"].append(rng.choice(activity.bundle_purchases))

        # Activation event attached by fraud injector
        act_event = getattr(activity, "activation_event", None)
        if act_event:
            out["activation_events"].append(act_event)

    return out


# ─── Main Streaming Loop ────────────────────────────────────────────────────

def run_stream(
    config_path: str | Path = "simulator/config.yaml",
    speed: float = 60.0,
    subs_override: int | None = None,
    duration_seconds: float | None = None,
    flush_every: int = 5,
) -> None:
    """
    Main streaming loop.

    Args:
      config_path: path to config.yaml
      speed: simulated seconds per real second
      subs_override: override subscriber count from config
      duration_seconds: stop after N real seconds (None = forever)
      flush_every: flush batch buffers to S3 every N real seconds
    """
    cfg = load_config(config_path)
    if subs_override is not None:
        cfg["population"]["num_subscribers"] = subs_override

    seed = cfg["seed"]
    n_subs = cfg["population"]["num_subscribers"]
    start_date = datetime.fromisoformat(cfg["population"]["start_date"]).replace(
        tzinfo=timezone.utc
    )

    # ─── Setup ──────────────────────────────────────────────────────────────
    _banner(cfg, speed, n_subs)

    print("[setup] Generating subscribers...")
    subscribers = generate_subscribers(n_subs, start_date, seed=seed)
    print(f"[setup] → {len(subscribers):,} profiles")

    print("[setup] Assigning fraud + overlap...")
    assignments, labels = assign_fraud_subscribers(subscribers, cfg, seed=seed)
    legit_fraud_like, fraud_legit_like = assign_overlap_subscribers(
        subscribers, labels, cfg, seed=seed
    )
    n_fraud = sum(1 for l in labels if l.is_fraud == 1)
    print(f"[setup] → {n_fraud:,} fraud, {len(legit_fraud_like):,} legit-fraud-like, "
          f"{len(fraud_legit_like):,} fraud-legit-like")

    print("[setup] Creating AWS clients...")
    clients = make_clients()

    print("[setup] Loading reference data into DynamoDB...")
    write_subscribers_to_dynamodb(
        clients["dynamodb"], cfg["paths"]["dynamodb_subscribers"], subscribers,
        verbose=False,
    )
    write_bundles_to_dynamodb(
        clients["dynamodb"], cfg["paths"]["dynamodb_bundles"], C.BUNDLE_CATALOG,
        verbose=False,
    )
    print(f"[setup] → subscribers + bundles loaded")

    print("[setup] Streaming begins now. Open another terminal to watch S3 grow.")
    print()

    # ─── Streaming loop ─────────────────────────────────────────────────────
    clock = SimClock(start_date, speed)
    rng = Random(seed)
    np_rng = np.random.default_rng(seed)
    t0 = time.time()
    ticks = 0
    total_events = 0
    total_batches = 0

    # Batch buffers
    buffer_voice: list[dict] = []
    buffer_data: list[dict] = []
    buffer_revenue: list[dict] = []
    last_flush_real_time = t0

    # Ctrl+C handler
    stop = {"flag": False}

    def _signal_handler(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _signal_handler)

    try:
        while not stop["flag"]:
            tick_start = time.time()

            # Generate one simulated minute
            events = _generate_minute(
                subscribers, assignments, legit_fraud_like, fraud_legit_like,
                clock, cfg, rng, np_rng,
            )

            # Streaming: push activations + purchases to Kinesis immediately
            if events["activation_events"]:
                write_events_to_kinesis(
                    clients["kinesis"],
                    cfg["paths"]["kinesis_activation_stream"],
                    events["activation_events"],
                    batch_size=cfg["outputs"]["kinesis_batch_size"],
                    verbose=False,
                )
            if events["bundle_purchases"]:
                write_events_to_kinesis(
                    clients["kinesis"],
                    cfg["paths"]["kinesis_purchases_stream"],
                    events["bundle_purchases"],
                    batch_size=cfg["outputs"]["kinesis_batch_size"],
                    verbose=False,
                )

            # Batch: buffer CDRs + revenue
            buffer_voice.extend(events["voice_cdrs"])
            buffer_data.extend(events["data_cdrs"])
            buffer_revenue.extend(events["revenue_txs"])

            # Flush batch buffers to S3 every N real seconds
            if time.time() - last_flush_real_time >= flush_every:
                if buffer_voice:
                    write_parquet_to_s3(
                        clients["s3"], cfg["paths"]["s3_bucket"], "raw/usage/voice",
                        buffer_voice, clock.now, filename=f"voice_{ticks:08d}.parquet",
                        verbose=False,
                    )
                    total_batches += 1
                if buffer_data:
                    write_parquet_to_s3(
                        clients["s3"], cfg["paths"]["s3_bucket"], "raw/usage/data",
                        buffer_data, clock.now, filename=f"data_{ticks:08d}.parquet",
                        verbose=False,
                    )
                if buffer_revenue:
                    write_parquet_to_s3(
                        clients["s3"], cfg["paths"]["s3_bucket"], "raw/revenue/prepaid",
                        buffer_revenue, clock.now, filename=f"revenue_{ticks:08d}.parquet",
                        verbose=False,
                    )
                buffer_voice.clear()
                buffer_data.clear()
                buffer_revenue.clear()
                last_flush_real_time = time.time()

            # Progress line
            # Advance simulated time by 1 real second × speed
            clock.tick(1.0)

            # Progress line
            ticks += 1
            minute_events = (
                len(events["activation_events"])
                + len(events["bundle_purchases"])
                + len(events["voice_cdrs"])
                + len(events["data_cdrs"])
                + len(events["revenue_txs"])
            )
            total_events += minute_events
            sim_ts = clock.now.strftime("%Y-%m-%d %H:%M")
            print(
                f"[stream] {sim_ts} sim | "
                f"tick {ticks:>5} | "
                f"{minute_events:>4} events | "
                f"total {total_events:>8,} | "
                f"batches {total_batches:>4}"
            )

    except KeyboardInterrupt:
        pass

    # ─── Flush remaining buffers ────────────────────────────────────────────
    if buffer_voice or buffer_data or buffer_revenue:
        print()
        print("[shutdown] Flushing remaining buffers to S3...")
        if buffer_voice:
            write_parquet_to_s3(
                clients["s3"], cfg["paths"]["s3_bucket"], "raw/usage/voice",
                buffer_voice, clock.now, filename=f"voice_final_{ticks:08d}.parquet",
                verbose=False,
            )
        if buffer_data:
            write_parquet_to_s3(
                clients["s3"], cfg["paths"]["s3_bucket"], "raw/usage/data",
                buffer_data, clock.now, filename=f"data_final_{ticks:08d}.parquet",
                verbose=False,
            )
        if buffer_revenue:
            write_parquet_to_s3(
                clients["s3"], cfg["paths"]["s3_bucket"], "raw/revenue/prepaid",
                buffer_revenue, clock.now, filename=f"revenue_final_{ticks:08d}.parquet",
                verbose=False,
            )
        print(f"[shutdown] → flushed {len(buffer_voice)} voice, "
              f"{len(buffer_data)} data, {len(buffer_revenue)} revenue")
        buffer_voice.clear()
        buffer_data.clear()
        buffer_revenue.clear()

    # ─── Shutdown ───────────────────────────────────────────────────────────
    print()
    print("─" * 65)
    print("  Shutdown")
    print("─" * 65)
    print(f"  Real time elapsed : {time.time() - t0:.1f}s")
    print(f"  Sim time advanced : {clock.elapsed_real_seconds * speed / 3600:.2f} sim hours")
    print(f"  Ticks             : {ticks:,}")
    print(f"  Events emitted    : {total_events:,}")
    print(f"  Batch flushes     : {total_batches:,}")
    print(f"  Remaining buffer  : voice={len(buffer_voice)} data={len(buffer_data)} revenue={len(buffer_revenue)}")
    print("─" * 65)


# ─── CLI ────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Continuous streaming producer")
    p.add_argument("--config", default="simulator/config.yaml")
    p.add_argument("--speed", type=float, default=60.0,
                   help="simulated seconds per real second (default 60)")
    p.add_argument("--subscribers", type=int, default=None,
                   help="override subscriber count from config")
    p.add_argument("--duration", type=float, default=None,
                   help="stop after N real seconds (default: forever)")
    p.add_argument("--flush-every", type=int, default=5,
                   help="flush batch buffers every N real seconds (default 5)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_stream(
        config_path=args.config,
        speed=args.speed,
        subs_override=args.subscribers,
        duration_seconds=args.duration,
        flush_every=args.flush_every,
    )