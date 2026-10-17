"""Emit synthetic activation and purchase records into LocalStack Kinesis."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import EndpointConnectionError

from simulator import constants as C


ROOT = Path(__file__).resolve().parents[1]
BUCKET = "telco-fraud-bronze"


def _duration(value: str) -> float | None:
    if value.lower() == "none":
        return None
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("duration must be seconds or 'none'") from exc
    if seconds <= 0:
        raise argparse.ArgumentTypeError("duration must be positive")
    return seconds


def _clients():
    common = {
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return boto3.client("kinesis", **common), boto3.resource("dynamodb", **common)


def _load_bundle_catalog(dynamodb) -> list[dict]:
    table = dynamodb.Table("bundle_catalog")
    items = []
    response = table.scan(ProjectionExpression="bundle_id, #p", ExpressionAttributeNames={"#p": "price"})
    while True:
        items.extend(response.get("Items", []))
        key = response.get("LastEvaluatedKey")
        if not key:
            break
        response = table.scan(
            ProjectionExpression="bundle_id, #p", ExpressionAttributeNames={"#p": "price"},
            ExclusiveStartKey=key,
        )
    catalog = [{"bundle_id": str(item["bundle_id"]), "price": Decimal(str(item["price"]))} for item in items]
    if not catalog:
        raise RuntimeError("DynamoDB bundle_catalog contains no bundle IDs")
    return catalog


def _read_config() -> dict:
    import yaml
    return yaml.safe_load((ROOT / "simulator" / "config.yaml").read_text(encoding="utf-8"))


def _make_activation(rng: random.Random) -> dict:
    now = datetime.now(timezone.utc)
    msisdn = "+216" + rng.choice(C.MSISDN_PREFIXES) + f"{rng.randrange(1_000_000):06d}"
    imsi = rng.choice(C.IMSI_PREFIXES) + f"{rng.randrange(10_000_000_000):010d}"
    return {
        "event_id": f"ACT{rng.randint(10**9, 10**10)}",
        "subscriber_id": f"SUB{rng.randrange(10_000):06d}",
        "msisdn": msisdn,
        "imsi": imsi,
        "event_type": rng.choice(["SWAPPED", "ACTIVATED"]),
        "timestamp": now.isoformat(),
        "channel": rng.choice(C.ACTIVATION_CHANNELS),
    }


def _make_purchase(rng: random.Random, bundles: list[dict]) -> dict:
    bundle = rng.choice(bundles)
    return {
        "purchase_id": f"BUY{rng.randint(10**9, 10**10)}",
        "subscriber_id": f"SUB{rng.randrange(10_000):06d}",
        "bundle_id": bundle["bundle_id"],
        "channel": rng.choice(C.PURCHASE_CHANNELS),
        "price": bundle["price"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(rate: float = 5, streams: tuple[str, ...] = ("activation", "purchases"),
        duration: float | None = None, quiet: bool = False) -> dict[str, int | float]:
    config = _read_config()
    paths = config["paths"]
    stream_names = {
        "activation": paths["kinesis_activation_stream"],
        "purchases": paths["kinesis_purchases_stream"],
    }
    kinesis, dynamodb = _clients()
    bundles = _load_bundle_catalog(dynamodb) if "purchases" in streams else []
    rng = random.Random()
    counts = {"activation": 0, "purchases": 0, "failed": 0}
    start = time.monotonic()
    fatal_endpoint_error = False

    try:
        while duration is None or time.monotonic() - start < duration:
            batch: list[tuple[str, str, dict]] = []
            for _ in range(10):
                if duration is not None and time.monotonic() - start >= duration:
                    break
                if len(streams) == 1:
                    event_type = streams[0]
                else:
                    event_type = "activation" if rng.random() < 0.4 else "purchases"
                event = _make_activation(rng) if event_type == "activation" else _make_purchase(rng, bundles)
                batch.append((event_type, stream_names[event_type], event))

            for category in streams:
                entries = [entry for entry in batch if entry[0] == category]
                if not entries:
                    continue
                records = [
                    {"Data": json.dumps(event, default=str, separators=(",", ":")).encode("utf-8"),
                     "PartitionKey": event["subscriber_id"]}
                    for _category, _stream, event in entries
                ]
                try:
                    response = kinesis.put_records(StreamName=stream_names[category], Records=records)
                except Exception as exc:
                    counts["failed"] += len(entries)
                    traceback.print_exception(exc, file=sys.stderr)
                    if isinstance(exc, EndpointConnectionError):
                        fatal_endpoint_error = True
                        break
                    continue
                results = response.get("Records", [])
                for (_category, stream, event), result in zip(entries, results):
                    if result.get("ErrorCode"):
                        counts["failed"] += 1
                        print(f"Kinesis record failed ({result.get('ErrorCode')}): {result.get('ErrorMessage', '')}", file=sys.stderr)
                        continue
                    counts[category] += 1
                    if not quiet:
                        tag = "ACT" if category == "activation" else "BUY"
                        event_id = event.get("event_id", event.get("purchase_id"))
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] {tag} {event['subscriber_id']} → {stream} (event_id={event_id})", flush=True)
            if fatal_endpoint_error or (duration is not None and time.monotonic() - start >= duration):
                break
            time.sleep(10 / rate)
    except KeyboardInterrupt:
        pass

    elapsed = time.monotonic() - start
    print(
        f"Stopped after {elapsed:.0f}s — {counts['activation'] + counts['purchases']} events sent "
        f"(ACT: {counts['activation']}, BUY: {counts['purchases']}, {counts['failed']} failed)",
        flush=True,
    )
    return {**counts, "elapsed_seconds": elapsed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream synthetic events into Kinesis.")
    parser.add_argument("--rate", type=float, default=5, help="Target records per second")
    parser.add_argument("--streams", default="activation,purchases", help="Comma-separated activation,purchases")
    parser.add_argument("--duration", type=_duration, default=None, help="Seconds or 'none' (default: until Ctrl+C)")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-event output")
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be positive")
    streams = tuple(dict.fromkeys(part.strip() for part in args.streams.split(",") if part.strip()))
    invalid = sorted(set(streams) - {"activation", "purchases"})
    if invalid or not streams:
        parser.error(f"--streams must contain activation and/or purchases; invalid={invalid}")
    run(args.rate, streams, args.duration, args.quiet)


if __name__ == "__main__":
    main()
