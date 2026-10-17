"""Tail LocalStack Kinesis streams and append their records to Bronze S3."""

from __future__ import annotations

import argparse
import io
import json
import os
import time
import traceback
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl
import yaml
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PREFIX = {
    "activation": "raw/activation",
    "purchases": "raw/bundles/purchases",
}


def _clients():
    common = {
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return boto3.client("kinesis", **common), boto3.client("s3", **common)


def _config() -> dict:
    return yaml.safe_load((ROOT / "simulator" / "config.yaml").read_text(encoding="utf-8"))


def _date_from_event(event: dict) -> str | None:
    value = event.get("timestamp")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def _to_parquet(records: list[dict]) -> bytes:
    frame = pl.DataFrame(records, strict=False)
    buffer = io.BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def _write_batch(s3, bucket: str, source: str, day: str, records: list[dict]) -> str:
    year, month, dom = day.split("-")
    prefix = SOURCE_PREFIX[source]
    timestamp = datetime.now().strftime("%H%M%S")
    key = f"{prefix}/{year}/{month}/{dom}/batch-{timestamp}-{uuid.uuid4().hex[:8]}.parquet"
    body = _to_parquet(records)
    for attempt in range(2):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/vnd.apache.parquet")
            return key
        except Exception as exc:
            if attempt == 0:
                print(f"S3 write failed for s3://{bucket}/{key}; retrying once: {exc}", flush=True)
                time.sleep(1)
            else:
                print(f"S3 write failed after retry for s3://{bucket}/{key}: {exc}", flush=True)
                traceback.print_exception(exc)
    return ""


def _initial_shard_state(kinesis, stream: str, shard: dict, quiet: bool) -> dict:
    shard_id = shard["ShardId"]
    iterator = kinesis.get_shard_iterator(
        StreamName=stream, ShardId=shard_id, ShardIteratorType="LATEST"
    )["ShardIterator"]
    prefetched = []
    try:
        peek = kinesis.get_records(ShardIterator=iterator, Limit=1)
        prefetched = peek.get("Records", [])
        iterator = peek.get("NextShardIterator", iterator)
        sequence = prefetched[0].get("SequenceNumber") if prefetched else "LATEST"
    except Exception as exc:
        print(f"Initial tail peek failed for {stream}/{shard_id}: {exc}", flush=True)
        sequence = "LATEST"
    if not quiet:
        print(f"Tailing {stream} shard {shard_id} from sequence {sequence} (LATEST)", flush=True)
    return {"stream": stream, "shard_id": shard_id, "iterator": iterator,
            "pending": prefetched, "last_sequence": sequence if sequence != "LATEST" else None}


def _recover_iterator(kinesis, state: dict) -> None:
    request = {
        "StreamName": state["stream"],
        "ShardId": state["shard_id"],
        "ShardIteratorType": "AFTER_SEQUENCE_NUMBER" if state.get("last_sequence") else "LATEST",
    }
    if state.get("last_sequence"):
        request["StartingSequenceNumber"] = state["last_sequence"]
    try:
        state["iterator"] = kinesis.get_shard_iterator(**request)["ShardIterator"]
    except Exception as exc:
        print(f"Could not reset iterator for {state['stream']}/{state['shard_id']}: {exc}", flush=True)


def run(interval: float = 2, quiet: bool = False) -> dict[str, int | float]:
    cfg = _config()
    paths = cfg["paths"]
    bucket = paths["s3_bucket"]
    streams = {
        "activation": paths["kinesis_activation_stream"],
        "purchases": paths["kinesis_purchases_stream"],
    }
    kinesis, s3 = _clients()
    states = []
    for source, stream in streams.items():
        try:
            description = kinesis.describe_stream(StreamName=stream)["StreamDescription"]
            for shard in description.get("Shards", []):
                states.append(_initial_shard_state(kinesis, stream, shard, quiet))
        except Exception as exc:
            print(f"Unable to initialize {source} stream {stream}: {exc}", flush=True)
            traceback.print_exception(exc)

    totals = {"activation": 0, "purchases": 0, "decode_errors": 0, "write_failures": 0}
    start = time.monotonic()
    try:
        while True:
            grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
            for state in states:
                records = state.pop("pending", [])
                try:
                    response = kinesis.get_records(ShardIterator=state["iterator"], Limit=100)
                    records.extend(response.get("Records", []))
                    state["iterator"] = response.get("NextShardIterator", state["iterator"])
                    if records:
                        state["last_sequence"] = records[-1].get("SequenceNumber", state.get("last_sequence"))
                except Exception as exc:
                    print(f"Kinesis poll failed for {state['stream']}/{state['shard_id']}: {exc}", flush=True)
                    if isinstance(exc, ClientError) and exc.response.get("Error", {}).get("Code") in {
                        "ExpiredIteratorException", "InvalidArgumentException", "ResourceNotFoundException",
                    }:
                        _recover_iterator(kinesis, state)

                source = next((name for name, stream in streams.items() if stream == state["stream"]), None)
                for record in records:
                    try:
                        event = json.loads(record["Data"].decode("utf-8"))
                    except Exception as exc:
                        totals["decode_errors"] += 1
                        print(f"Skipping undecodable Kinesis record ({state['stream']}): {exc}", flush=True)
                        continue
                    day = _date_from_event(event)
                    if source is None or day is None:
                        totals["decode_errors"] += 1
                        print(f"Skipping record with missing/invalid timestamp: {event}", flush=True)
                        continue
                    grouped[(source, day)].append(event)

            for (source, day), batch in grouped.items():
                key = _write_batch(s3, bucket, source, day, batch)
                if not key:
                    totals["write_failures"] += len(batch)
                    continue
                totals[source] += len(batch)
                if not quiet:
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] Ingested {len(batch)} records "
                        f"→ s3://{bucket}/{key}", flush=True,
                    )
            time.sleep(interval)
    except KeyboardInterrupt:
        pass

    elapsed = time.monotonic() - start
    print(
        f"Stopped after {elapsed:.0f}s — {totals['activation'] + totals['purchases']} records ingested "
        f"(activation: {totals['activation']}, purchases: {totals['purchases']})",
        flush=True,
    )
    if totals["decode_errors"] or totals["write_failures"]:
        print(f"Skipped: {totals['decode_errors']} decode errors, {totals['write_failures']} write failures", flush=True)
    return {**totals, "elapsed_seconds": elapsed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Tail Kinesis and write events to Bronze S3.")
    parser.add_argument("--interval", type=float, default=2, help="Polling interval in seconds")
    parser.add_argument("--quiet", action="store_true", help="Suppress ingestion lines")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    run(args.interval, args.quiet)


if __name__ == "__main__":
    main()
