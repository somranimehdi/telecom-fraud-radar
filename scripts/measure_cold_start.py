"""Measure cold and warm invoke latency for the LocalStack scoring Lambda."""

from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

import boto3


FUNCTION_NAME = "fraud-scoring"
BUCKET = "telco-fraud-bronze"
FEATURE_PREFIX = "gold/model_features/"
REGION = "us-east-1"


def _clients():
    common = {
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        "region_name": REGION,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return boto3.client("lambda", **common), boto3.client("s3", **common)


def latest_gold_day(s3) -> str:
    token = None
    days: list[str] = []
    pattern = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")
    while True:
        request = {"Bucket": BUCKET, "Prefix": FEATURE_PREFIX}
        if token:
            request["ContinuationToken"] = token
        result = s3.list_objects_v2(**request)
        for obj in result.get("Contents", []):
            match = pattern.search(obj["Key"])
            if match:
                days.append("-".join(match.groups()))
        token = result.get("NextContinuationToken")
        if not token:
            break
    if not days:
        raise RuntimeError(f"No daily feature objects found under s3://{BUCKET}/{FEATURE_PREFIX}")
    return max(days)


def _wait_for_update(lambda_client) -> None:
    for _ in range(60):
        config = lambda_client.get_function_configuration(FunctionName=FUNCTION_NAME)
        status = config.get("LastUpdateStatus", "Successful")
        if status == "Successful":
            return
        if status == "Failed":
            raise RuntimeError(f"Lambda configuration update failed: {config}")
        time.sleep(1)
    raise TimeoutError("Timed out waiting for the Lambda environment update")


def _invoke(lambda_client, payload: bytes) -> tuple[float, str]:
    start = time.perf_counter()
    try:
        response = lambda_client.invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=payload,
        )
    except Exception as exc:
        error_body = getattr(exc, "response", None)
        print(f"Lambda invocation failed: {exc}", file=sys.stderr)
        if error_body is not None:
            print(json.dumps(error_body, indent=2, default=str), file=sys.stderr)
        raise
    elapsed_ms = (time.perf_counter() - start) * 1000
    body = response.get("Payload").read().decode("utf-8", errors="replace")
    if response.get("FunctionError") or response.get("StatusCode") != 200:
        print("Lambda invocation returned an error payload:", file=sys.stderr)
        print(body, file=sys.stderr)
        raise RuntimeError(f"Lambda invoke failed: {body}")
    return elapsed_ms, body


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def measure() -> str:
    lambda_client, s3 = _clients()
    day = latest_gold_day(s3)
    current = lambda_client.get_function_configuration(FunctionName=FUNCTION_NAME)
    variables = dict(current.get("Environment", {}).get("Variables", {}))
    variables["COLD_START_MARKER"] = str(uuid.uuid4())
    lambda_client.update_function_configuration(
        FunctionName=FUNCTION_NAME,
        Environment={"Variables": variables},
    )
    _wait_for_update(lambda_client)
    time.sleep(3)

    payload = json.dumps({"date": day, "dry_run": True}).encode("utf-8")
    measurements = []
    duration, _response = _invoke(lambda_client, payload)
    measurements.append(("cold_1", "Cold", duration))
    for index in range(1, 6):
        duration, _response = _invoke(lambda_client, payload)
        measurements.append((f"warm_{index}", "Warm", duration))

    warm = [duration for _name, kind, duration in measurements if kind == "Warm"]
    warm_mean = sum(warm) / len(warm)
    warm_p95 = _percentile(warm, 0.95)
    cold = measurements[0][2]
    speedup = cold / warm_mean if warm_mean else float("inf")
    lines = [
        "# Scoring Lambda cold-start measurement", "",
        "| Run | Type | Duration (ms) |", "|---|---:|---:|",
    ]
    lines.extend(f"| {name} | {kind} | {duration:.2f} |" for name, kind, duration in measurements)
    lines.extend([
        "", f"- Cold start: {cold:.2f} ms",
        f"- Warm mean: {warm_mean:.2f} ms",
        f"- Warm p95: {warm_p95:.2f} ms",
        f"- Speedup: {speedup:.2f}x",
        "", "Measured against LocalStack Lambda, Python 3.13 container, model size 644 KB loaded from S3 at cold start.", "",
    ])
    report = "\n".join(lines)
    output = Path(__file__).resolve().parents[1] / "model" / "out" / "cold_start.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")
    print(report)
    print(f"Wrote {output}")
    return report


def main() -> None:
    try:
        measure()
    except Exception as exc:
        # Invocation failures already include the complete Lambda response body.
        print(f"Cold-start measurement failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
