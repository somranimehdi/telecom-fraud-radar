"""
Quality Gate — CLI Entry Point
--------------------------------
Usage:
    python -m quality_gate.run
    python -m quality_gate.run --sources voice,data
    python -m quality_gate.run --config simulator/config.small.yaml

Prints a gate run summary. Designed to be invoked by the Makefile
target `make quality`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import boto3
import yaml

from quality_gate.gate import DEFAULT_SOURCES, run_gate


# ─── Argument Parsing ───────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the quality gate.")
    p.add_argument(
        "--config",
        default="simulator/config.yaml",
        help="Path to config.yaml",
    )
    p.add_argument(
        "--sources",
        default=",".join(DEFAULT_SOURCES),
        help="Comma-separated list of sources (default: all)",
    )
    p.add_argument(
        "--endpoint",
        default="http://localhost:4566",
        help="AWS endpoint URL (LocalStack by default)",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-source progress output",
    )
    return p.parse_args()


# ─── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        raise SystemExit(f"config not found: {config_path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    client_kwargs = {
        "endpoint_url": args.endpoint,
        "region_name": cfg.get("region", "us-east-1"),
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    s3 = boto3.client("s3", **client_kwargs)
    sns = boto3.client("sns", **client_kwargs)
    dynamodb = boto3.resource("dynamodb", **client_kwargs)

    report = run_gate(
        s3=s3,
        sns=sns,
        cfg=cfg,
        dynamodb=dynamodb,
        sources=sources,
        verbose=not args.quiet,
    )

    # Exit code: 0 if all sources in threshold, 1 if any over
    if report.sources_over_threshold():
        raise SystemExit(1)


if __name__ == "__main__":
    main()