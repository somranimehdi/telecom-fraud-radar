"""Evaluate persisted fraud rule alerts against simulator-only labels."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import boto3
import polars as pl
import yaml


RULE_ARCHETYPES = {
    "SIM_SWAP_INTL_24H": "sim_swap_takeover",
    "IRSF_HIGH_RISK_BURST": "irsf",
    "NEW_ACCOUNT_HIGH_VALUE_PURCHASE": "subscription_fraud",
    "REPEATED_BUNDLE_PURCHASES": "bundle_abuse",
}


def evaluate_rules(alerts: Iterable[dict], labels: Iterable[dict]) -> dict:
    """Return rule metrics using distinct subscribers as the evaluation unit."""
    alert_rows = list(alerts)
    label_by_id = {str(row["subscriber_id"]): row for row in labels}
    by_rule: dict[str, list[dict]] = {rule_id: [] for rule_id in RULE_ARCHETYPES}
    for alert in alert_rows:
        by_rule.setdefault(str(alert.get("rule_id", "")), []).append(alert)

    metrics = {}
    for rule_id, rule_alerts in sorted(by_rule.items()):
        flagged = {str(a.get("subscriber_id", "")) for a in rule_alerts if a.get("subscriber_id") is not None}
        archetype = RULE_ARCHETYPES.get(rule_id)
        matching_flagged = {
            sid for sid in flagged
            if sid in label_by_id and str(label_by_id[sid].get("archetype", "")) == archetype
        }
        archetype_accounts = {
            sid for sid, label in label_by_id.items()
            if str(label.get("archetype", "")) == archetype
        } if archetype else set()
        fraud_flagged = {
            sid for sid in flagged
            if sid in label_by_id and int(label_by_id[sid].get("is_fraud", 0)) == 1
        }
        metrics[rule_id] = {
            "alerts": len(rule_alerts),
            "unique_accounts_flagged": len(flagged),
            "precision_matching_archetype": len(matching_flagged) / len(flagged) if flagged else 0.0,
            "recall_matching_archetype": len(matching_flagged) / len(archetype_accounts) if archetype_accounts else 0.0,
            "any_fraud_precision": len(fraud_flagged) / len(flagged) if flagged else 0.0,
            "alerts_per_flagged_account": len(rule_alerts) / len(flagged) if flagged else 0.0,
        }

    all_flagged = {
        str(a["subscriber_id"]) for a in alert_rows if a.get("subscriber_id") is not None
    }
    return {
        "rules": metrics,
        "overall_share_subscribers_flagged": len(all_flagged) / len(label_by_id) if label_by_id else 0.0,
        "subscriber_count": len(label_by_id),
        "unique_subscribers_flagged": len(all_flagged),
    }


def _scan(table) -> list[dict]:
    items: list[dict] = []
    response = table.scan()
    while True:
        items.extend(response.get("Items", []))
        key = response.get("LastEvaluatedKey")
        if not key:
            return items
        response = table.scan(ExclusiveStartKey=key)


def _render_markdown(result: dict) -> str:
    columns = ["alerts", "unique_accounts_flagged", "precision_matching_archetype",
               "recall_matching_archetype", "any_fraud_precision", "alerts_per_flagged_account"]
    lines = ["# Rules baseline", "", "| Rule | " + " | ".join(columns) + " |",
             "|---|" + "|".join(["---:"] * len(columns)) + "|"]
    for rule_id, values in result["rules"].items():
        cells = [str(values[col]) if col in ("alerts", "unique_accounts_flagged")
                 else f"{values[col]:.4f}" for col in columns]
        lines.append("| " + rule_id + " | " + " | ".join(cells) + " |")
    lines.extend(["", f"Overall share of subscribers flagged by any rule: {result['overall_share_subscribers_flagged']:.4f}.",
                  "Results are from simulated data with injected patterns.", ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate fraud_alerts against simulator labels.")
    parser.add_argument("--config", default="simulator/config.yaml")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--labels", default="simulator/out/labels.parquet")
    parser.add_argument("--out", default="evaluation/out")
    args = parser.parse_args()
    with Path(args.config).open() as handle:
        cfg = yaml.safe_load(handle)
    ddb = boto3.resource("dynamodb", endpoint_url=args.endpoint, region_name="us-east-1",
                         aws_access_key_id="test", aws_secret_access_key="test")
    alerts = _scan(ddb.Table("fraud_alerts"))
    labels = pl.read_parquet(args.labels).to_dicts()
    result = evaluate_rules(alerts, labels)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "rules_baseline.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (out_dir / "rules_baseline.md").write_text(_render_markdown(result))
    print(f"Wrote rules baseline for {result['subscriber_count']:,} subscribers to {out_dir}")


if __name__ == "__main__":
    main()
