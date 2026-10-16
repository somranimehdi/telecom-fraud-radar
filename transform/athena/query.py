"""Run a saved Athena investigation query and print its result rows."""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import boto3
import yaml

from transform.athena.setup import DATABASE, WORKGROUP


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one SQL file against LocalStack Athena.")
    parser.add_argument("--sql-file", required=True)
    parser.add_argument("--config", default="simulator/config.yaml")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()
    with Path(args.config).open() as handle:
        cfg = yaml.safe_load(handle)
    sql = re.sub(r"(?m)^\s*--.*$", "", Path(args.sql_file).read_text()).strip()

    athena = boto3.client(
        "athena",
        endpoint_url=args.endpoint,
        region_name=cfg.get("region", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": f"s3://{cfg['paths']['s3_bucket']}/athena-results/"},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        details = execution["Status"]
        reason = details.get("StateChangeReason") or details.get("AthenaError") or f"Athena query {state}"
        raise SystemExit(f"Athena query {query_id} {state}: {reason}\nSQL: {sql}")

    print(f"Athena query {query_id}: {state}")
    token = None
    printed_header = False
    while True:
        request = {"QueryExecutionId": query_id, "MaxResults": 1000}
        if token:
            request["NextToken"] = token
        response = athena.get_query_results(**request)
        rows = response.get("ResultSet", {}).get("Rows", [])
        if not printed_header and rows:
            print("\t".join(col.get("VarCharValue", "") for col in rows[0].get("Data", [])))
            rows = rows[1:]
            printed_header = True
        for row in rows:
            print("\t".join(col.get("VarCharValue", "") for col in row.get("Data", [])))
        token = response.get("NextToken")
        if not token:
            break


if __name__ == "__main__":
    main()
