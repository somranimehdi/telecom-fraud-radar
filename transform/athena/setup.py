"""Provision the Week 4 Glue/Athena catalog and register S3 partitions."""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import boto3
import yaml
DATABASE = "telecom_fraud_radar"
WORKGROUP = "fraud-radar-workgroup"
ALERTS_TABLE = "fraud_alerts"
PARTITIONED_TABLES = {
    "activation": ("silver/activation/", ("year", "month", "day")),
    "purchases": ("silver/bundles/purchases/", ("year", "month", "day")),
    "voice": ("silver/usage/voice/", ("year", "month", "day")),
    "data_sessions": ("silver/usage/data/", ("year", "month", "day")),
    "revenue": ("silver/revenue/prepaid/", ("year", "month", "day")),
    "account_daily_features": ("gold/account_daily_features/", ("year", "month", "day")),
    "subscriber_profiles": ("gold/reference/subscriber_profile/", ("snapshot_date",)),
    "bundle_catalog": ("gold/reference/bundle_catalog/", ("snapshot_date",)),
}


def _ensure_database(glue) -> None:
    try:
        glue.get_database(Name=DATABASE)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": DATABASE})


def _ensure_workgroup(athena, output_location: str) -> None:
    try:
        athena.get_work_group(WorkGroup=WORKGROUP)
    except athena.exceptions.ResourceNotFoundException:
        athena.create_work_group(
            Name=WORKGROUP,
            Configuration={
                "ResultConfiguration": {"OutputLocation": output_location},
                "EnforceWorkGroupConfiguration": True,
            },
            Description="Local investigation queries for Telecom Fraud Radar",
        )


def _ensure_alerts_table(dynamodb) -> None:
    try:
        dynamodb.describe_table(TableName=ALERTS_TABLE)
        return
    except dynamodb.exceptions.ResourceNotFoundException:
        pass
    try:
        dynamodb.create_table(
            TableName=ALERTS_TABLE,
            AttributeDefinitions=[
                {"AttributeName": "alert_id", "AttributeType": "S"},
                {"AttributeName": "subscriber_id", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "subscriber-index",
                "KeySchema": [{"AttributeName": "subscriber_id", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException:
        pass
    dynamodb.get_waiter("table_exists").wait(TableName=ALERTS_TABLE)


def _s3_partitions(s3, bucket: str, prefix: str, fields: tuple[str, ...]):
    paginator = s3.get_paginator("list_objects_v2")
    partitions: dict[tuple[str, ...], str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            key = obj["Key"]
            if not key.startswith(prefix):
                continue
            pieces = key[len(prefix):].split("/")[:-1]
            values: list[str] = []
            for field, piece in zip(fields, pieces):
                marker = field + "="
                if not piece.startswith(marker):
                    break
                values.append(piece[len(marker):])
            if len(values) == len(fields):
                partition_location = obj["Key"].rsplit("/", 1)[0] + "/"
                partitions[tuple(values)] = f"s3://{bucket}/{partition_location}"
    return partitions


def _run_query(
    athena,
    sql: str,
    output_location: str,
    verbose: bool = True,
    max_wait_seconds: int = 900,
    poll_interval_seconds: int = 5,
) -> None:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=WORKGROUP,
    )
    query_id = response["QueryExecutionId"]
    print(f"  Athena query {query_id}: submitted")
    started = time.monotonic()
    next_report = 30
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]["State"]
        if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        elapsed = time.monotonic() - started
        if elapsed >= max_wait_seconds:
            raise TimeoutError(
                f"Athena query {query_id} exceeded {max_wait_seconds}s; "
                f"status object: {execution.get('Status', {})}\nSQL: {sql}"
            )
        if elapsed >= next_report:
            print(f"  Athena query {query_id}: {status} after {int(elapsed)}s")
            next_report += 30
        time.sleep(min(poll_interval_seconds, max_wait_seconds - elapsed))
    if status != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "unknown Athena error")
        raise RuntimeError(f"Athena query {query_id} {status}: {reason}\nSQL: {sql}")
    if verbose:
        print(f"  Athena {query_id}: {status}")


def _create_tables(athena, output_location: str) -> None:
    sql_path = Path(__file__).with_name("tables.sql")
    sql = re.sub(r"(?m)^\s*--.*$", "", sql_path.read_text())
    # Refresh catalog definitions so schema corrections are picked up on rerun.
    # These are external tables; dropping them preserves all S3 Parquet data.
    for table in PARTITIONED_TABLES:
        _run_query(athena, f"DROP TABLE IF EXISTS {DATABASE}.{table}", output_location)
    for statement in (part.strip() for part in sql.split(";")):
        if statement:
            _run_query(athena, statement, output_location)


def _register_partitions(s3, athena, bucket: str, output_location: str) -> int:
    added = 0
    for table, (prefix, fields) in PARTITIONED_TABLES.items():
        partitions = _s3_partitions(s3, bucket, prefix, fields)
        for values, location in partitions.items():
            parts = ", ".join(
                f"{name}='{value}'" for name, value in zip(fields, values)
            )
            sql = (
                f"ALTER TABLE {DATABASE}.{table} ADD IF NOT EXISTS "
                f"PARTITION ({parts}) LOCATION '{location}'"
            )
            _run_query(athena, sql, output_location, verbose=False)
            added += 1
        print(f"  {table}: registered {len(partitions)} partition(s)")
    return added


def main() -> None:
    parser = argparse.ArgumentParser(description="Set up Athena/Glue and fraud_alerts in LocalStack.")
    parser.add_argument("--config", default="simulator/config.yaml")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()
    with Path(args.config).open() as handle:
        cfg = yaml.safe_load(handle)

    common = {
        "endpoint_url": args.endpoint,
        "region_name": cfg.get("region", "us-east-1"),
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    glue = boto3.client("glue", **common)
    athena = boto3.client("athena", **common)
    s3 = boto3.client("s3", **common)
    dynamodb = boto3.client("dynamodb", **common)
    bucket = cfg["paths"]["s3_bucket"]
    output_location = f"s3://{bucket}/athena-results/"

    print("Provisioning Week 4 resources")
    _ensure_database(glue)
    print(f"  Glue database: {DATABASE}")
    _ensure_workgroup(athena, output_location)
    print(f"  Athena workgroup: {WORKGROUP}")
    s3.put_object(Bucket=bucket, Key="athena-results/")
    _ensure_alerts_table(dynamodb)
    print(f"  DynamoDB table: {ALERTS_TABLE}")

    _create_tables(athena, output_location)
    _register_partitions(s3, athena, bucket, output_location)
    print("Week 4 catalog setup complete.")


if __name__ == "__main__":
    main()
