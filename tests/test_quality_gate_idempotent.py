import io

import polars as pl

from quality_gate.runner import validate_source
from transform.athena.setup import _s3_partitions


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def get_paginator(self, _operation):
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [
            {"Key": key, "Size": len(body)}
            for key, body in self.objects.items()
            if key.startswith(Prefix)
        ]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[Key] = Body


def test_athena_partition_discovery_accepts_unknown_quarantine_partition():
    s3 = MemoryS3()
    s3.objects["quarantine/usage/voice/year=unknown/month=unknown/day=unknown/quarantine.parquet"] = b"parquet"
    partitions = _s3_partitions(
        s3, "bucket", "quarantine/usage/voice/", ("year", "month", "day")
    )
    assert ("unknown", "unknown", "unknown") in partitions


def test_gate_rerun_overwrites_deterministic_event_date_files():
    s3 = MemoryS3()
    raw = pl.DataFrame({
        "call_id": ["c1", "c2", "c2", "c3", "c4"],
        "subscriber_id": ["s1", "s2", "s2", "s3", "s4"],
        "destination_number": ["+21612345678"] * 5,
        "duration_sec": [30, 45, 45, 60, 75],
        "cost": [1.5, 2.0, 2.0, 2.5, 3.0],
        "timestamp": ["2026-09-15T12:00:00+00:00", "2026-09-16T12:00:00+00:00", "2026-09-16T12:00:00+00:00", None, "2030-09-17T12:00:00+00:00"],
        "country_code": ["TN"] * 5,
    })
    buffer = io.BytesIO()
    raw.write_parquet(buffer)
    s3.objects["raw/usage/voice/input.parquet"] = buffer.getvalue()

    first = validate_source(s3, "bucket", "voice", "gate_first", verbose=False)
    first_keys = sorted(k for k in s3.objects if k.startswith(("silver/usage/voice/", "quarantine/usage/voice/")))
    first_counts = {
        key: pl.read_parquet(io.BytesIO(s3.objects[key])).height for key in first_keys
    }
    second = validate_source(s3, "bucket", "voice", "gate_second", verbose=False)
    second_keys = sorted(k for k in s3.objects if k.startswith(("silver/usage/voice/", "quarantine/usage/voice/")))
    second_counts = {
        key: pl.read_parquet(io.BytesIO(s3.objects[key])).height for key in second_keys
    }

    assert first.passed_rows == second.passed_rows == 2
    assert first.failed_rows == second.failed_rows == 3
    assert first_keys == second_keys
    assert first_counts == second_counts
    assert len(first_keys) == 4
    assert any("year=unknown/month=unknown/day=unknown/quarantine.parquet" in key for key in first_keys)
    assert all(first_counts[key] == second_counts[key] for key in first_keys)
    unknown_key = next(key for key in first_keys if "year=unknown" in key)
    assert first_counts[unknown_key] == 2
