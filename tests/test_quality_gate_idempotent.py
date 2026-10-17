import io

import polars as pl

from quality_gate.runner import validate_source


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


def test_gate_rerun_overwrites_deterministic_event_date_files():
    s3 = MemoryS3()
    raw = pl.DataFrame({
        "call_id": ["c1", "c2", "c2"],
        "subscriber_id": ["s1", "s2", "s2"],
        "destination_number": ["+21612345678"] * 3,
        "duration_sec": [30, 45, 45],
        "cost": [1.5, 2.0, 2.0],
        "timestamp": ["2026-09-15T12:00:00+00:00", "2026-09-16T12:00:00+00:00", "2026-09-16T12:00:00+00:00"],
        "country_code": ["TN"] * 3,
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
    assert first.failed_rows == second.failed_rows == 1
    assert first_keys == second_keys
    assert first_counts == second_counts
    assert len(first_keys) == 3
    assert all("year=2026/month=09/day=1" in key for key in first_keys)
