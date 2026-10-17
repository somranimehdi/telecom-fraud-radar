import io
from datetime import date, timedelta

import polars as pl

from quality_gate.gate import _check_source_freshness
from quality_gate.models import SourceReport
from simulator.run import simulation_start_date


class MemoryS3:
    def __init__(self, objects):
        self.objects = objects

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


def test_freshness_uses_newest_event_date_and_applies_max_age():
    frame = pl.DataFrame({"timestamp": [
        "2026-10-10T23:59:00+00:00", "2026-10-12T01:00:00+00:00",
        "2027-10-17T00:00:00+00:00", None,
    ]})
    buf = io.BytesIO()
    frame.write_parquet(buf)
    s3 = MemoryS3({
        # Deliberately misleading partition: freshness follows row event time.
        "raw/voice/year=2026/month=10/day=17/data.parquet": buf.getvalue()
    })

    assert _check_source_freshness(
        s3, "bucket", "raw/voice/", reference_date=date(2026, 10, 17), max_days_behind=5
    ) == (True, 5, 1, 1)
    assert _check_source_freshness(
        s3, "bucket", "raw/voice/", reference_date=date(2026, 10, 17), max_days_behind=4
    ) == (False, 5, 1, 1)


def test_end_yesterday_shifts_start_to_keep_configured_window_length():
    cfg = {"population": {"start_date": "2026-09-15", "days": 5}}
    start = simulation_start_date(cfg, end_yesterday=True, today=date(2026, 10, 17))
    assert start == date(2026, 10, 12)
    assert start + timedelta(days=cfg["population"]["days"] - 1) == date(2026, 10, 16)
    assert simulation_start_date(cfg) == date(2026, 9, 15)


def test_timestamp_quality_counts_are_serialized_in_source_metrics():
    report = SourceReport(source="voice", future_timestamp_rows=1, null_timestamp_rows=2)
    metrics = report.to_dict()
    assert metrics["future_timestamp_rows"] == 1
    assert metrics["null_timestamp_rows"] == 2
