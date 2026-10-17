from datetime import datetime, timezone

import pytest

from simulator.run import _parse_args, verify_s3_outputs


class ListingS3:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, _operation):
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [
            {"Key": key, "Size": size, "LastModified": modified}
            for key, (size, modified) in self.objects.items()
            if key.startswith(Prefix)
        ]}


def test_simulator_cli_parses_config_defect_rate_and_window_flags():
    args = _parse_args([
        "--config", "simulator/config.small.yaml", "--defect-rate", "0",
        "--end-yesterday",
    ])
    assert args.config == "simulator/config.small.yaml"
    assert args.defect_rate == 0
    assert args.end_yesterday


def test_s3_verification_reports_counts_and_requires_current_run_output():
    modified = datetime(2026, 10, 17, tzinfo=timezone.utc)
    s3 = ListingS3({
        "raw/usage/voice/year=2026/voice.parquet": (100, modified),
        "raw/usage/voice/empty/": (0, None),
    })
    result = verify_s3_outputs(
        s3, "bucket", {"s3_voice": {"2026-10-17": "raw/usage/voice/year=2026/voice.parquet"}}, True
    )
    assert result["voice"] == {"count": 1, "newest_last_modified": modified}

    with pytest.raises(SystemExit, match="wrote no raw objects"):
        verify_s3_outputs(s3, "bucket", {}, True)
