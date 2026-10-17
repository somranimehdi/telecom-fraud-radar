from pathlib import Path

import yaml

from datetime import date

from simulator.run import apply_effective_window, write_effective_config_snapshot


def test_effective_config_snapshot_records_window_start_days_and_end(tmp_path: Path):
    config = {
        "population": {"start_date": "2026-09-15", "days": 5},
    }
    apply_effective_window(config, date(2026, 10, 12))
    path = write_effective_config_snapshot(config, tmp_path / "config.used.yaml")
    saved = yaml.safe_load(path.read_text())
    assert saved["population"]["start_date"] == "2026-10-12"
    assert saved["population"]["days"] == 5
    assert saved["window_end_date"] == "2026-10-16"
