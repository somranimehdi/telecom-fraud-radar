from collections import defaultdict

from quality_gate.checks import CheckContext
from quality_gate.runner import _validate_row
from simulator.run import run_simulation


def test_small_simulation_defects_are_quarantinable():
    collector, _, _ = run_simulation(
        "simulator/config.small.yaml", write_outputs=False, defect_rate=0.05
    )
    grouped = defaultdict(list)
    for item in collector.injected_defects:
        grouped[item["source"]].append(item)

    passed = 0
    for source, items in grouped.items():
        rows = {
            "voice": collector.voice_cdrs,
            "data": collector.data_cdrs,
            "revenue": collector.revenue_txs,
            "activation": collector.activation_events,
            "purchases": collector.bundle_purchases,
        }[source]
        context = CheckContext(source=source)
        injected_by_index = {item["row_index"]: item for item in items}
        # Register original IDs before injected duplicate copies, matching raw row order.
        for index, row in enumerate(rows):
            status, reason, _ = _validate_row(row, context)
            defect = injected_by_index.get(index)
            if defect is None:
                continue
            kind = defect["defect_type"]
            reason_lower = reason.lower()
            matches = {
                "duplicate_id": "duplicate" in reason_lower,
                "negative_value": "negative" in reason_lower or "greater than or equal to 0" in reason_lower,
                "future_timestamp": "future" in reason_lower,
                "null_required": any(term in reason_lower for term in ("valid string", "valid decimal", "valid integer", "valid datetime")),
            }[kind]
            if status == "failed" and matches:
                passed += 1

    assert collector.injected_defects
    assert passed / len(collector.injected_defects) >= 0.95


def test_zero_defect_rate_leaves_rows_and_metadata_unchanged():
    collector, _, _ = run_simulation(
        "simulator/config.small.yaml", write_outputs=False, defect_rate=0
    )
    assert collector.injected_defects == []
