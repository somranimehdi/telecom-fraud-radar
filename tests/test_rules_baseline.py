from evaluation.rules_baseline import evaluate_rules


def test_evaluate_rules_uses_accounts_for_precision_recall_and_alert_ratio():
    labels = [
        {"subscriber_id": "a", "is_fraud": 1, "archetype": "irsf"},
        {"subscriber_id": "b", "is_fraud": 1, "archetype": "irsf"},
        {"subscriber_id": "c", "is_fraud": 0, "archetype": "none"},
        {"subscriber_id": "d", "is_fraud": 1, "archetype": "bundle_abuse"},
    ]
    alerts = [
        {"subscriber_id": "a", "rule_id": "IRSF_HIGH_RISK_BURST"},
        {"subscriber_id": "a", "rule_id": "IRSF_HIGH_RISK_BURST"},
        {"subscriber_id": "c", "rule_id": "IRSF_HIGH_RISK_BURST"},
        {"subscriber_id": "d", "rule_id": "REPEATED_BUNDLE_PURCHASES"},
    ]

    result = evaluate_rules(alerts, labels)

    irsf = result["rules"]["IRSF_HIGH_RISK_BURST"]
    assert irsf == {
        "alerts": 3,
        "unique_accounts_flagged": 2,
        "precision_matching_archetype": 0.5,
        "recall_matching_archetype": 0.5,
        "any_fraud_precision": 0.5,
        "alerts_per_flagged_account": 1.5,
    }
    assert result["overall_share_subscribers_flagged"] == 0.75
    assert result["rules"]["SIM_SWAP_INTL_24H"]["recall_matching_archetype"] == 0.0
