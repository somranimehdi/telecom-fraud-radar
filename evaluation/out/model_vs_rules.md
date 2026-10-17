# Model vs Rules

## Training stability and row-level test metrics

TRAIN 3-fold GroupKFold PR-AUC: 0.5939, 0.6072, 0.5009 (mean 0.5673).
Ablation GroupKFold PR-AUC: 0.3922, 0.3770, 0.3856 (mean 0.3849).
Dropped constant feature(s): failed_payment_count.

| Test cohort | Positive subscribers | ROC-AUC | PR-AUC |
|---|---|---|---|
| Overall | all test fraud accounts | 0.9004 | 0.6903 |
| sim_swap_takeover | 22 | 0.9440 | 0.8009 |
| irsf | 19 | 0.8092 | 0.2889 |
| subscription_fraud | 13 | 0.9900 | 0.6666 |
| bundle_abuse | 6 | 0.6865 | 0.0220 |

## Ablation row-level test metrics

| Test cohort | Positive subscribers | ROC-AUC | PR-AUC |
|---|---|---|---|
| Overall | all test fraud accounts | 0.7992 | 0.4510 |
| sim_swap_takeover | 22 | 0.6806 | 0.1033 |
| irsf | 19 | 0.8294 | 0.4807 |
| subscription_fraud | 13 | 0.9822 | 0.8206 |
| bundle_abuse | 6 | 0.7019 | 0.1065 |

## Account-level test comparison

| Method | Accounts flagged | Precision | Recall | Precision 95% CI | Recall 95% CI |
|---|---|---|---|---|---|
| Rules | 236 | 0.1907 | 0.7500 | 0.1461–0.2335 | 0.6342–0.8741 |
| Model at rules alert budget | 236 | 0.2373 | 0.9333 | 0.1814–0.3002 | 0.8543–1.0000 |
| Rules + model top 25% | 59 | 0.7458 | 0.7333 | 0.5470–0.9040 | 0.6171–0.8468 |
| Rules + model top 50% | 118 | 0.3729 | 0.7333 | 0.2781–0.4640 | 0.6171–0.8468 |

### Per-archetype account recall

| Archetype | Positive test subscribers | Rules recall | Model recall | Ablation model recall |
|---|---|---|---|---|
| sim_swap_takeover | 22 | 0.8182 | 0.9091 | 0.4545 |
| irsf | 19 | 0.8947 | 0.9474 | 1.0000 |
| subscription_fraud | 13 | 0.4615 | 1.0000 | 1.0000 |
| bundle_abuse | 6 | 0.6667 | 0.8333 | 0.6667 |

### Per-rule account precision

| Rule | Accounts flagged | Precision | Any-fraud precision |
|---|---|---|---|
| IRSF_HIGH_RISK_BURST | 196 | 0.0867 | 0.1429 |
| NEW_ACCOUNT_HIGH_VALUE_PURCHASE | 23 | 0.0435 | 0.0435 |
| REPEATED_BUNDLE_PURCHASES | 5 | 0.8000 | 0.8000 |
| SIM_SWAP_INTL_24H | 21 | 0.8571 | 0.8571 |

### Precision@K

| Top share of test accounts | Accounts flagged | Precision | Recall |
|---|---|---|---|
| 1% | 30 | 0.9333 | 0.4667 |
| 2% | 60 | 0.8500 | 0.8500 |
| 5% | 150 | 0.3667 | 0.9167 |

### Ablation comparison

| Measure | Main model | Ablated model | Difference (ablated − main) |
|---|---|---|---|
| Precision at rules budget | 0.2373 | 0.1949 | -0.0424 |
| Recall at rules budget | 0.9333 | 0.7667 | -0.1667 |
| Rules + model top 25% precision | 0.7458 | 0.6102 | -0.1356 |
| Rules + model top 25% recall | 0.7333 | 0.6000 | -0.1333 |
| Rules + model top 50% precision | 0.3729 | 0.3475 | -0.0254 |
| Rules + model top 50% recall | 0.7333 | 0.6833 | -0.0500 |

### Main-model permutation importance (average precision decrease)

| Rank | Feature | Importance |
|---|---|---|
| 1 | hours_since_last_swap | 0.520716 |
| 2 | high_risk_destination_call_count_7d_sum | 0.289285 |
| 3 | session_count | 0.176539 |
| 4 | total_cost_7d_sum | 0.166374 |
| 5 | lines_per_identity | 0.080503 |
| 6 | total_cost_3d_sum | 0.050370 |
| 7 | purchase_count_7d_sum | 0.026772 |
| 8 | night_call_count | 0.019433 |
| 9 | cost | 0.016881 |
| 10 | intl_call_count | 0.016169 |

Results come from simulated data with injected patterns. SIM swap is easy because `hours_since_last_swap` is near-perfectly informative. Bundle abuse (6 test accounts) and subscription fraud (13) are small samples.
