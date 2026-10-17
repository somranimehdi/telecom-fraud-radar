Generated from simulated data; snapshot copied on 2026-10-17

# Shortcut check

## Overall TRAIN split

| Feature | Direction-independent AUC | Flag |
|---|---|---|
| intl_call_count_7d_sum | 0.6865 | — |
| hours_since_last_swap | 0.6858 | — |
| high_risk_destination_call_count_7d_sum | 0.6854 | — |
| total_cost_7d_sum | 0.6576 | — |
| high_risk_destination_call_count_3d_sum | 0.5948 | — |
| intl_call_count_3d_sum | 0.5903 | — |
| total_cost_3d_sum | 0.5678 | — |
| purchase_count_7d_sum | 0.5662 | — |
| intl_call_count | 0.5545 | — |
| high_risk_destination_call_count | 0.5530 | — |
| night_call_count | 0.5416 | — |
| distinct_destination_countries | 0.5353 | — |
| account_age_days | 0.5330 | — |
| total_cost_to_prior_7d_avg_ratio | 0.5252 | — |
| purchase_count_3d_sum | 0.5245 | — |
| session_count | 0.5241 | — |
| total_cost | 0.5237 | — |
| net_amount | 0.5144 | — |
| bytes | 0.5135 | — |
| transaction_count | 0.5130 | — |
| cost | 0.5128 | — |
| sim_swap_count | 0.5123 | — |
| activation_event_count | 0.5123 | — |
| total_duration_sec | 0.5116 | — |
| lines_per_identity | 0.5088 | — |
| segment | 0.5085 | — |
| call_count | 0.5083 | — |
| purchase_count | 0.5071 | — |
| distinct_bundles | 0.5069 | — |
| short_call_count | 0.5064 | — |
| spend | 0.5053 | — |
| avg_call_duration | 0.5044 | — |
| kyc_status | 0.5042 | — |
| max_calls_in_any_hour | 0.5039 | — |
| risk_score_baseline | 0.5034 | — |
| failed_payment_count | 0.5000 | — |

## Per-archetype top features

### bundle_abuse

| Feature | Direction-independent AUC | Flag |
|---|---|---|
| purchase_count_7d_sum | 0.6958 | — |
| lines_per_identity | 0.5934 | — |
| risk_score_baseline | 0.5882 | — |
| purchase_count_3d_sum | 0.5798 | — |
| segment | 0.5678 | — |
| account_age_days | 0.5586 | — |
| total_cost_3d_sum | 0.5521 | — |
| intl_call_count_7d_sum | 0.5484 | — |
| total_duration_sec | 0.5448 | — |
| max_calls_in_any_hour | 0.5417 | — |

### irsf

| Feature | Direction-independent AUC | Flag |
|---|---|---|
| high_risk_destination_call_count_7d_sum | 0.7145 | — |
| intl_call_count_7d_sum | 0.7025 | — |
| total_cost_7d_sum | 0.6853 | — |
| total_cost_to_prior_7d_avg_ratio | 0.5907 | — |
| high_risk_destination_call_count_3d_sum | 0.5745 | — |
| intl_call_count_3d_sum | 0.5729 | — |
| total_cost_3d_sum | 0.5621 | — |
| lines_per_identity | 0.5546 | — |
| account_age_days | 0.5524 | — |
| purchase_count_7d_sum | 0.5380 | — |

### sim_swap_takeover

| Feature | Direction-independent AUC | Flag |
|---|---|---|
| hours_since_last_swap | 0.9995 | SHORTCUT |
| intl_call_count_7d_sum | 0.6398 | — |
| high_risk_destination_call_count_7d_sum | 0.6315 | — |
| total_cost_7d_sum | 0.6093 | — |
| account_age_days | 0.5553 | — |
| risk_score_baseline | 0.5552 | — |
| high_risk_destination_call_count_3d_sum | 0.5391 | — |
| intl_call_count_3d_sum | 0.5365 | — |
| sim_swap_count | 0.5331 | — |
| activation_event_count | 0.5330 | — |

### subscription_fraud

| Feature | Direction-independent AUC | Flag |
|---|---|---|
| intl_call_count_7d_sum | 0.8363 | — |
| high_risk_destination_call_count_7d_sum | 0.8318 | — |
| total_cost_7d_sum | 0.8161 | — |
| high_risk_destination_call_count_3d_sum | 0.7886 | — |
| intl_call_count_3d_sum | 0.7774 | — |
| intl_call_count | 0.7474 | — |
| total_cost_3d_sum | 0.7434 | — |
| high_risk_destination_call_count | 0.7419 | — |
| night_call_count | 0.6997 | — |
| purchase_count_7d_sum | 0.6569 | — |

## Mean subscriber-level profile values

| Archetype | Subscribers | risk_score_baseline | kyc_status | account_age_days | lines_per_identity |
|---|---|---|---|---|---|
| bundle_abuse | 15 | 1241.6000 | 2.5333 | 573.7333 | 1.4667 |
| irsf | 44 | 1259.2500 | 2.5909 | 541.8636 | 3.3864 |
| legit | 6860 | 1299.4880 | 2.5397 | 627.7847 | 2.6875 |
| sim_swap_takeover | 50 | 1426.1200 | 2.6000 | 461.7800 | 1.7800 |
| subscription_fraud | 31 | 1173.3871 | 2.6452 | 492.6129 | 2.0323 |

All results are from simulated data.
