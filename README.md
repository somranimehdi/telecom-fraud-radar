# Telecom Fraud Radar

An AWS-first fraud detection pipeline for telecom, running on **LocalStack**.

> **Honest notice**
> - All data is simulated. No real subscriber, call, revenue, or KYC data is
>   used. The simulator injects fraud patterns to provide labels.
> - The services run on LocalStack, not AWS.
> - Results come from simulated data with injected patterns and are optimistic.

<!-- TODO: demo video / GIF and dashboard screenshots go here -->

## Results

The account-level test comparison below uses the same alert budget for rules
and model. The combined rows use model re-ranking of the rule candidates.

| Method | Accounts flagged | Precision (95% CI) | Recall (95% CI) |
|---|---:|---:|---:|
| Rules alone | 236 | 0.1907 (0.1461–0.2335) | 0.7500 (0.6342–0.8741) |
| Model at rules alert budget | 236 | 0.2373 (0.1814–0.3002) | 0.9333 (0.8543–1.0000) |
| Rules + model, top quarter | 59 | 0.7458 (0.5470–0.9040) | 0.7333 (0.6171–0.8468) |
| Ablated model at rules alert budget | 236 | 0.1949 (CI not reported) | 0.7667 (CI not reported) |

The test set contains 3,000 subscriber accounts, including 60 fraud accounts:
22 SIM-swap takeover, 19 IRSF, 13 subscription-fraud, and 6 bundle-abuse
accounts. These results and intervals are from the saved synthetic-data
comparison; see [model vs rules report](docs/reports/model_vs_rules.md).

There is no temporal holdout: training and test subscribers are split within
the same 30-day window. SIM swap is easy in this simulation because it has a
planted signal in `hours_since_last_swap`.

## What It Does

A telecom operator receives events from multiple systems. This project
validates those data, builds daily subscriber features, scores accounts, and
writes candidate fraud alerts. The patterns represented are SIM-swap
takeover, international revenue share fraud (IRSF), subscription fraud, and
bundle abuse.

## Architecture

```text
Synthetic batch sources ───────────────► S3 Bronze ───────┐
                                                          │
Synthetic activation / purchase events                    │
       └──► Kinesis ──► polling consumer ──► S3 Bronze ───┤
                    scripts/live_ingest.py                 │
                                                          ▼
                                             Python quality gate
                                             ├──► S3 Silver
                                             ├──► S3 Quarantine
                                             └──► S3 quality metrics
                                                   └──► SNS alert
                                                          │
S3 Silver ──► Gold account-day features ──► Glue Data Catalog / Athena
                         │                                │
                         ├──► rules CLI ──────────────────┤
                         └──► model training / daily scoring CLI
                                                          │
                                       DynamoDB fraud_alerts ◄──┘
                                             ├──► SNS scoring summary
                                             └──► Streamlit dashboard

Written but not deployed: activation and purchase Lambda handlers;
                           scoring Lambda provisioning reference script.
```

The ingestion Lambda handlers exist under `ingest/`, but their deployment is
not part of this iteration. The live demo reads Kinesis with a polling
consumer. Scoring runs as the `scoring/score_day.py` CLI and has a Dockerfile;
it is not a deployed Lambda.

**Medallion layers:**
- **Bronze** (`raw/`) — raw events and files, treated as immutable by
  convention; simulator reruns overwrite files with the same keys, and no
  object lock or versioning is enforced.
- **Silver** (`silver/`) — validated, cleaned, deduplicated records.
- **Gold** (`gold/`) — curated Parquet features, queryable with Athena.
- **Quarantine** (`quarantine/`) — failed rows with a reason column.

## Sources

| # | Source | Transport / status |
|---|---|---|
| 1 | SIM activation | Kinesis to polling consumer to S3 in the live demo. Lambda handler is written but not deployed. |
| 2 | Revenue (prepaid) | Batch files in S3. |
| 3 | Usage (voice and data CDRs) | Batch files in S3. |
| 4 | Bundle catalog | DynamoDB reference table. |
| 5 | Bundle purchases | Kinesis to polling consumer to S3 in the live demo. Lambda handler is written but not deployed. |
| 6 | Bundle usage | Not implemented. |
| 7 | Churn / customer state | Not implemented. |
| 8 | Subscriber profile | DynamoDB reference table. |

Postpaid revenue is not generated.

## What is implemented and what is not

| Implemented and exercised in local workflows | Not implemented or not deployed |
|---|---|
| S3, Kinesis, DynamoDB, SNS, Glue Data Catalog, and Athena through LocalStack workflows. | Lambda deployment; Glue ETL jobs (the gate runs as Python and has a Glue-compatible wrapper); Step Functions and EventBridge. |
| Python quality gate, simulator, Silver/Gold transforms, rule candidate generation, model training, and CLI scoring. | Postpaid revenue, bundle usage, and churn/customer-state inputs. |
| Streamlit dashboard and polling-based Kinesis live-demo consumer. | PII hashing, IAM least privilege, and S3 retention policies. |

## Data Quality Gate

The Python gate checks source schemas, duplicate IDs, ranges, referential
integrity against subscriber and bundle reference tables, and source
freshness. It writes failed rows to `quarantine/` with a reason and writes run
metrics to S3. The code publishes an SNS alert when any source's quarantine
rate exceeds its configured threshold **or** any source is stale. SNS
publishing is best-effort: failures are logged and do not stop the gate. The
saved quality snapshot has no sources over threshold and does not demonstrate
an SNS alert; alert delivery has not yet been verified end-to-end in a test
run.

## Data Governance

The project includes data contracts, quality checks, lineage across Bronze,
Silver, and Gold, and quarantine reasons. LocalStack persistence is enabled
in `docker-compose.yml`.

Not implemented:
- IAM least-privilege roles.
- S3 lifecycle or retention policies.
- PII hashing at ingestion (`msisdn` and `imsi` are stored in clear text).
- Data catalog ownership tags.

## Stack

| Layer | Tools used by the project |
|---|---|
| Cloud emulation and containers | LocalStack, Docker, Docker Compose |
| Language and contracts | Python, Pydantic |
| Data and file formats | Polars, PyArrow |
| AWS clients | boto3, awslocal |
| Machine learning and artifacts | scikit-learn, joblib |
| Dashboard | Streamlit |
| Tests | pytest |
| Configuration | PyYAML |

## Repository Layout

```text
telecom-fraud-radar/
├── contracts/          Source data contracts
├── dashboard/          Streamlit app and JSON export
├── docs/reports/       Copied evaluation reports
├── evaluation/         Rules baseline and model comparison
├── infra/              LocalStack provisioning and reference scripts
├── ingest/              Activation and purchase Lambda handlers
├── model/               Features, training, metrics, model publication
├── quality_gate/        Python validation, quarantine, metrics, Glue wrapper
├── scoring/             Rules and daily model scoring CLI
├── scripts/             Live producer, polling consumer, loop, utilities
├── simulator/           Synthetic batch and streaming data generators
├── tests/               Unit and behavior tests
├── transform/            Silver/Gold transforms and Athena assets
├── docker-compose.yml
├── Makefile
└── requirements.txt
```

## Prerequisites

- Docker Engine and Docker Compose
- Python and `make`
- A LocalStack auth token configured as `LOCALSTACK_AUTH_TOKEN` in `.env`
  (see `.env.example`)

## Quick Start

`make simulate` runs `python -m simulator.run` with no extra arguments; it
does not pass `--end-yesterday` or `--defect-rate`. For a run ending
relative to the current date, use the direct command below. The full
10,000-subscriber, 30-day generation run recorded peak RSS of 1.91 GB.

```bash
cp .env.example .env
# Set LOCALSTACK_AUTH_TOKEN in .env

make up
make infra
python -m simulator.run --end-yesterday
make quality
make week4
make train-table
make train
make publish-model
make score-day DATE=YYYY-MM-DD
make dashboard
```

For the live demo, start these in separate terminals after infrastructure is
available:

```bash
make live-producer
make live-ingest
make live-loop
make dashboard
```

`make live-demo-help` prints the terminal setup. The first Athena startup can
take many minutes while LocalStack initializes its engine.

## Build Progress

- [x] Foundations, LocalStack infrastructure, and contracts
- [x] Synthetic simulator and Bronze ingestion
- [x] Quality gate and Silver layer
- [x] Gold, Athena investigations, and rule candidates
- [x] Model training, publication, and CLI scoring
- [x] Dashboard and live-demo scripts

## What's Simulated (And How Honestly)

The simulator generates synthetic data with structured randomness.

- Realistic distributions include log-normal amounts, Poisson event rates,
  and weekday/hour seasonality.
- Legitimate and fraud distributions overlap for most fraud types.
- Fraud patterns are probabilistic; a SIM swap does not always lead to IRSF.
- Hard cases include legitimate users who resemble fraud and fraud users who
  resemble legitimate behavior.

The shortcut audit found `hours_since_last_swap` has direction-independent
AUC 0.9995 for SIM-swap takeover. The simulator plants the SIM swap on the day
that fraud starts, creating this near-perfect signal. For IRSF, the best
single feature, `high_risk_destination_call_count_7d_sum`, has AUC 0.7145.
An ablation without the swap-related features is reported. See the
[shortcut check](docs/reports/shortcut_check.md) and
[model comparison](docs/reports/model_vs_rules.md).

### Why a simulator?

Real subscriber CDRs, KYC records, and revenue files are private and
regulated. Synthetic data allows pipeline development without access to
those records. The simulator and its injected patterns do not establish
performance on real telecom data.

## Week 4: Gold, Athena, and Rules

`make week4` builds Gold account-day features, exports subscriber and bundle
reference snapshots, creates Glue Data Catalog and Athena metadata, registers
S3 partitions, runs rule generation, and upserts candidate alerts into
DynamoDB `fraud_alerts`. The full-run baseline flags 8.12% of subscribers.
The saved rules report records 1,046 `IRSF_HIGH_RISK_BURST` alerts, 96
`NEW_ACCOUNT_HIGH_VALUE_PURCHASE` alerts, 19 `REPEATED_BUNDLE_PURCHASES`
alerts, and 67 `SIM_SWAP_INTL_24H` alerts. See the
[full-run rules baseline](docs/reports/rules_baseline.md).

Use `make athena-query SQL=<file>` to run an investigation from
`transform/athena/investigations/`. The rules cover SIM swap followed by
international calls within 24 hours, high-risk destination call bursts,
high-value purchases on young or non-verified accounts without a completed
payment in the prior 7 days, and repeated purchases of one bundle within
24 hours.

Bundle usage is not available, so repeated bundle purchases are a proxy; the
project does not measure consumption beyond an allowance.

## Week 5: Model and Scoring

**Model.** The model is scikit-learn's `HistGradientBoostingClassifier`,
trained on subscriber-day features. The training table excludes label and
split fields from the model inputs and splits by subscriber. Artifacts and
feature-only daily data are published to S3 under
`s3://telco-fraud-bronze/models/fraud_gbm/v1/`.

Main-model test PR-AUC by archetype: SIM-swap takeover 0.8009, IRSF 0.2889,
subscription fraud 0.6666, and bundle abuse 0.0220. Ablated-model test PR-AUC:
SIM-swap takeover 0.1033, IRSF 0.4807, subscription fraud 0.8206, and bundle
abuse 0.1065. IRSF is the harder case and bundle-abuse results are weak. The
bundle-abuse test has 6 positive accounts, and bundle-usage data (source 6)
is not available. See the
[model vs rules report](docs/reports/model_vs_rules.md).

**Scoring.** `scoring/score_day.py` is a CLI that scores one date of daily
features, writes up to 100 model alerts for that date to DynamoDB
`fraud_alerts`, and publishes an SNS summary when alerts exist. Alerts are
HIGH when corroborated by a rule alert on the same date and MEDIUM otherwise.
Run `make score-day DATE=YYYY-MM-DD` or
`python -m scoring.score_day --date YYYY-MM-DD [--dry-run]`.

Scoring runs as a CLI and a Docker image; Lambda deployment is not part of
this iteration. `infra/provision_scoring_lambda.sh` is a reference script and
has not been run end-to-end.

## Week 6: Dashboard and Live Demo

The Streamlit dashboard reads alert data from DynamoDB and the latest quality
gate metrics. It shows alert totals, quarantine rate, model alerts for the
day, alert-type and quarantine-rate charts, top scored accounts, recent
alerts, and limitations. Start it with `make dashboard`.

The live producer sends synthetic activation and purchase records into
Kinesis. `scripts/live_ingest.py` polls those streams and writes batches to
Bronze S3. The quality gate and scoring run in a repeated 60-second loop in
the demo. Events reach Bronze in near-real-time via Kinesis; Silver refreshes
on the demo's 60-second loop; the model scores daily aggregate features in
batch. This is not real-time fraud detection. The live loop does not rebuild
the published daily model-feature partitions from each ingest batch.

## Limitations

- Data and labels are simulated; results with injected patterns are
  optimistic and do not establish real-world performance.
- SIM-swap labels have a circular planted signal: the swap is placed on the
  fraud start day and `hours_since_last_swap` is near-perfectly informative.
- Fraud counts are small, especially for bundle abuse (6 test accounts), and
  reported confidence intervals are wide.
- There is no temporal holdout; train and test are from the same 30-day
  window.
- Rules have not been calibrated against real operational data; the IRSF and
  new-account purchase rules have low precision in the saved evaluation.
- Rule alerts are candidates for review. They do not block accounts.
- `risk_score_baseline` has an apparent scale mismatch: the shortcut report's
  subscriber-level means are around 1,173–1,426, while the profile contract
  constrains the value to the 0–1 range. This needs an audit.
- Bundle abuse uses repeated purchases as a proxy because bundle-usage data
  is unavailable.
- PII hashing, least-privilege IAM, retention controls, and data-catalog
  ownership tags are not implemented.

## Rules baseline

The saved full-run evaluation is linked above at
[docs/reports/rules_baseline.md](docs/reports/rules_baseline.md). Results come
from simulated data with injected patterns and should not be treated as
expected real-world performance.

## License

MIT (for the code). The project name, structure, and simulator design are
original.
