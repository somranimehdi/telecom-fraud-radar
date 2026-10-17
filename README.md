# Telecom Fraud Radar

An AWS-first fraud detection pipeline for telecom, running entirely on
**LocalStack**. No paid cloud account, no credit card — just Docker.

> ⚠️ **Honest notice**
> - **All data is simulated.** No real subscriber, call, revenue, or KYC data
>   is used anywhere in this project. The simulator injects known fraud
>   patterns so the model has labels to learn from.
> - **Runs on LocalStack**, not real AWS. Resource names, IAM policies, and
>   service configurations are realistic, but they execute locally.
> - The Kaggle telecom fraud dataset is used only as **secondary validation**;
>   the primary training signal comes from the simulator.

---

## What It Does

A telco receives events from many systems. Fraud hides across them: SIM swap
takeovers, international revenue share fraud (IRSF), subscription fraud, and
bundle abuse. This project ingests those sources, enforces data quality,
scores accounts, and raises alerts.

**Fraud types covered:**
- SIM swap takeover
- International Revenue Share Fraud (IRSF)
- Subscription fraud
- Bundle abuse

---

## Architecture

```
Producers              Ingestion                 Storage & Processing           Consumers
─────────              ─────────                 ─────────────────────          ─────────
SIM activation  ──►  Kinesis ──► Lambda ──►  S3 raw/activation/  ─┐
Bundle purchase ──►  Kinesis ──► Lambda ──►  S3 raw/bundles/     ─┤
Revenue files   ─────────────────────────►   S3 raw/revenue/     ─┼──► Quality gate ──► S3 silver/
Usage CDRs      ─────────────────────────►   S3 raw/usage/       ─┤        │
Subscriber ref  ─────────────────────────►   DynamoDB            ─┤        └──► S3 quarantine/
Bundle catalog  ─────────────────────────►   DynamoDB            ─┘
                                                                    │
                                                          Athena ◄──┤
                                                                    │
                                                    Rules + Model ◄─┤
                                                                    ▼
                                                          DynamoDB fraud_alerts
                                                                    │
                                                          SNS fraud-alerts-topic
                                                                    │
                                                          Dashboard (static)
```

**Medallion layers:**
- **Bronze** (`raw/`) — immutable raw events and files. The audit trail.
- **Silver** (`silver/`) — validated, cleaned, deduplicated.
- **Gold** (`gold/`) — curated Parquet, queryable with Athena.
- **Quarantine** (`quarantine/`) — failed rows with a reason column.

---

## Sources

| # | Source | Transport | In first demo |
|---|---|---|---|
| 1 | SIM activation | Kinesis → Lambda → S3 | ✅ |
| 2 | Revenue (prepaid) | S3 batch | ✅ |
| 3 | Usage (voice + data CDRs) | S3 batch | ✅ |
| 4 | Bundle catalog | DynamoDB (reference) | ✅ |
| 5 | Bundle purchases | Kinesis → Lambda → S3 | ✅ |
| 6 | Bundle usage | S3 batch | 🔜 second iteration |
| 7 | Churn / customer state | S3 batch | 🔜 second iteration |
| 8 | Subscriber profile | DynamoDB (reference) | ✅ |

The simulator currently generates prepaid revenue only; postpaid revenue is
not generated yet.

---

## Data Quality Gate

The gate applies these checks per source:
- **Schema** — every row matches its Pydantic contract (`contracts/`).
- **Duplicates** — no repeated `event_id` / `transaction_id` / `call_id`.
- **Ranges** — non-negative amounts and durations, no future timestamps.

It also checks referential integrity against the subscriber and bundle
reference tables, and alerts when the newest S3 partition is stale.

Failed rows are written to `quarantine/` with a `reason` column. Quarantine
rate above a threshold publishes to SNS.

---

## Data Governance

This project includes governance controls by design:

- **Quality gate** — schema, uniqueness, and range enforcement.
- **Data contracts** — Pydantic schemas in `contracts/` define every source.
- **Lineage** — bronze → silver → gold; bronze is immutable.
- **Quarantine with reasons** — no silent data loss.
- **Persistence** — LocalStack runs with `PERSISTENCE=1`; state survives restarts.

**Not implemented (and documented as such):**
- IAM least-privilege roles (root is used for simplicity).
- S3 lifecycle/retention policies.
- PII hashing at ingestion (`msisdn`, `imsi` stored in the clear).
- Data catalog with ownership tags.

These are called out honestly so the gap is visible, not hidden.

---

## Stack

| Layer | Tool |
|---|---|
| Cloud emulation | LocalStack (Pro, Student plan) |
| Container runtime | Docker + Docker Compose |
| Language | Python 3.13 |
| Contracts | Pydantic v2 |
| Dataframes | Polars |
| ML | scikit-learn, XGBoost |
| Tests | pytest |
| Infrastructure | `docker-compose.yml` + `make` |

---

## Repository Layout

```
telecom-fraud-radar/
├── contracts/          Pydantic schemas for every source
├── dashboard/          Static metrics export + web UI
├── infra/              LocalStack provisioning scripts
├── model/              Training scripts + feature engineering
├── quality_gate/       Validation engine + Glue-compatible wrapper
├── scoring/            Rules engine + Lambda scoring + model inference
├── simulator/          Data generators (streaming + batch)
├── transform/          Silver/Gold transforms + Athena DDLs
├── docker-compose.yml  LocalStack container definition
├── Makefile            One-command workflows
├── requirements.txt    Python dependencies
└── README.md           This file
```

---

## Prerequisites

- Docker Engine + Docker Compose
- Python 3.13 (system or via `python3.13`)
- `make`
- A LocalStack auth token (Student plan). Set it in `.env` as
  `LOCALSTACK_AUTH_TOKEN=...` — see `.env.example` if you're cloning this.

Running the LocalStack pipeline requires a valid LocalStack auth token in
`.env`.

---

## Quick Start

```bash
# 1. Configure your LocalStack token
cp .env.example .env
# edit .env and paste your LOCALSTACK_AUTH_TOKEN

# 2. Launch LocalStack
make up

# 3. Provision infrastructure (S3, DynamoDB, Kinesis, SNS)
make infra

# 4. Run the simulator (Week 2 onwards)
make simulate

# 5. Validate source data and produce Silver/Quarantine
make quality

# 6. Build Gold, set up Athena, and write rule candidates
make week4

# 7. Run an investigation query
make athena-query SQL=transform/athena/investigations/sim_swap_24h.sql

# 8. Tear down
make destroy
```

---

## Build Progress

- [x] **Day 1** — Environment, infrastructure, contracts
- [x] **Week 2** — Simulator + bronze ingestion
- [x] **Week 3** — Quality gate + silver
- [x] **Week 4** — Gold, Athena, rules engine
- [x] **Week 5** — Model training + scoring (Lambda deployment plan-gated)
- [x] **Week 6** — Dashboard, live demo, polish

---

## What's Simulated (And How Honestly)

The simulator (Week 2) generates data with **structured randomness**:

The default configuration simulates **10,000 subscribers over 30 days**.
The small configuration simulates **500 subscribers over 5 days** for quick
tests. Both use simulated data with injected fraud patterns.

- Realistic distributions (log-normal amounts, Poisson event rates, weekday/
  hour seasonality).
- **Overlapping** legit and fraud distributions — no single feature separates
  the classes.
- Probabilistic fraud patterns — a SIM swap doesn't *always* lead to IRSF.
- Deliberate hard cases — legit users who look like fraud, fraud users who
  look legit.

This prevents the model from memorizing a single-feature shortcut. If any
feature correlates > 0.7 with `is_fraud`, the simulator is considered too
easy and is retuned.




---


### Why a simulator?

The simulator is not a shortcut — it's the standard way to prototype a
pipeline without access to real telco data. Real subscriber CDRs, KYC
records, and revenue files are private and regulated. The simulator
generates realistic distributions (log-normal, Poisson, seasonality) with
deliberate overlap between legit and fraud, so the model has a
non-trivial learning problem. The pipeline itself — ingestion, quality,
transformations, scoring, alerting — is the deliverable and would run
unchanged on real AWS with real data.


---

## Week 4: Gold, Athena, and Rules

Run `make week4` after Bronze has been validated into Silver. It builds
`gold/account_daily_features/` (one subscriber-day row, with source IDs
deduplicated across repeated quality-gate runs), exports DynamoDB subscriber
and bundle snapshots to `gold/reference/`, creates Glue database
`telecom_fraud_radar` and Athena workgroup `fraud-radar-workgroup`, registers
external tables and discovered S3 partitions, then upserts candidate alerts
into DynamoDB table `fraud_alerts` (key `alert_id`, GSI `subscriber-index`).

Week 4 was completed and run end to end in LocalStack. All eight external
tables were created and their S3 partitions registered. The four saved
investigation queries were executed successfully, and the rules workflow
upserted 26 candidate alerts to `fraud_alerts`. Rerun the full workflow with
`make week4`.

Saved investigation SQL is in `transform/athena/investigations/`. Use
`make athena-query SQL=<file>` to run one query. Rules cover a SIM swap
followed by international calls within 24 hours, high-risk destination call
bursts, high-value purchases on young or non-verified accounts without a
completed payment in the prior seven days, and repeat purchases of one bundle
within 24 hours.

**Bundle abuse limitation:** bundle-usage source 6 is deferred, so the
repeat-purchase rule is only a bundle-abuse proxy. The project does not claim
to detect allowance over-consumption yet.

## Week 5: Model and Scoring

**Model.** An XGBoost binary classifier (`HistGradientBoostingClassifier`)
trained on 30 days of `gold/model_features/` across 10,000 simulated
subscribers. Model artifact, feature columns, and evaluation metrics are
published to `s3://telco-fraud-bronze/models/fraud_gbm/v1/`. Evaluation
metrics include per-archetype ROC-AUC and PR-AUC, GroupKFold cross-
validation, and a rules-only vs model vs combined comparison — see
`model/out/model_metrics.json` and `evaluation/out/model_vs_rules.md`.

Known limitation: bundle abuse detection is weak (PR-AUC 0.022 on
6 test positives) because bundle-usage source 6 is deferred. The model
detects SIM swap takeover (PR-AUC 0.80), subscription fraud (PR-AUC 0.67),
and IRSF (PR-AUC 0.29) well enough to be useful; bundle abuse is a proxy.

**Scoring.** `scoring/score_day.py` scores one day of Gold features and
writes idempotent `MODEL_GBM_V1` alerts to DynamoDB `fraud_alerts`, capped
at 100 per day by score. Alerts are `HIGH` severity when corroborated by
a rule alert on the same day, `MEDIUM` otherwise. An SNS summary is
published to `fraud-alerts-topic` per run. Invoke with
`make score-day DATE=YYYY-MM-DD` or `python -m scoring.score_day --date
YYYY-MM-DD [--dry-run]`.

The scoring module is containerized (`scoring/Dockerfile`, verified Docker
build), but Lambda deployment is out of scope for this iteration because
ECR is plan-gated in LocalStack's Student plan. The provision script in
`infra/provision_scoring_lambda.sh` documents the intended deployment
path and is retained as a reference for a Team-plan LocalStack instance.

## Week 6: Dashboard and Live Demo

**Streamlit dashboard.** `dashboard/app.py` reads live state from DynamoDB
`fraud_alerts` and the latest quality-gate run, and renders total alerts,
quarantine rate, model alerts today, alert-type and quarantine-rate charts,
top-10 scored accounts, recent alerts, and an honest limitations panel.
Launch with `make dashboard`.

**Live demo.** Three scripts simulate near-real-time ingestion without
requiring Lambda deployment (ECR is plan-gated on LocalStack Student):
- `make live-producer` streams activation and purchase events into Kinesis
  at 5 events/sec (`scripts/live_producer.py`).
- `make live-ingest` polls Kinesis and writes batches to Bronze S3,
  simulating what the Lambda consumer would do in production
  (`scripts/live_ingest.py`).
- `make live-loop` runs the quality gate and daily scoring every 60 seconds
  so new quality results and alerts can appear in the dashboard
  (`scripts/live_loop.sh`).

**What this is and isn't.** Events are produced in real time and land in
Bronze S3 in near-real-time. Silver is a batch layer: the quality gate runs
every 60 seconds and processes available Bronze data. Gold model-feature
partitions remain the published daily snapshot; `live-loop` does not rebuild
them from each ingest batch. Scoring therefore uses daily aggregate features,
not per-event features, which fits low-velocity telecom fraud such as SIM
swap and subscription fraud. Production-grade event-driven scoring would
require a different model on CDR-level features and a feature store; that's
out of scope for this iteration.

Run `make live-demo-help` for the 4-terminal demo setup.

## Rules baseline

Run `make eval-rules` to evaluate alerts in `fraud_alerts` against the
simulator labels. The report is written to
[`evaluation/out/rules_baseline.md`](evaluation/out/rules_baseline.md).
Results come from simulated data with injected patterns, so they are
optimistic and should not be treated as expected real-world performance.

## License

MIT (for the code). The project name, structure, and simulator design are
original.
