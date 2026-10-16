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
| 2 | Revenue (prepaid + postpaid) | S3 batch | ✅ |
| 3 | Usage (voice + data CDRs) | S3 batch | ✅ |
| 4 | Bundle catalog | DynamoDB (reference) | ✅ |
| 5 | Bundle purchases | Kinesis → Lambda → S3 | ✅ |
| 6 | Bundle usage | S3 batch | 🔜 second iteration |
| 7 | Churn / customer state | S3 batch | 🔜 second iteration |
| 8 | Subscriber profile | DynamoDB (reference) | ✅ |

---

## Data Quality Gate

Starting with three checks per source:
- **Schema** — every row matches its Pydantic contract (`contracts/`).
- **Duplicates** — no repeated `event_id` / `transaction_id` / `call_id`.
- **Ranges** — non-negative amounts and durations, no future timestamps.

Later iterations add:
- Referential checks (does `bundle_id` exist in the catalog?).
- Freshness alerts (is today's revenue file present?).
- Business rules (revenue total vs. expected daily range).

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

# 5. Run tests
make test

# 6. Tear down
make destroy
```

---

## Build Progress

- [x] **Day 1** — Environment, infrastructure, contracts
- [ ] **Week 2** — Simulator + bronze ingestion
- [ ] **Week 3** — Quality gate + silver
- [ ] **Week 4** — Gold, Athena, rules engine
- [ ] **Week 5** — Model training + scoring Lambda
- [ ] **Week 6** — Dashboard, demo video, polish

---

## What's Simulated (And How Honestly)

The simulator (Week 2) generates data with **structured randomness**:

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

## License

MIT (for the code). The project name, structure, and simulator design are
original.