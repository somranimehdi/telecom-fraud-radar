# ────────────────────────────────────────────────────────────────────────────
# Telecom Fraud Radar — Makefile
# ────────────────────────────────────────────────────────────────────────────
# Usage:
#   make up          Start LocalStack (requires .env with LOCALSTACK_AUTH_TOKEN)
#   make infra       Provision S3, DynamoDB, Kinesis, SNS
#   make simulate    Run the simulator (Week 2+)
#   make test        Run the test suite
#   make destroy     Stop the container and delete local state
#   make logs        Tail LocalStack logs
#   make shell       Open a shell inside the LocalStack container
#   make health      Print the LocalStack health endpoint
# ────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV        := .venv
PY          := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
AWS_LOCAL   := $(VENV)/bin/awslocal

BUCKET      := telco-fraud-bronze
SNS_TOPIC   := fraud-alerts-topic
KINESIS_1   := sim-activation-stream
KINESIS_2   := bundle-purchases-stream

.PHONY: help up down destroy infra test simulate logs shell health clean quality quality-quiet quality-one gold athena athena-query rules eval-rules train-table shortcut-check train publish-model score-day scoring-lambda measure-cold-start week4 live-demo-help live-producer live-ingest live-loop dashboard-export dashboard

help:
	@echo "Telecom Fraud Radar — available targets:"
	@echo "  make up         Start LocalStack container"
	@echo "  make down       Stop LocalStack container (keeps state)"
	@echo "  make destroy    Stop container AND delete .localstack/ state"
	@echo "  make infra      Provision all AWS resources on LocalStack"
	@echo "  make simulate   Run the data simulator (Week 2+)"
	@echo "  make test       Run pytest"
	@echo "  make logs       Tail LocalStack logs"
	@echo "  make shell      Open a shell inside the container"
	@echo "  make health     Print LocalStack health JSON"
	@echo "  make quality    Run the quality gate on all sources"
	@echo "  make quality-one SOURCE=voice   Run on a single source"
	@echo "  make gold        Build account-day Gold features + reference snapshots"
	@echo "  make athena      Set up Glue catalog, Athena tables, and partitions"
	@echo "  make athena-query SQL=path      Run a saved investigation query"
	@echo "  make rules       Write rule candidate alerts to DynamoDB"
	@echo "  make eval-rules  Evaluate fraud_alerts against simulator labels"
	@echo "  make train-table Build the subscriber-day model training table"
	@echo "  make shortcut-check Check model features for label shortcuts"
	@echo "  make train       Train and evaluate the Week 5 model"
	@echo "  make publish-model Publish the model and feature-only daily data to LocalStack S3"
	@echo "  make score-day   Score one date (DATE=YYYY-MM-DD)"
	@echo "  make scoring-lambda Provision the scoring Lambda container in LocalStack"
	@echo "  make measure-cold-start Measure cold and warm Lambda latency"
	@echo "  make live-demo-help Show the four-terminal live demo setup"
	@echo "  make live-producer Stream synthetic events into Kinesis"
	@echo "  make live-ingest   Poll Kinesis into Bronze S3"
	@echo "  make live-loop     Run quality + daily scoring every 60s"
	@echo "  make dashboard-export Export current LocalStack state to JSON"
	@echo "  make dashboard     Start the Streamlit dashboard"
	@echo "  make week4       Run the complete Week 4 build"
	@echo "  make clean      Remove Python caches"

# ─── Environment ────────────────────────────────────────────────────────────

.env:
	@echo "❌ .env not found. Copy .env.example to .env and paste your LOCALSTACK_AUTH_TOKEN."
	@exit 1

up: .env
	@if grep -q "your_localstack_token_here" .env; then \
		echo "❌ .env still contains the placeholder token. Paste your real LOCALSTACK_AUTH_TOKEN."; \
		exit 1; \
	fi
	docker compose up -d
	@echo "⏳ Waiting for LocalStack to become healthy..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:4566/_localstack/health > /dev/null; then \
			echo "✅ LocalStack is healthy."; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "❌ LocalStack did not become healthy within 60s. Check 'make logs'."; \
	exit 1

down:
	docker compose down

destroy:
	docker compose down
	docker run --rm -v "$(PWD)/.localstack:/data" alpine sh -c "rm -rf /data/state /data/tmp"
	@echo "🗑️  Containers stopped; LocalStack state and temporary files removed. Athena engine files preserved."

# ─── Infrastructure ─────────────────────────────────────────────────────────

infra: up
	@echo "── S3 ──"
	@$(AWS_LOCAL) s3 mb s3://$(BUCKET) 2>/dev/null || echo "  bucket $(BUCKET) already exists"
	@for p in raw/activation/ raw/revenue/prepaid/ raw/revenue/postpaid/ \
	          raw/usage/voice/ raw/usage/data/ raw/bundles/purchases/ raw/customers/; do \
		$(AWS_LOCAL) s3api put-object --bucket $(BUCKET) --key $$p > /dev/null; \
	done
	@echo "  ✅ 7 raw prefixes present"

	@echo "── DynamoDB ──"
	@$(AWS_LOCAL) dynamodb create-table \
		--table-name subscriber_profile \
		--attribute-definitions AttributeName=subscriber_id,AttributeType=S \
		--key-schema AttributeName=subscriber_id,KeyType=HASH \
		--billing-mode PAY_PER_REQUEST > /dev/null 2>&1 || echo "  subscriber_profile already exists"
	@$(AWS_LOCAL) dynamodb create-table \
		--table-name bundle_catalog \
		--attribute-definitions AttributeName=bundle_id,AttributeType=S \
		--key-schema AttributeName=bundle_id,KeyType=HASH \
		--billing-mode PAY_PER_REQUEST > /dev/null 2>&1 || echo "  bundle_catalog already exists"
	@echo "  ✅ 2 tables provisioned"

	@echo "── Kinesis ──"
	@$(AWS_LOCAL) kinesis create-stream --stream-name $(KINESIS_1) --shard-count 1 2>/dev/null || echo "  $(KINESIS_1) already exists"
	@$(AWS_LOCAL) kinesis create-stream --stream-name $(KINESIS_2) --shard-count 1 2>/dev/null || echo "  $(KINESIS_2) already exists"
	@echo "  ✅ 2 streams provisioned"

	@echo "── SNS ──"
	@$(AWS_LOCAL) sns create-topic --name $(SNS_TOPIC) > /dev/null
	@echo "  ✅ $(SNS_TOPIC) provisioned"

	@echo ""
	@echo "🎉 Infrastructure ready."

# ─── Simulator (Week 2+) ────────────────────────────────────────────────────

simulate: infra
	@if [ -f simulator/run.py ]; then \
		$(PY) -m simulator.run; \
	else \
		echo "⏳ Simulator not yet implemented (Week 2)."; \
	fi


# ─── Quality Gate ───────────────────────────────────────────────────────────

quality:
	$(PY) -m quality_gate.run

quality-quiet:
	$(PY) -m quality_gate.run --quiet

quality-one:
	@if [ -z "$(SOURCE)" ]; then \
		echo "Usage: make quality-one SOURCE=voice"; \
		exit 1; \
	fi
	$(PY) -m quality_gate.run --sources $(SOURCE)

# ─── Gold, Athena, and Rules (Week 4) ──────────────────────────────────────

gold:
	$(PY) -m transform.gold --config simulator/config.yaml

athena: gold
	$(PY) -m transform.athena.setup --config simulator/config.yaml

athena-query:
	@if [ -z "$(SQL)" ]; then \
		echo "Usage: make athena-query SQL=transform/athena/investigations/irsf_bursts.sql"; \
		exit 1; \
	fi
	$(PY) -m transform.athena.query --sql-file "$(SQL)" --config simulator/config.yaml

rules: athena
	$(PY) -m scoring.rules --config simulator/config.yaml

eval-rules:
	$(PY) -m evaluation.rules_baseline --config simulator/config.yaml

train-table:
	$(PY) -m model.build_training_table --config simulator/out/config.used.yaml

shortcut-check:
	$(PY) -m model.shortcut_check

train:
	$(PY) -m model.train

publish-model:
	$(PY) -m model.publish

score-day:
	@if [ -z "$(DATE)" ]; then \
		echo "Usage: make score-day DATE=YYYY-MM-DD [DRY_RUN=1]"; \
		exit 1; \
	fi
	$(PY) -m scoring.score_day --date $(DATE) $(if $(filter 1 true yes,$(DRY_RUN)),--dry-run,)

scoring-lambda:
	@echo "═══ Provisioning scoring Lambda ═══"
	bash infra/provision_scoring_lambda.sh

measure-cold-start:
	@echo "═══ Measuring cold-start latency ═══"
	$(PY) -m scripts.measure_cold_start

# ─── Live Demo and Dashboard (Week 6) ──────────────────────────────────────

live-demo-help:
	@echo "═══ Live demo (3 terminals) ═══"
	@echo "Terminal 1:  make live-producer"
	@echo "Terminal 2:  make live-ingest"
	@echo "Terminal 3:  make live-loop   # every 60s: quality + score"
	@echo "Terminal 4:  make dashboard   # Streamlit, auto-refreshes"

live-producer:
	$(PY) -m scripts.live_producer --rate 5

live-ingest:
	$(PY) -m scripts.live_ingest

live-loop:
	bash scripts/live_loop.sh

dashboard-export:
	$(PY) -m dashboard.export

dashboard:
	$(PY) -m streamlit run dashboard/app.py

week4: rules
	@echo "✅ Week 4 Gold, Athena, and rule candidates are ready."

# ─── Tests ──────────────────────────────────────────────────────────────────

test:
	@$(PY) -m pytest -v

# ─── Utilities ──────────────────────────────────────────────────────────────

logs:
	docker compose logs -f localstack

shell:
	docker exec -it telecom-fraud-radar-localstack bash

health:
	@curl -s http://localhost:4566/_localstack/health | $(PY) -m json.tool

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@echo "🧹 Python caches removed."
