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

.PHONY: help up down destroy infra test simulate logs shell health clean quality quality-quiet quality-one

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
	rm -rf .localstack
	@echo "🗑️  Container stopped and .localstack/ deleted."

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