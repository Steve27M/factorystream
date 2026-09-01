# The single automation interface: the same targets run locally and in CI, so
# CI is never a special environment (PMLO Ch. 1).
#
# OneDrive's filesystem filter blocks the hardlinks uv uses by default
# (os error 396). This repo lives under OneDrive, so copy mode is not optional.
export UV_LINK_MODE = copy

.PHONY: install lint typecheck test check generate replay \
        broker consume dbt-build reconcile \
        tf-plan tf-apply tf-destroy teardown-verify costs

install:  ## Runtime + dev dependencies. Phase 1 needs no broker or cloud extras.
	uv pip install --link-mode=copy -e ".[dev]"

lint:
	ruff check src tests tools

typecheck:
	mypy

test:  ## Unit tests. No network, no broker, no cloud.
	pytest -q

check: lint typecheck test  ## Everything CI runs on a PR.

# --- Phase 1: generator ------------------------------------------------------

generate:  ## Emit a synthetic shift of events + the ground-truth manifest.
	python -m factorystream.generator.cli --config plant/scenario.yaml --out out/

# --- Phase 2: broker + consumer ----------------------------------------------

broker:  ## Redpanda + the topic (6 partitions, keyed by machine_id).
	docker compose up -d redpanda topic-init

broker-ui:  ## Optional Redpanda console on :8080 — partition + lag view for the demo.
	docker compose --profile ui up -d console

broker-down:  ## Stop the broker. Add `-v` yourself to drop the volume too.
	docker compose down

contracts:  ## Compatibility matrix across contract versions, and what it cannot see.
	python -m factorystream.contracts.report

contract-check:  ## Validate out/events.jsonl against each event's declared version.
	python -m factorystream.consumer.producer --events out/events.jsonl --contract-check-only

publish:  ## Produce the generated events to the topic. Corrupt payloads go as-is.
	python -m factorystream.consumer.producer --events out/events.jsonl

consume:  ## Poll -> validate -> batch -> parquet -> THEN commit offsets.
	python -m factorystream.consumer.consumer --root $(ROOT) --stop-after-idle 5

replay:  ## Re-land bronze from offset 0. Idempotent naming makes this safe.
	python -m factorystream.consumer.consumer --root $(ROOT) --from-beginning 	  --group replay-$(shell date +%s) --stop-after-idle 5

load:  ## Bootstrap path: generator -> bronze, bypassing the broker.
	python -m factorystream.consumer.load --root $(ROOT)

test-integration:  ## The kill-test. Needs a running broker. Proves no loss, no duplicates.
	pytest -q -m integration

# --- Phase 3/4: warehouse + reconciliation -----------------------------------

dbt-build:  ## staging -> silver -> gold -> completeness_ledger, with tests as the gate.
	cd transform && dbt build --profiles-dir .

reconcile: dbt-build  ## Build the ledger and fail if any window is broken.

status-page:  ## Render the ledger as a static page for GitHub Pages.
	python tools/build_status_page.py --out docs/status/index.html

# --- Cloud -------------------------------------------------------------------
# Nothing here is invoked until Phase 0 guardrails exist. `teardown-verify`
# runs at the end of every working session, per the shared account discipline.

tf-plan:
	cd infra/terraform && terraform plan

tf-apply:
	cd infra/terraform && terraform apply

tf-destroy:
	cd infra/terraform && terraform destroy

teardown-verify:  ## Assert no unexpected billable resource exists. Run every session.
	python tools/teardown_verify.py

costs:  ## Refresh the measured cost table in COSTS.md from Cost Explorer.
	python tools/costs.py
