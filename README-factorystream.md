# FactoryStream

A streaming manufacturing-telemetry lakehouse with provable completeness: an adversarial synthetic event generator (late, duplicate, out-of-order, clock-skewed, schema-drifting events by design) streams shop-floor telemetry through a Kafka-API broker into an AWS data lake — S3 parquet, Glue catalog, Athena — transformed by dbt into a Kimball star, with every layer reconciled exactly against the generator's ground-truth manifest. All AWS infrastructure is Terraform-provisioned and deployed via GitHub Actions OIDC; the monthly cost is measured and published.

> **How to use this document:** This is the full build specification — third rung of the ladder after HelioSentinel and TransportIQ, and buildable independently of both (it reuses their *conventions*, not their code). Build phase by phase (Milestones); Non-Goals are binding. When a design decision is ambiguous, prefer the simpler option and record the rejected alternative in `docs/decisions.md`.
>
> **Citations:** Grounded in three reference texts — **[DMLS]**, **[PMLO]**, **[MLDP]** (see References). **[AIE]** is deliberately not cited: this project contains no LLM layer, by design (see Purpose).

---

## Mission

Build and operate a cloud-native streaming pipeline that ingests high-volume synthetic manufacturing telemetry through a real broker with real streaming semantics — partitions, consumer groups, offset management, late and duplicate and out-of-order data — into an S3 lakehouse modeled with dbt, and *prove* end-to-end correctness by exact reconciliation against generator ground truth.

## Purpose

This is the ladder's **data-engineering credential rung**, and it exists to cover exactly what the other two rungs cannot:

1. **Managed cloud services + infrastructure as code.** HelioSentinel runs on GitHub Actions; TransportIQ runs on a Terraform'd VPS. Neither touches managed cloud data services. Here, S3 / Glue / Athena / IAM are the warehouse, and every resource is Terraform — the repo demonstrates cloud data-platform work, not cloud-adjacent work.
2. **Broker-based streaming semantics.** Both prior projects explicitly bind "no Kafka/broker" as non-goals because their sources didn't warrant one *(DMLS Ch. 3, "Batch Processing Versus Stream Processing")*. This project *manufactures* the conditions that warrant one — sustained event rates, multiple logical producers, consumer-side replay — so the broker is justified, not decorative. The hard parts of streaming are not the broker; they are late data, duplicates, ordering, and exactly-once claims. The generator produces all of them **on purpose, at configured rates**, so correctness handling is testable rather than anecdotal.
3. **Provable completeness.** Real pipelines assert correctness; few can prove it, because ground truth is unknowable. A synthetic generator knows exactly what it emitted. The generator writes a signed per-window **manifest** (counts, sums, checksums), and the pipeline's gold layer must reconcile against it *exactly*. Reconciliation-as-a-product-feature is the thesis of the repo.
4. **No LLM layer, deliberately.** The other rungs carry the AI credential. A rung that is pure data engineering, done to the same documentation standard, shows the discipline is not dependent on the model layer for interest.

The domain is manufacturing (machines, work orders, cycles, defects) because it is the author's professional domain — but **every event is synthetic**, from a generator whose behavior is fully specified below. No employer data, schema, or system detail informs this project.

## End State (Definition of Done)

1. A configurable discrete-event generator emits realistic multi-machine shop-floor telemetry with injected disorder (late/dup/out-of-order/skew/drift) at declared rates, and writes a ground-truth manifest per window.
2. Events flow through a Kafka-API broker (Redpanda) with keyed partitions and a consumer group that lands partitioned parquet to S3 **bronze** with at-least-once delivery and idempotent, replay-safe writes.
3. Glue catalogs the lake; dbt (Athena adapter) builds **silver** (validated, deduplicated, conformed) and **gold** (Kimball star: machine/product/work-order dims with SCD2, cycle and defect facts) with dbt tests gating CI.
4. A reconciliation job proves gold-vs-manifest equality per window and publishes a **completeness ledger**; any nonzero delta is a build failure with a documented root cause, not a footnote.
5. All AWS resources exist only via Terraform; CI deploys via GitHub Actions **OIDC** (no long-lived cloud keys anywhere); `terraform destroy && apply` plus a replay rebuilds the lake from the broker/raw layer — the acceptance test.
6. The system runs on a **published duty cycle** with badges, an ops/status page, and a measured **monthly cost table in the README** (target ≤ $10/month).

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────────┐
│  Generator (adversarial by design)                                 │
│  sim clock → machines/jobs/cycles/defects → disorder injector      │
│      │                                   └→ ground-truth manifest ─┼──► S3 /manifests
│      ▼                                                             │
│  Redpanda (Kafka API): topic factory.events                        │
│    keyed by machine_id → partition (per-key ordering)              │
│      ▼                                                             │
│  Consumer group (Python): batch → parquet → S3 /bronze             │
│    manual offset commit AFTER S3 flush (at-least-once)             │
│    idempotent object naming (replay-safe)                          │
│      ▼                                                             │
│  Glue catalog ─► Athena ─► dbt: silver (validate/dedupe/conform)   │
│                             └──► gold (Kimball star, SCD2 dims)    │
│      ▼                                                             │
│  Reconciliation: gold vs manifest, per window ─► completeness      │
│  ledger ─► status page + badges (+ dashboard)                      │
│                                                                    │
│  Terraform: S3, Glue, Athena, IAM (OIDC role), budgets/alarms      │
└────────────────────────────────────────────────────────────────────┘
```

Principles:

- **The broker earns its place.** The generator's rates and replay requirements are declared in the README as the justification; a project where a cron job would do is not allowed to keep the broker *(DMLS Ch. 3)*.
- **Effectively-once, stated honestly.** Delivery is at-least-once (manual offset commit after durable write); *processing* is made effectively-once by idempotent object naming and key-based dedupe in silver. The README says exactly this and never claims "exactly-once" unqualified.
- **Everything observable, everything reconciled.** Per-batch consumer telemetry, per-window reconciliation rows, per-run ledger entries — same observability posture as the other rungs *(DMLS Ch. 8; PMLO Ch. 6)*.

---

## The Generator (Phase 1 — it is a product, not a fixture)

A discrete-event simulation of a small plant: N machines (config, default 8) across 2 lines, running work orders of M units; per-unit **cycle events**, per-cycle possible **defect events**, machine **state-change events** (running/idle/down/changeover), and **operator scan events**. Shift calendar (2 shifts, breaks), per-machine base rates with drift, seeded RNG throughout — same seed, same event stream, byte for byte *(MLDP Ch. 1, "Reproducibility")*.

**Disorder injector — the point of the project.** Applied post-generation, pre-publish, at *configured, logged* rates:

| Disorder | Default rate | What it stresses |
|---|---|---|
| Late arrival (event_time ≪ publish_time, up to 6 h) | 2% | watermarks, window re-statement |
| Duplicate publish (same event_id, 1–3 extra times) | 1% | dedupe keys, idempotency |
| Out-of-order within key | 3% | per-key ordering assumptions |
| Clock skew per machine (±90 s, wandering) | 2 machines | event-time vs ingest-time discipline |
| Schema drift (v2 payload: added field, one renamed) | one cutover event mid-history | contract handling, staging tolerance |
| Corrupt payload (truncated/invalid JSON) | 0.1% | quarantine path, never silent drop |

Every event carries `event_id` (uuid), `event_time` (sim), `publish_time` (wall), `schema_version`, and a `machine_id` partition key. Every injected disorder is *also logged to the manifest* — the pipeline's handling of each disorder class is therefore checkable by class, not just in aggregate.

**Ground-truth manifest.** Per 15-minute event-time window, per machine: exact event counts by type, unit counts, defect counts, sum of cycle durations, and a checksum over sorted event_ids — written directly to `s3://…/manifests/` by the generator (it bypasses the pipeline; it *is* the truth the pipeline is judged against). This is the public, provable analogue of a production reconciliation boundary — the thing real warehouses need and rarely have.

---

## Broker & Consumer (Phase 2)

**Redpanda, single node, Docker** (Kafka-API-compatible; chosen over a managed cloud stream purely on cost — see decisions; over Apache Kafka on operational footprint — no ZooKeeper/KRaft ceremony for one node). Runs wherever the duty cycle runs (local or the TransportIQ VPS — either is fine; document which).

- Topic `factory.events`, 6 partitions, keyed by `machine_id` → per-machine ordering within a partition, which is exactly the ordering guarantee real Kafka gives and no more — the out-of-order injector proves the pipeline doesn't secretly assume global order.
- Producer: the generator publishes with acks=all; corrupt payloads are published as-is (the *consumer* must survive them).
- **Consumer group** (Python, `confluent-kafka`): poll → validate/quarantine → buffer → write parquet batch to S3 → **then** commit offsets manually. Commit-after-durable-write is the at-least-once contract; auto-commit is the classic data-loss bug and is explicitly disabled. A crash between write and commit produces re-delivery, which the idempotent object naming absorbs: object key = `bronze/date=…/hour=…/part-{partition}-{first_offset}-{last_offset}.parquet` — a replayed batch overwrites itself, never duplicates.
- Consumer telemetry per batch to a local log shipped into the lake: records consumed, quarantined, S3 write latency, offset lag. Lag is the freshness metric *(DMLS Ch. 8)*.
- **Replay is a feature:** `make replay FROM=<offset|timestamp>` re-lands bronze deterministically; combined with dbt rebuilds this is the disaster-recovery story, exercised in the acceptance test.

## Lake & Warehouse (Phase 3)

- **Bronze:** raw parquet as landed, partitioned `date/hour`, plus `quarantine/` for corrupt payloads (with offset provenance). Never mutated.
- **Glue** catalogs bronze and manifests (Terraform-managed crawlers or explicit table DDL — prefer explicit: crawlers drift, declared schemas are reviewable).
- **dbt + Athena adapter:**
  - **Silver:** parse both schema versions into one conformed model (the drift cutover is a staging concern, invisible downstream); dedupe on `event_id` keeping earliest `publish_time`; event-time vs ingest-time both preserved; late arrivals flagged `is_late` against the window watermark. dbt tests: uniqueness on `event_id`, accepted values, non-null keys, freshness.
  - **Gold:** Kimball star — `dim_machine` and `dim_work_order` as **SCD2** (the generator changes machine configs mid-history to force real SCD2 rows), `dim_product`, `fct_cycles`, `fct_defects`, and window-grained aggregate marts sized to match manifest windows.
  - Incremental models with a **late-data reprocessing window** (recompute trailing N hours each run) — the honest alternative to pretending late data doesn't exist; N is config and its tradeoff (cost vs completeness lag) is a decisions entry *(DMLS Ch. 8, on time-scale windows)*.
- Athena cost discipline: partitioned + columnar everything, no `SELECT *` in models, per-query bytes-scanned logged into the ops data.

## Reconciliation & the Completeness Ledger (Phase 4 — the headline)

A job (dbt model + small runner) joins gold window aggregates to the generator manifests, per window per machine:

- Delta columns per measure; expected state is **all zeros** once a window's watermark has passed (declared: 6 h + reprocessing window).
- Per-disorder-class assertions: duplicates in manifest vs rows in silver (must be deduped), late events flagged, quarantined count == corrupt count injected.
- Output: `completeness_ledger` — one row per window: reconciled exactly / pending (inside watermark) / **broken** (delta after watermark). Broken fails CI and gets a root-cause note in the ledger row itself. No silent re-statements: a window that changes after being marked reconciled is a logged incident.
- The status page renders the ledger as the centerpiece: *"Every 15-minute window since <date>: exact match."* That sentence, backed by public data, is the resume line.

## Infrastructure as Code & CI (Phase 0 + throughout)

- **Terraform** (`infra/terraform/`) owns: S3 buckets (lake, manifests, athena-results; lifecycle rules), Glue databases/tables, Athena workgroup (with per-query bytes-scanned limit), IAM (least-privilege roles), **AWS Budgets alarm** (email at 50%/90% of $10), and the **GitHub OIDC provider + deploy role** — CI assumes a role via OIDC; **no long-lived AWS keys exist in GitHub, in `.env`, or anywhere else.**
- Terraform state: local + gitignored to start; the S3-backend upgrade is a decisions entry when multi-machine need appears.
- CI (GitHub Actions): lint (ruff, sqlfluff) / unit tests (generator determinism, disorder injector rates, consumer idempotency with a dockerized Redpanda service container) / `terraform validate` + `plan` on PR / dbt build + tests against a CI Athena workgroup on merge. Same `make` targets locally and in CI *(PMLO Ch. 1)*.
- Acceptance test, run once and written up: `terraform destroy && terraform apply && make replay && dbt build` — empty AWS account to fully reconciled lake, timed, documented.

## Cost Engineering & Duty Cycle (design constraint, not afterthought)

Always-on streaming on a hobby budget is theater; a **published duty cycle** is honest engineering:

- The generator + broker + consumer run on a schedule (default: 4 h/day, e.g. two shifts' worth of sim time compressed) — enough sustained volume to be real (target ≥ 50–100 events/s during runs; state the measured number), bounded enough to stay ≤ $10/month.
- S3 + Glue + Athena at this volume are cents; the budget alarm is the backstop; the README carries a **measured monthly cost table** (S3 / Athena bytes scanned / requests / total), updated monthly for the first quarter. Publishing the number — and the engineering that keeps it small — is itself the senior signal.
- Scale honesty: state clearly what this volume does and does not prove, and point to the optional scale chapter for the large-data credential.

## Operating in Public (same contract as the other rungs)

- Badge row: CI · dbt tests · reconciliation status ("N/N windows exact") · last-run · cost badge (static, updated with the table).
- Status page (GitHub Pages, regenerated per run): completeness ledger view, consumer lag/telemetry, disorder-injection vs handled counts, run history.
- Rendered architecture image + status-page screenshot in the first screenful; 90-second GIF (generator run → events flowing → ledger going green).
- **"What this demonstrates"** plain-English section — e.g., *"The pipeline is fed deliberately corrupted, duplicated, and late data — and proves, window by window, that nothing was lost or double-counted"* — each bullet paired with the technical term (at-least-once + idempotent writes, watermarking, SCD2, IaC, OIDC).
- Dated stage writeups in `docs/stages/`.

---

## Repository Layout

```
factorystream/
├── README.md
├── Makefile                    # run/replay/deploy/reconcile — same targets in CI
├── pyproject.toml
├── docker-compose.yml          # redpanda, generator, consumer (duty-cycle stack)
├── .github/workflows/          # ci.yml, run.yml (duty-cycle trigger), status_page.yml
├── infra/terraform/            # S3, Glue, Athena, IAM+OIDC, budgets — the whole cloud
├── docs/
│   ├── decisions.md            # every rejected alternative, dated + cited
│   ├── runbooks.md
│   └── stages/
├── src/factorystream/
│   ├── generator/              # sim engine, disorder injector, manifest writer
│   ├── consumer/               # kafka consumer group, parquet writer, quarantine
│   └── recon/                  # ledger runner
├── transform/                  # dbt project: staging → silver → gold (+ recon models)
├── eval/                       # (none — no model layer; reconciliation is the eval)
└── tests/
    ├── test_generator.py       # seeded determinism, injector rates
    ├── test_consumer.py        # idempotent naming, commit-after-write, corrupt payloads
    └── test_recon.py           # known-good and known-broken fixture windows
```

## Milestones

**Phase 0 — Terraform skeleton (weekend 1, first half).** Buckets, IAM, OIDC role, budget alarm, Athena workgroup. `terraform apply` from zero; CI assumes the role and lists the bucket. Cloud plumbing proven before any data exists.

**Phase 1 — Generator + manifest (weekend 1–2).** Sim engine, disorder injector, manifest writer, determinism tests. Deliverable: two runs with the same seed produce identical streams and manifests.

**Phase 2 — Broker + consumer (weekend 3).** Redpanda, keyed topic, consumer group with commit-after-write and idempotent naming, quarantine path, replay target. Deliverable: kill the consumer mid-batch, restart, prove no loss and no duplicates in bronze.

**Phase 3 — dbt lakehouse (weekends 4–5).** Glue tables, silver conform/dedupe/late-flagging across the schema-drift cutover, gold star with real SCD2 rows, dbt tests in CI.

**Phase 4 — Reconciliation ledger + public ops (weekend 6).** Ledger job, status page, badges, duty-cycle scheduling, cost table. Deliverable: the "every window exact" sentence, publicly verifiable.

**Phase 5 — Acceptance + writeup (weekend 7).** Destroy/rebuild/replay acceptance run, timed and documented; README presentation layer finalized.

**Phase 6 — Optional depth (pick ONE):**
- **Scale chapter:** run the dbt models' patterns against a genuinely large public dataset (e.g., NYC TLC trip records, ~1B+ rows, already public parquet) via Athena — partition-projection, bytes-scanned economics, a before/after performance writeup. Cheapest honest route to a big-data credential; EMR/Spark remains rejected (see Non-Goals) unless this chapter *measures* a need.
- **Stream-side aggregation:** a second consumer maintaining live windowed aggregates (pure Python state machine) compared against the batch gold numbers — a materialization-tradeoff writeup *(DMLS Ch. 3)*.
- **Contract testing:** JSON-schema registry for the event contract with producer/consumer CI checks and a v3 evolution exercised end to end.

## Non-Goals (binding)

- **No real factory data, ever** — no employer schemas, rates, machine names, or derived numbers. The generator spec above is the entire domain input. This is a hard boundary, not a sanitation task.
- No Spark/EMR/Databricks in v1 (Athena + dbt covers the warehouse; the optional scale chapter must *measure* a need before any distributed engine enters).
- No Kubernetes; docker compose is the ceiling.
- No LLM/ML layer of any kind — other rungs carry it; reconciliation is this project's "eval."
- No managed Kafka (MSK/Confluent/Kinesis) — cost without additional learning at this scale; the Kafka *API* and its semantics are the transferable asset. Decisions entry, revisit only with a measured driver.
- No 24/7 streaming — the duty cycle is a documented cost decision, not a limitation to hide.
- No multi-cloud, no data egress beyond AWS + the public status page.

## Engineering Conventions

Identical to HelioSentinel/TransportIQ: Python 3.12, ruff + mypy, Makefile as the single automation interface, explicit timeouts, quarantine-never-drop, everything config, pinned versions, secrets via env only (and in CI, via OIDC — no stored cloud keys), conventional commits, one milestone = one PR-sized change set with a dated stage writeup. Plus: sqlfluff for dbt models; every dbt model has at least a uniqueness or not-null test; no `SELECT *` in models.

---

## References

- **[DMLS]** Chip Huyen, *Designing Machine Learning Systems* (O'Reilly, 2022). Batch-vs-stream justification and real-time transport (Ch. 3), monitoring/observability/freshness and time-scale windows (Ch. 8).
- **[PMLO]** Noah Gift & Alfredo Deza, *Practical MLOps* (O'Reilly, 2021). Makefile/CI symmetry and automation (Ch. 1), container and environment reproducibility (Ch. 3), automated checks (Ch. 4), monitoring and metric taxonomy (Ch. 6).
- **[MLDP]** Valliappa Lakshmanan, Sara Robinson & Michael Munn, *Machine Learning Design Patterns* (O'Reilly, 2020). Reproducibility discipline (Ch. 1); Windowed Inference mechanics inform the watermark/window handling (DP 24).

Scope note: **[AIE]** and the modeling texts are deliberately not cited — this rung contains no model layer by binding non-goal; the reconciliation ledger plays the role the eval harness plays elsewhere in the ladder.

## Seed Decision Entries (copy into `docs/decisions.md` at Phase 0)

Format: **Decision** — rejected alternative — why — source.

1. **Redpanda single-node (Kafka API)** — rejected Kinesis/MSK/Confluent — managed streams bill by the hour and teach a narrower API; Kafka semantics (partitions, consumer groups, offsets) are the transferable asset; single-node is honest about scale *(DMLS Ch. 3)*.
2. **Synthetic adversarial generator with ground-truth manifest** — rejected replaying a public dataset — public datasets can't prove completeness (no ground truth) and can't inject disorder at declared rates; the manifest turns correctness from a claim into a checkable equality *(MLDP Ch. 1, "Reproducibility")*.
3. **At-least-once + idempotent writes, called "effectively-once"** — rejected claiming exactly-once — commit-after-durable-write plus offset-ranged object names plus silver dedupe is the real-world pattern; unqualified exactly-once claims are marketing *(DMLS Ch. 3; Ch. 8)*.
4. **Manual offset commit after S3 flush; auto-commit disabled** — rejected auto-commit — the canonical restart-loses-data bug; the consumer test kills the process mid-batch to prove the property.
5. **Explicit Glue table DDL in Terraform** — rejected crawlers — crawlers drift and surprise; declared schemas are code-reviewable contracts.
6. **Incremental dbt with a trailing reprocessing window for late data** — rejected full-refresh-always and rejected ignoring late data — recompute-the-tail is the cost/completeness compromise; window length is config with its tradeoff documented *(DMLS Ch. 8)*.
7. **GitHub OIDC role assumption for CI** — rejected long-lived AWS keys in repo secrets — key leakage is the top cloud failure mode; short-lived federated credentials remove the class of bug *(PMLO Ch. 4, on deployment hygiene)*.
8. **Duty-cycle operation with published cost table** — rejected 24/7 streaming — always-on at hobby scale proves nothing extra and hides cost; scheduled sustained bursts + a real monthly bill is the honest posture.
9. **Athena + dbt as the warehouse; no Spark in v1** — rejected EMR/Spark by default — at this volume a distributed engine is ceremony; the optional scale chapter must measure a need first *(DMLS Ch. 3)*.
10. **Reconciliation ledger failures block CI with root-cause notes** — rejected reconciliation as a dashboard-only metric — a completeness gate that doesn't gate is a decoration *(PMLO Ch. 4 & 6; MLDP DP 25, Trade-Offs)*.
