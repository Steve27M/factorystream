# factorystream

**A streaming manufacturing lakehouse that does not claim correctness — it proves it,
by reconciling every window against ground truth the generator wrote down before the
pipeline ever saw it.**

An adversarial event generator (late, duplicate, out-of-order, clock-skewed and
schema-drifting events, **on purpose, at declared rates**) → a Kafka-API broker → S3 parquet
→ Glue + Athena → dbt into a Kimball star → **exact reconciliation against a per-window
manifest**. All AWS resources are Terraform; nothing is created by console.

> **Status:** Phase 1 and the AWS medallion complete. 78 tests, **523 of 523 windows
> reconciling exactly**. Later phases (broker semantics depth, operate-in-public) are
> not built yet — the spec is [`README-factorystream.md`](README-factorystream.md) and
> what is done is dated in [`docs/stages/`](docs/stages).

Built in parallel with **WearWatch** on a shared plant canon — one fictional factory,
simulated by both, read from [`plant/canon.yaml`](plant/canon.yaml). The two are
deliberately **not merged**: each spec binds the other's territory as a Non-Goal. This
rung prosecutes streaming plumbing; that one consumes it.

---

## The claim, and the arithmetic behind it

Most pipelines assert correctness. Very few can prove it, because ground truth is
unknowable — you cannot diff against the world. A synthetic generator *knows exactly what
it emitted*, so it writes a per-window manifest of counts, sums and checksums, and the
gold layer must reconcile against it exactly.

```
windows  events_exact  bronze_deduped  manifest_events  corrupt  dup_extra  late
    523           523            4151             4155        4         92     85
```

**523 of 523 windows exact**, and the arithmetic closes with nothing left over:

```
manifest events            4,155
  − corrupt (quarantined)      4
  = deduped bronze         4,151   ✓ matches Athena
  + duplicate extra copies    92
  = raw bronze             4,243   ✓ matches Athena
```

Every row is attributable to a disorder class that was **injected deliberately at a
configured rate**. That is the difference this repo exists to demonstrate: not "the counts
look about right", but "the counts are exactly what they must be, and here is why each one
is not what a naive count would give".

The manifest never travels the pipeline. It is written straight to storage by the
generator, so reconciliation is a comparison against known truth rather than a self-check
— the same discipline that lets a test be a test rather than a restatement.

## Why the disorder is manufactured

The hard parts of streaming are not the broker. They are late data, duplicates, ordering
and exactly-once claims — and a generator that emitted clean events would make every one
of those untestable.

| injected | at a declared rate | what it forces |
|---|---|---|
| **late** events | arriving after their window closed | watermarking, and a late-arrival policy that is stated rather than implied |
| **duplicates** | same event, more than once | idempotent dedup, and a bronze layer that keeps both so the count is explainable |
| **out-of-order** | timestamps that go backwards | ordering that cannot rely on arrival sequence |
| **clock skew** | producer clocks that disagree | event time vs. ingest time kept as separate columns, never reconciled away |
| **corrupt** | malformed payloads | a quarantine path, so bad rows are *counted* rather than silently dropped |

Each one appears in the reconciliation above as a named quantity. A disorder class you
cannot point to in the arithmetic is a disorder class you have not actually handled.

## Layout

```
src/factorystream/generator/   adversarial synthetic telemetry + ground-truth manifests
src/factorystream/consumer/    broker consumer, offsets, dedup, quarantine
transform/                     dbt: staging -> silver -> gold -> recon
infra/terraform/account/       account-level guardrails: budgets, CUR, shared workgroup
infra/terraform/app/           the pipeline's own resources
tools/                         teardown_verify.py, build_status_page.py
evidence/                      dated ledger, including the first exact reconciliation
plant/canon.yaml               the shared fictional factory
docs/                          decisions.md, stages/, status/
```

## Account guardrails live here

Budgets, the Cost and Usage Report, and the shared Athena workgroup with its per-query
scan cap are in `infra/terraform/account/` and cover the whole account — including
WearWatch's resources. That is deliberate: account-level budgets are account-level, and
two Terraform states both claiming one budget is a fight rather than a safety net.

`tools/teardown_verify.py` sweeps 17 idle-billable resource types across every region.
Run it at the end of every session:

```bash
python tools/teardown_verify.py --all-regions
```

## Running it

```bash
make help                  # the entry points
pytest                     # 78 tests
pytest -m integration      # needs the broker (make broker)
```

## Licence

[MIT](LICENSE). The plant, the machines, and every event are simulated. No real
manufacturer's data, tooling, or process parameters appear anywhere in this repository —
a binding non-goal in the spec, not a sanitisation exercise.
