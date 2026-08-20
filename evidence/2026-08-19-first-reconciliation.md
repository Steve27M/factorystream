# Evidence — first exact reconciliation

**Date:** 2026-08-19 UTC · **Account:** `867207177469` · Athena workgroup `plant-platform`

The project's headline claim, proven end to end against a real lake for the
first time: **every 15-minute window reconciles exactly against generator ground
truth.**

---

## Result

```
windows  events_exact  bronze_deduped  manifest_events  corrupt  dup_extra  late
    523           523            4151             4155        4         92     85
```

**523 of 523 windows exact.** The arithmetic closes with nothing unexplained:

```
manifest events            4,155
  − corrupt (quarantined)      4
  = deduped bronze         4,151   ✓ matches Athena
  + duplicate extra copies    92
  = raw bronze             4,243   ✓ matches Athena
```

Every row is attributable to a disorder class that was injected on purpose at a
declared rate. This is the difference the project exists to demonstrate: not
"the counts look about right", but "the counts are exactly what they must be,
and here is why each one differs."

## Path proven

```
generator → disorder injector → JSONL
         ↘ manifest (bypasses the pipeline entirely)

JSONL → BatchWriter → S3 parquet (bronze/ + quarantine/)
     → Glue catalog (explicit DDL, partition projection)
     → Athena
```

Scanned 317,940 bytes — comfortably inside the 2 GiB workgroup cap.

## Two design bugs this run exposed

Both were found *by* the reconciliation failing, which is the check doing its
job. Neither would have produced an error anywhere else.

### 1. The manifest was computed before clock skew

First run: 480/521 windows matched, and the failures concentrated on
`MW1-A-01` (29/61) and `MW1-B-01` (61/67) — **exactly the two machines the
injector had skewed.**

The manifest was built from the pristine pre-injection stream, but clock skew
mutates `event_time`. A skewed machine therefore reported one window while the
manifest recorded another.

The rule this establishes: **the manifest describes what the machine
*reported*.** A machine with a broken clock genuinely believes the wrong time,
the pipeline sees that time, and grading the pipeline against a corrected time
it never saw would fail it for the generator's fiction.

`inject()` now returns a third value — the **canonical stream**: after skew and
schema drift (which mutate the record) and before lateness, reordering,
duplication and corruption (which are transport phenomena). The manifest is
built from that.

### 2. Corrupt events were unattributable

Four events were corrupted before publish and quarantined, so four windows were
short by one — correct behaviour, but indistinguishable from data loss.

The manifest now carries per-window injection counts (`corrupt_count`,
`duplicate_extra_count`, `late_count`), making each class checkable
individually:

```
deduped bronze = event_count − corrupt_count
raw bronze     = event_count − corrupt_count + duplicate_extra_count
```

The spec asked for disorder handling to be "checkable by class, not just in
aggregate." This is what that requires in practice.

## Cost

$0.00. Three Athena queries scanning well under a megabyte; S3 holds 578 KB
across 72 objects.

## Not yet done

- **The broker.** Bronze was landed by `consumer/load.py`, a documented
  bootstrap path that feeds the *same* `BatchWriter` the Kafka consumer will
  use. Offsets are synthesised by mirroring Kafka's default partitioner
  (`crc32(machine_id) % 6`), so the layout is structurally right but not
  literally from a broker. Phase 2 replaces the feeder, not the writer.
- **Silver and gold.** The dedupe above is a query, not a dbt model.
- **The ledger as an artifact.** Reconciliation is currently an ad-hoc query;
  Phase 4 makes it a model that writes `completeness_ledger` and fails CI.
