# Decisions

Every rejected alternative, dated and cited. Format:
**Decision** — rejected alternative — why — source.

Citation keys: **[DMLS]** Huyen, *Designing ML Systems* (2022) · **[PMLO]** Gift &
Deza, *Practical MLOps* (2021) · **[MLDP]** Lakshmanan, Robinson & Munn, *ML
Design Patterns* (2020). **[AIE]** is deliberately absent — this rung carries no
model layer by binding non-goal, and the reconciliation ledger plays the role the
eval harness plays elsewhere in the ladder.

Entries are append-only. A reversed decision gets a new entry that supersedes the
old one rather than an edit to history.

---

## Seed entries (from the build spec, adopted at Phase 0)

**1 — Redpanda single-node, Kafka API** — rejected Kinesis / MSK / Confluent —
managed streams bill by the hour and teach a narrower API; partitions, consumer
groups and offsets are the transferable asset, and single node is honest about
scale *(DMLS Ch. 3)*.

**2 — Synthetic adversarial generator with a ground-truth manifest** — rejected
replaying a public dataset — public data cannot prove completeness (no ground
truth) and cannot inject disorder at declared rates; the manifest turns
correctness from a claim into a checkable equality *(MLDP Ch. 1)*.

**3 — At-least-once + idempotent writes, called "effectively-once"** — rejected
claiming exactly-once — commit-after-durable-write plus offset-ranged object
names plus silver dedupe is the real pattern; unqualified exactly-once is
marketing *(DMLS Ch. 3, Ch. 8)*.

**4 — Manual offset commit after the S3 flush; auto-commit disabled** — rejected
auto-commit — the canonical restart-loses-data bug. Proven by killing the
consumer mid-batch (`tests/test_integration_consumer.py`).

**5 — Explicit Glue table DDL in Terraform** — rejected crawlers — crawlers drift
and mutate a schema under a running build; declared schemas are code-reviewable
contracts that fail loudly.

**6 — Incremental dbt with a trailing reprocessing window** — rejected
full-refresh-always and rejected ignoring late data — recompute-the-tail is the
cost/completeness compromise; the window is `var('late_arrival_window_hours')`
*(DMLS Ch. 8)*.

**7 — GitHub OIDC role assumption for CI** — rejected long-lived AWS keys in repo
secrets — key leakage is the top cloud failure mode; short-lived federated
credentials remove the class of bug *(PMLO Ch. 4)*.

**8 — Duty-cycle operation with a published cost table** — rejected 24/7
streaming — always-on at hobby scale proves nothing extra and hides cost.

**9 — Athena + dbt as the warehouse; no Spark in v1** — rejected EMR/Spark by
default — at this volume a distributed engine is ceremony *(DMLS Ch. 3)*.

**10 — Reconciliation failures block CI with root-cause notes** — rejected
reconciliation as a dashboard-only metric — a completeness gate that does not
gate is a decoration *(PMLO Ch. 4 & 6; MLDP DP 25)*.

---

## Implementation decisions

### 11 — Bronze partitions on ingest time, not event time
*2026-08-19*

Rejected partitioning bronze by `event_time`. The injector publishes events up to
six hours late; partitioning on event time means a late arrival must be written
into a partition closed hours earlier, which breaks both immutability and the
idempotent object naming that makes replay safe.

Partitioning on arrival keeps bronze append-only. Silver re-keys to event time,
which is where that logic belongs. *(DMLS Ch. 8, on time-scale windows)*

### 12 — Partition projection instead of a partition registry
*2026-08-19*

Rejected `MSCK REPAIR` and `ALTER TABLE ADD PARTITION`. A query against an
unregistered partition returns **nothing, silently** — the worst possible failure
for a completeness thesis, because the pipeline looks broken when only the
catalog is stale. Projection computes locations from the query predicate, so a
partition exists the moment its objects do.

### 13 — The manifest is built from the canonical stream, not the pristine one
*2026-08-19*

Rejected computing the manifest before injection.

Clock skew mutates `event_time`. Building the manifest from pre-injection events
meant a skewed machine reported one window while ground truth recorded another,
and reconciliation failed on exactly the two skewed machines — 480 of 521 windows
matching, for a reason that had nothing to do with the pipeline.

**The manifest describes what the machine reported.** A broken clock genuinely
believes the wrong time, the pipeline sees that time, and grading against a
corrected time it never saw would fail the pipeline for the generator's fiction.

`inject()` therefore returns a third value: the canonical stream, after
record-level mutations (skew, schema drift) and before transport-level ones
(lateness, reordering, duplication, corruption).

### 14 — Injected disorder is attributed per window AND per measure
*2026-08-19*

Rejected recording only aggregate injection totals.

The first iteration recorded `corrupt_count` per window, and the ledger
subtracted it from the expected event count. Four windows still reported broken
with a strange signature: `event_delta = 0` but `unit_delta = -1`.

The cause: all four corrupted events were cycles, and a corrupted cycle removes
one event, **one unit, and its duration** from what the warehouse can observe. A
single total cannot express that. The manifest now carries `corrupt_cycle_count`,
`corrupt_defect_count` and `corrupt_duration_sum_s`, and each expected value gets
its own adjustment.

This is what "checkable by disorder class, not just in aggregate" requires in
practice. Note what a looser gate would have done: with a one-row tolerance all
four windows would have passed silently and the accounting error would still be
in the code.

### 15 — The ledger separates pending from broken
*2026-08-19*

Rejected a two-state reconciled/broken ledger.

A window inside the watermark may still receive late data, so divergence there is
expected rather than wrong. A two-state ledger either fires constantly on fresh
windows or suppresses real breaks by widening its tolerance — and a gate that
cries wolf gets switched off. Separating "too early to tell" from "wrong" is what
makes it trustworthy enough to leave enabled.

### 16 — FULL OUTER JOIN between gold and the manifest
*2026-08-19*

Rejected an inner join. An inner join silently drops the two failures that matter
most: a window the pipeline lost entirely, and a window the pipeline invented.
Both must surface as broken rather than vanish from the report.

### 17 — Idle polls are not counted until partitions are assigned
*2026-08-19*

Rejected counting empty polls from the first poll.

Joining a consumer group takes several seconds during which every poll returns
`None`. A bounded duty-cycle run hit its idle threshold **before it was ever
assigned a partition**, consumed zero records, and exited reporting success.
Silently processing nothing while claiming completion is the worst available
failure mode.

### 18 — Integration tests are excluded from the default test run
*2026-08-19*

Rejected running broker-dependent tests by default. A unit suite that requires
Docker is a unit suite people stop running. `pytest` runs 78 tests with no
external dependency; `make test-integration` runs the kill-test against a live
broker.

### 19 — tzdata is a runtime dependency, not a dev extra
*2026-08-19*

Windows ships no system timezone database, so pyarrow cannot read back a
tz-aware timestamp without it. Omitting it means parquet round-trips pass on
Linux CI and fail on a developer laptop — an environment-dependent bug, which is
the worst kind to diagnose.

### 20 — A bootstrap loader exists alongside the consumer
*2026-08-19*

`consumer/load.py` feeds bronze directly from a generator file, bypassing the
broker. Rejected making it the production path, and rejected deleting it.

It exists so the lake, catalog and Athena could be proven before Redpanda was
running, and it feeds the **same `BatchWriter`** the consumer uses — so the
partitioning and naming under test are the identical code. It synthesises offsets
by mirroring Kafka's default partitioner (`crc32(machine_id) % 6`), which is
structurally faithful but not literally from a broker, and the module docstring
says so.

### 21 — Bronze keeps duplicates; silver removes them
*2026-08-19*

Rejected deduplicating at ingest.

Bronze is *raw as landed*. An integration test initially asserted zero duplicate
`event_id` values in bronze and failed with exactly 92 — precisely the number the
injector publishes on purpose. It was measuring the generator, not the consumer.

Worse, a version that had passed by deduplicating in bronze would have quietly
destroyed the evidence the ledger depends on: the ledger asserts that silver
removed *exactly* the injected extras, which is unprovable if they never landed.

### 22 — dim_machine is built from the event stream, not from the canon
*2026-08-19*

Rejected sourcing the machine dimension from `plant/canon.yaml`.

A dimension read from the config file would agree with the manifest by
construction and prove nothing. Sourcing it from the stream means a machine that
stopped reporting shows up as a machine that stopped reporting.

SCD2 is deferred to Phase 3b, once the generator changes machine configs
mid-history. Modelling slowly-changing rows before anything changes is ceremony —
there would be exactly one version of every row.

### 23 — Athena workgroup enforcement OFF; the scan cap stays
*2026-08-19*

Rejected `enforce_workgroup_configuration = true`, which was the original
setting and looked like the safer choice.

Enforcement forces the workgroup's `ResultConfiguration` onto every query, and
that **also overrides the `external_location` dbt sets on a CTAS**. Every
materialised model therefore landed in the Athena *results* bucket rather than
the lake — and that bucket carries a 7-day expiration rule, because query
results are scratch.

Silver, gold and the completeness ledger would have silently disappeared a week
later. No error, no failed build: queries would simply have started returning
nothing, and the ledger would have looked like a pipeline failure rather than a
lifecycle one.

Nothing is given up by disabling it:

- `bytes_scanned_cutoff_per_query` is a workgroup-level limit that no client can
  supply, so there is nothing for enforcement to protect. Verified still active
  after the change.
- Encryption is configured on the **bucket**, not only on the workgroup, so it
  applies regardless of what any client asks for.

Two related findings worth recording, because both cost time:

**The adapter's own docstring is wrong.** `_s3_table_prefix` claims it falls back
to the connection's `s3_data_dir`; the Python only reads the model-level value
and otherwise returns `s3_staging_dir/tables`. The Jinja macro *does* read
`target.s3_data_dir`, so a profile setting works for tables but the docstring
cannot be trusted about the mechanism.

**Materialised tables and query scratch must not share a bucket.** Even with the
placement fixed, the lesson stands: a lifecycle rule written for scratch will
happily delete a warehouse if anything lands under the same prefix. The
`external_location` a table declares should be somewhere no expiry rule points.

---

## Phase 6 — contract testing chosen over the scale chapter

The spec offers three optional-depth chapters and says pick one.

**Contract testing, because this project already manufactures the conditions
for it.** The generator performs a schema cutover halfway through every run, so
there is a stream that genuinely changes shape while it is being consumed. A
contract chapter written against a stream that never drifts is a chapter about a
hypothetical.

**The scale chapter was the tempting pick and is the wrong one here.** Running
these dbt patterns against ~1B rows of NYC TLC data via Athena costs real money
to demonstrate something the Non-Goals already decline to claim, and the spec is
explicit that a distributed engine may enter only if a chapter *measures* a
need. Nothing has measured one, so buying the credential would be buying it
rather than earning it.

Stream-side aggregation is the runner-up and remains open.

## A registry answers one question, and it is not "is this correct"

The question is *would this change break a consumer that is already running* —
answerable mechanically, before a change ships. That is why the schemas are
files a build can read rather than prose in a README: a document cannot fail a
build.

Compatibility directions, named carefully because they are easy to state
backwards:

- **BACKWARD** — a consumer on the *new* schema reads *old* data. Matters when
  the producer moves first.
- **FORWARD** — a consumer on the *old* schema reads *new* data. Matters when
  consumers cannot all be upgraded at once, which is always.

A rename is not a distinct rule in the checker. It is a removal plus an
addition, so it breaks both directions — which is exactly why renames hurt, and
the checker derives that rather than being told it.

## The blind spot is documented, because a quiet registry is worse than none

v3 replaces `duration_s` (seconds, float) with `duration_ms` (milliseconds,
integer), and the checker correctly calls it breaking.

Now the change it **cannot** see: keeping the name `duration_s` and changing
only the unit to milliseconds. Every schema check passes, every type check
passes, every row validates, and every cycle-time aggregate downstream is wrong
by a factor of a thousand. **JSON Schema describes shape; a unit is meaning.**

`registry.SEMANTIC_CHANGES` lists three such changes by hand — the unit change,
a redefined enum meaning, a timezone convention change — because no mechanism
could derive them. `test_a_unit_change_that_keeps_the_name_passes_every_check`
asserts the gap exists: a test whose *passing* is the bad news, written down so
the limit is not forgotten by someone reading a green suite.

A registry that implies full coverage converts an unknown risk into a
believed-absent one, which is a worse position than having no registry at all.

## The contract gate runs at the producer, and skips corrupt records

Two decisions inside one check.

**At the producer**, before anything reaches the topic: a bad record on the
topic becomes somebody else's quarantine investigation, and the cost of catching
it here is one exit code (exit 2, refusing to publish).

**Corrupt records are skipped rather than failed.** They are unparseable by
construction, emitted on purpose at a declared rate. Counting them as contract
violations would make the gate fire whenever the disorder injector works
correctly — the fastest way to get a check switched off by the person it keeps
interrupting.

Each event is validated against **the version it declares**, not against v1, for
the same reason: the stream is supposed to change version mid-run.

The gate's output also cross-checks the reconciliation from an independent
direction — 4,243 parseable + 4 corrupt = 4,247 lines, and 4,151 deduped + 92
injected duplicates = 4,243. Two tools counting different things and agreeing.
