# Evidence — Phase 2: broker, consumer, and the kill-test

**Date:** 2026-08-19 · Redpanda v24.2.7, single node, docker compose

The Phase 2 deliverable, verbatim from the spec: *"kill the consumer mid-batch,
restart, prove no loss and no duplicates in bronze."* Done, against a live
broker.

---

## The stack

```
factory.events — 6 partitions, replicas 1, keyed by machine_id
```

Partition spread from a real publish (4,247 records):

```
PARTITION  HIGH-WATERMARK
0          0
1          372
2          0
3          2042
4          1326
5          507
```

Two partitions empty is correct, not a bug: eight machines hash across six
partitions, so some partitions take several machines and some take none. Keying
buys **per-machine ordering within a partition and nothing more** — which is
exactly what Kafka guarantees, and why the out-of-order injector exists.

## The commit contract, proven

```
consumed   4,247
  bronze   4,243 rows
  quarantine   4 rows
  objects      5
  commits      4
```

| Property | Test | Result |
|---|---|---|
| Committed work is not redone | re-run same group | **0 consumed** |
| Replay does not duplicate | fresh group, offset 0 | 4,247 re-consumed → **still 5 objects, still 4,243 rows** |
| A crash loses nothing | `kill()` mid-batch, restart | **0 events lost** |
| A crash duplicates nothing | same | row count matches published exactly |
| A crash changes nothing | clean run vs crashed run | **identical bronze** |

`kill()` rather than a signal, deliberately — a graceful shutdown would flush and
commit, which is the behaviour the test exists to *not* exercise.

## Two real bugs the integration tests caught

Both would have passed every unit test.

### 1. The consumer could exit before it was ever assigned a partition

`--stop-after-idle` counted empty polls from the first poll. But joining a
consumer group and being assigned partitions takes several seconds, and every
poll during that window returns `None` — so a bounded run hit the idle threshold
**before receiving an assignment**, consumed zero records, and exited reporting
success.

Silently processing nothing while claiming to have finished is the worst
available failure mode. Idle now only counts once partitions are held.

### 2. The test measured the generator, not the consumer

First version asserted bronze contained zero duplicate `event_id`s. It failed
with exactly 92 — precisely the number of duplicate copies the disorder injector
publishes on purpose.

Bronze is *raw as landed*; those duplicates belong there and silver removes them.
The correct bar is that the consumer adds none of its own, so the assertion is
now row count against published count. A test that had "passed" by deduplicating
in bronze would have quietly destroyed the dedupe evidence the ledger depends on.

## Running it

```
make broker            # redpanda + topic, waits for cluster health
make generate          # 4,247 records with injected disorder
make publish           # produce, keyed by machine_id, acks=all
make consume ROOT=out/lake
make test-integration  # the kill-test — needs the broker up
```

Integration tests are excluded from the default `pytest` run: a unit suite that
needs Docker is a unit suite people stop running.

## Cost

$0.00 — everything on this page ran locally.
