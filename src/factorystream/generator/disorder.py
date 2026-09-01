"""The disorder injector — the point of the project.

A clean synthetic stream proves nothing: any pipeline handles tidy data. The
hard parts of streaming are late arrivals, duplicates, out-of-order delivery,
clock skew, schema drift, and corrupt payloads, and real pipelines meet them
anecdotally — a bug here, an incident there, never at a known rate.

So this module produces all six **on purpose, at configured rates, and logs
every injection**. That last clause is what makes the pipeline's handling
checkable *by disorder class* rather than only in aggregate: the manifest
records that machine X received exactly 37 duplicate publishes, so silver
deduplicating to exactly the right count is an equality, not a vibe.

Applied post-generation and pre-publish, so the underlying simulation stays
clean and the manifest can be computed from truth before anything is corrupted.

Order of application matters and is deliberate:

1. **clock skew** — mutates `event_time`, so it must precede anything that
   reads event_time (the manifest is computed after this, on the skewed truth,
   because a skewed machine's *reported* time is what the pipeline will see)
2. **schema drift** — a version cutover applied to a contiguous tail
3. **late arrival** — pushes `publish_time` far past `event_time`
4. **out-of-order** — perturbs publish ordering within a key
5. **duplicate** — republishes whole events, after all field mutations so the
   copies are genuinely identical
6. **corrupt** — replaces the serialised payload, last, because a corrupted
   record must not then be mutated further
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import timedelta
from random import Random
from typing import Any

from pydantic import BaseModel, Field

from factorystream.generator.events import SCHEMA_V2, SCHEMA_V3, Event, to_v2, to_v3


class DisorderConfig(BaseModel):
    """Rates are fractions of total events unless noted."""

    model_config = {"extra": "forbid"}

    late_rate: float = Field(default=0.02, ge=0, le=1)
    late_max_hours: float = Field(default=6.0, gt=0)

    duplicate_rate: float = Field(default=0.01, ge=0, le=1)
    duplicate_max_extra: int = Field(default=3, ge=1)

    out_of_order_rate: float = Field(default=0.03, ge=0, le=1)
    out_of_order_max_shift: int = Field(default=12, ge=1)

    # Count of machines given a wandering clock, not a fraction of events.
    skewed_machines: int = Field(default=2, ge=0)
    skew_max_seconds: float = Field(default=90.0, ge=0)

    # A single cutover: every event after this fraction of the run uses v2.
    schema_drift_at: float | None = Field(default=0.5, ge=0, le=1)
    # A SECOND cutover, later in the run, to v3 (`duration_s` -> `duration_ms`).
    #
    # Off by default, and that is a deliberate compromise rather than an
    # oversight. Turning it on changes the published stream, which changes the
    # manifest, which changes every number in the reconciliation evidence and
    # in the README. Those numbers are dated and cited; silently moving them to
    # demonstrate a feature would be the least honest thing in the repository.
    #
    # So v3 is exercised by `tests/test_contracts_end_to_end.py`, which builds
    # its own lake, and the documented run stays reproducible.
    schema_v3_at: float | None = Field(default=None, ge=0, le=1)

    corrupt_rate: float = Field(default=0.001, ge=0, le=1)


@dataclass
class InjectionLog:
    """Exactly what was done, so the pipeline can be graded per class."""

    late_event_ids: set[str] = field(default_factory=set)
    duplicated_event_ids: dict[str, int] = field(default_factory=dict)
    out_of_order_event_ids: set[str] = field(default_factory=set)
    skewed_machines: dict[str, float] = field(default_factory=dict)
    schema_v2_event_ids: set[str] = field(default_factory=set)
    schema_v3_event_ids: set[str] = field(default_factory=set)
    corrupt_event_ids: set[str] = field(default_factory=set)

    def summary(self) -> dict[str, Any]:
        return {
            "late": len(self.late_event_ids),
            # The count of *extra* copies published, not of distinct events —
            # this is the number silver must remove.
            "duplicate_extra_copies": sum(self.duplicated_event_ids.values()),
            "duplicate_distinct_events": len(self.duplicated_event_ids),
            "out_of_order": len(self.out_of_order_event_ids),
            "skewed_machines": dict(self.skewed_machines),
            "schema_v2": len(self.schema_v2_event_ids),
            "schema_v3": len(self.schema_v3_event_ids),
            "corrupt": len(self.corrupt_event_ids),
        }


@dataclass
class PublishRecord:
    """One thing that reaches the broker.

    Corrupt records carry `raw` instead of `event`: the consumer must survive a
    payload it cannot parse, so the injector has to be able to emit bytes that
    are not a valid event at all.
    """

    machine_id: str
    event: Event | None
    raw: str | None = None

    @property
    def is_corrupt(self) -> bool:
        return self.raw is not None


def _injector_seed(run_seed: int) -> int:
    digest = hashlib.blake2b(f"disorder:{run_seed}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def inject(
    events: list[Event], config: DisorderConfig, run_seed: int
) -> tuple[list[PublishRecord], InjectionLog, list[Event]]:
    """Apply every disorder class in the documented order.

    Returns three things:

    - the publish-order record list (what reaches the broker)
    - the log of what was done, per class
    - the **canonical stream**: post-skew, post-drift, pre-publish

    That third return exists because of a bug this design had at first. The
    manifest was being computed from the pristine pre-injection events, but
    clock skew mutates `event_time` — so a skewed machine's events landed in
    one window in bronze and a different window in the manifest, and
    reconciliation failed on two machines for a reason that had nothing to do
    with the pipeline.

    The rule: **the manifest describes what the machine reported.** A machine
    with a broken clock genuinely believes the wrong time, the pipeline sees
    that time, and grading the pipeline against a corrected time it never saw
    would fail it for the generator's fiction. Skew and schema drift are
    properties of the record; lateness, reordering, duplication and corruption
    are properties of the transport. The manifest is computed after the former
    and before the latter.

    The input list is never mutated — events are deep-copied first.
    """
    rng = Random(_injector_seed(run_seed))
    log = InjectionLog()

    working = [e.model_copy(deep=True) for e in events]

    # --- record-level mutations: these belong in the manifest ---
    working = _apply_clock_skew(working, config, rng, log)
    working = _apply_schema_drift(working, config, log)

    # Snapshot before any transport-level disorder. Ordered by event time so
    # the manifest is computed from a stable sequence.
    canonical = [
        e.model_copy(deep=True)
        for e in sorted(working, key=lambda e: (e.event_time, e.machine_id, e.event_id))
    ]

    # --- transport-level disorder: NOT reflected in manifest counts ---
    working = _apply_late_arrival(working, config, rng, log)

    # Publish order starts as publish_time order — which, after lateness, is
    # already not event_time order.
    working.sort(key=lambda e: (e.publish_time, e.machine_id, e.event_id))

    working = _apply_out_of_order(working, config, rng, log)
    records = _apply_duplicates(working, config, rng, log)
    records = _apply_corruption(records, config, rng, log)

    return records, log, canonical


def _apply_clock_skew(
    events: list[Event], config: DisorderConfig, rng: Random, log: InjectionLog
) -> list[Event]:
    """Give a few machines a wandering clock.

    This is the disorder that most resembles a real plant: one controller's NTP
    is broken and nobody has noticed. It mutates `event_time` only — the
    machine genuinely believes that is when the event happened — so the
    event-time-vs-ingest-time gap becomes observable downstream.
    """
    if config.skewed_machines <= 0 or config.skew_max_seconds <= 0:
        return events

    machine_ids = sorted({e.machine_id for e in events})
    chosen = rng.sample(machine_ids, min(config.skewed_machines, len(machine_ids)))
    # Constant offset per machine plus a slow wander, rather than per-event
    # noise: a broken clock is wrong consistently, which is what makes it hard
    # to spot and worth simulating properly.
    offsets = {m: rng.uniform(-config.skew_max_seconds, config.skew_max_seconds) for m in chosen}
    log.skewed_machines = {m: round(v, 3) for m, v in offsets.items()}

    for event in events:
        base = offsets.get(event.machine_id)
        if base is None:
            continue
        wander = rng.uniform(-0.1, 0.1) * config.skew_max_seconds
        event.event_time = event.event_time + timedelta(seconds=base + wander)
    return events


def _apply_schema_drift(
    events: list[Event], config: DisorderConfig, log: InjectionLog
) -> list[Event]:
    """One cutover, mid-history — not a random scatter.

    Real schema changes are deployments: everything before is v1, everything
    after is v2. A random per-event mix would be easier for a pipeline to
    tolerate and would not resemble anything that happens.
    """
    if config.schema_drift_at is None or not events:
        return events

    ordered = sorted(events, key=lambda e: e.event_time)
    cut = int(len(ordered) * config.schema_drift_at)
    for event in ordered[cut:]:
        event.schema_version = SCHEMA_V2
        event.payload = to_v2(event.payload, event.event_type)
        log.schema_v2_event_ids.add(event.event_id)

    # The v3 cutover, if enabled, lands inside the v2 tail: a deployment
    # supersedes the one before it rather than branching from the original.
    if config.schema_v3_at is not None:
        cut3 = int(len(ordered) * config.schema_v3_at)
        for event in ordered[cut3:]:
            event.schema_version = SCHEMA_V3
            event.payload = to_v3(event.payload, event.event_type)
            log.schema_v3_event_ids.add(event.event_id)

    return events


def _apply_late_arrival(
    events: list[Event], config: DisorderConfig, rng: Random, log: InjectionLog
) -> list[Event]:
    """Push publish_time hours past event_time.

    Stresses watermarks and window re-statement: the window these belong to has
    long since been aggregated by the time they land.
    """
    for event in events:
        if rng.random() >= config.late_rate:
            continue
        # Skewed toward small delays with a long tail — most late data is a
        # little late; the six-hour stragglers are what break assumptions.
        hours = config.late_max_hours * (rng.random() ** 2)
        event.publish_time = event.publish_time + timedelta(hours=hours)
        log.late_event_ids.add(event.event_id)
    return events


def _apply_out_of_order(
    events: list[Event], config: DisorderConfig, rng: Random, log: InjectionLog
) -> list[Event]:
    """Swap events backwards within their own partition key.

    Deliberately **within a machine**, because that is the ordering Kafka
    actually guarantees and therefore the assumption a pipeline is most likely
    to lean on. Reordering across machines would prove nothing — nothing is
    entitled to that ordering in the first place.
    """
    if config.out_of_order_rate <= 0:
        return events

    by_machine: dict[str, list[int]] = {}
    for index, event in enumerate(events):
        by_machine.setdefault(event.machine_id, []).append(index)

    reordered = list(events)
    for positions in by_machine.values():
        for slot, index in enumerate(positions):
            if rng.random() >= config.out_of_order_rate or slot == 0:
                continue
            back = rng.randint(1, min(config.out_of_order_max_shift, slot))
            target = positions[slot - back]
            reordered[index], reordered[target] = reordered[target], reordered[index]
            log.out_of_order_event_ids.add(events[index].event_id)
    return reordered


def _apply_duplicates(
    events: list[Event], config: DisorderConfig, rng: Random, log: InjectionLog
) -> list[PublishRecord]:
    """Republish whole events, unchanged.

    Applied after every field mutation so the copies are byte-identical to the
    original — which is what a broker retry actually produces, and what makes
    `event_id` a usable dedupe key.
    """
    records: list[PublishRecord] = []
    for event in events:
        records.append(PublishRecord(machine_id=event.machine_id, event=event))
        if rng.random() >= config.duplicate_rate:
            continue
        extra = rng.randint(1, config.duplicate_max_extra)
        log.duplicated_event_ids[event.event_id] = extra
        for _ in range(extra):
            # A retry lands slightly later than the original.
            copy = event.model_copy(deep=True)
            copy.publish_time = copy.publish_time + timedelta(
                milliseconds=rng.randint(5, 900)
            )
            records.append(PublishRecord(machine_id=copy.machine_id, event=copy))
    return records


_TRUNCATIONS = (
    '{"event_id": "',
    '{"event_id": "abc", "event_type": "cyc',
    '{"machine_id": ',
    "{",
    "",
)


def _apply_corruption(
    records: list[PublishRecord], config: DisorderConfig, rng: Random, log: InjectionLog
) -> list[PublishRecord]:
    """Replace some payloads with unparseable bytes.

    Applied last, so nothing mutates a record after it has been corrupted. The
    consumer must quarantine these with offset provenance and keep going —
    never silently drop, and never die.

    The machine_id survives on the record because the *broker* still has the
    partition key even when the value is garbage; that is how a real corrupt
    message arrives.
    """
    if config.corrupt_rate <= 0:
        return records

    out: list[PublishRecord] = []
    for record in records:
        if record.event is not None and rng.random() < config.corrupt_rate:
            log.corrupt_event_ids.add(record.event.event_id)
            out.append(
                PublishRecord(
                    machine_id=record.machine_id,
                    event=None,
                    raw=rng.choice(_TRUNCATIONS),
                )
            )
            continue
        out.append(record)
    return out
