"""Generator: canon integrity, determinism, and simulation sanity.

Determinism is the Phase 1 deliverable and the foundation of everything above
it — the reconciliation thesis only means something if a run can be reproduced.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, timedelta

import pytest
import yaml

from factorystream.generator.canon import Canon, load_canon
from factorystream.generator.events import EventType
from factorystream.generator.sim import generate

START = date(2026, 3, 2)


@pytest.fixture(scope="module")
def canon() -> Canon:
    return load_canon()


@pytest.fixture(scope="module")
def events(canon: Canon) -> list:
    return generate(canon, seed=42, start=START, days=1)


def _digest(evts: list) -> str:
    return hashlib.sha256(
        json.dumps([e.to_wire() for e in evts], sort_keys=True).encode()
    ).hexdigest()


# --- Canon -------------------------------------------------------------------


def test_canon_loads_and_validates(canon: Canon) -> None:
    assert canon.canon_version == 1
    assert len(canon.machines) == 8
    assert {ln.id for ln in canon.lines} == {"A", "B"}


def test_every_machine_belongs_to_a_line(canon: Canon) -> None:
    for machine in canon.machines:
        assert machine.id in {m.id for m in canon.machines_on(machine.line)}


def test_every_line_has_products(canon: Canon) -> None:
    """A line with no products would silently generate nothing."""
    for line in canon.lines:
        assert canon.products_on(line.id), f"line {line.id} has no products"


def test_integrity_check_rejects_a_dangling_machine_reference(tmp_path) -> None:
    """The check exists because this bug looks like a modelling choice."""
    raw = yaml.safe_load(
        (load_canon.__globals__["CANON_PATH"]).read_text(encoding="utf-8")
    )
    raw["lines"][0]["machines"].append("MW1-A-99")
    path = tmp_path / "broken.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown machines"):
        load_canon(path)


def test_integrity_check_rejects_an_orphan_machine(tmp_path) -> None:
    raw = yaml.safe_load(
        (load_canon.__globals__["CANON_PATH"]).read_text(encoding="utf-8")
    )
    raw["lines"][0]["machines"].remove("MW1-A-01")
    path = tmp_path / "orphan.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="belong to no line"):
        load_canon(path)


# --- Determinism — the Phase 1 deliverable -----------------------------------


def test_same_seed_produces_an_identical_stream(canon: Canon) -> None:
    a = generate(canon, seed=42, start=START, days=1)
    b = generate(canon, seed=42, start=START, days=1)
    assert _digest(a) == _digest(b)


def test_different_seed_produces_a_different_stream(canon: Canon) -> None:
    a = generate(canon, seed=42, start=START, days=1)
    b = generate(canon, seed=43, start=START, days=1)
    assert _digest(a) != _digest(b)


def test_event_ids_are_deterministic_not_random(canon: Canon) -> None:
    """`uuid4()` here would break reproducibility on every run."""
    a = generate(canon, seed=7, start=START, days=1)
    b = generate(canon, seed=7, start=START, days=1)
    assert [e.event_id for e in a] == [e.event_id for e in b]


def test_a_machine_stream_does_not_depend_on_other_machines(canon: Canon) -> None:
    """Per-machine RNG seeding, verified.

    With one shared RNG, each machine's stream would depend on how the others
    were interleaved — and adding a machine to the canon would reshuffle
    everything. This asserts the isolation that prevents that.
    """
    full = generate(canon, seed=42, start=START, days=1)
    target = "MW1-B-02"
    from_full = [e.to_wire() for e in full if e.machine_id == target]

    trimmed = canon.model_copy(deep=True)
    trimmed.machines = [m for m in trimmed.machines if m.id in {target, "MW1-B-01"}]
    trimmed.lines = [ln for ln in trimmed.lines if ln.id == "B"]
    trimmed.lines[0].machines = [target, "MW1-B-01"]
    partial = generate(trimmed, seed=42, start=START, days=1)
    from_partial = [e.to_wire() for e in partial if e.machine_id == target]

    assert from_full == from_partial


# --- Simulation sanity -------------------------------------------------------


def test_all_event_types_are_produced(events: list) -> None:
    kinds = {e.event_type for e in events}
    assert kinds == set(EventType)


def test_events_are_sorted_by_event_time(events: list) -> None:
    times = [e.event_time for e in events]
    assert times == sorted(times)


def test_publish_time_is_never_before_event_time(events: list) -> None:
    """Pre-injection, publish always follows the event."""
    for event in events:
        assert event.publish_time >= event.event_time


def test_every_event_has_a_partition_key(events: list, canon: Canon) -> None:
    known = {m.id for m in canon.machines}
    assert all(e.machine_id in known for e in events)


def test_event_ids_are_unique_before_duplication(events: list) -> None:
    ids = [e.event_id for e in events]
    assert len(ids) == len(set(ids))


def test_faster_machines_produce_more_cycles(events: list, canon: Canon) -> None:
    """Cycle time should drive throughput — otherwise the sim is decorative."""
    cycles = Counter(e.machine_id for e in events if e.event_type is EventType.CYCLE)
    fastest = min(canon.machines, key=lambda m: m.nominal_cycle_s)
    slowest = max(canon.machines, key=lambda m: m.nominal_cycle_s)
    assert cycles[fastest.id] > cycles[slowest.id]


def test_defect_rate_is_near_the_canon_base_rate(events: list, canon: Canon) -> None:
    cycles = sum(1 for e in events if e.event_type is EventType.CYCLE)
    defects = sum(1 for e in events if e.event_type is EventType.DEFECT)
    rates = [p.defect_base_rate for p in canon.products]
    assert min(rates) * 0.5 <= defects / cycles <= max(rates) * 1.5


def test_unit_sequence_restarts_per_work_order(events: list) -> None:
    """A work order counts its own units from 1."""
    seen: dict[str, list[int]] = {}
    for event in events:
        if event.event_type is EventType.CYCLE:
            seen.setdefault(str(event.payload["work_order"]), []).append(
                int(event.payload["unit_seq"])
            )
    for order, seqs in seen.items():
        assert seqs == list(range(1, len(seqs) + 1)), f"{order} sequence is not contiguous"


def test_machines_stop_outside_staffed_shifts(events: list) -> None:
    """Third shift is unstaffed — the quiet window must actually be quiet.

    It matters because downstream aggregation has to handle empty windows
    rather than skipping them.
    """
    cycles = [e for e in events if e.event_type is EventType.CYCLE]
    hours = {e.event_time.hour for e in cycles}
    assert not (hours & {0, 1, 2, 3, 4}), "cycles ran during the unstaffed shift"


def test_multi_day_runs_extend_the_span(canon: Canon) -> None:
    one = generate(canon, seed=42, start=START, days=1)
    two = generate(canon, seed=42, start=START, days=2)
    assert len(two) > len(one)
    assert two[-1].event_time - two[0].event_time > timedelta(days=1)
