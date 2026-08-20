"""Disorder injector: rates, per-class properties, and manifest independence.

Every claim the reconciliation ledger later makes rests on these injections
being what the config says they are. "We handle duplicates" is a claim; "we
removed exactly the 92 extra copies injected" is a proof â€” but only if the
injector really produced 92.
"""

from __future__ import annotations

from datetime import date

import pytest

from factorystream.generator.canon import Canon, load_canon
from factorystream.generator.disorder import DisorderConfig, inject
from factorystream.generator.events import SCHEMA_V1, SCHEMA_V2, EventType
from factorystream.generator.manifest import build_manifest, checksum_event_ids
from factorystream.generator.sim import generate

START = date(2026, 3, 2)
SEED = 42


@pytest.fixture(scope="module")
def canon() -> Canon:
    return load_canon()


@pytest.fixture(scope="module")
def clean(canon: Canon) -> list:
    return generate(canon, seed=SEED, start=START, days=1)


def _inject(clean: list, **overrides: object):
    config = DisorderConfig(**overrides)  # type: ignore[arg-type]
    return inject(clean, config, SEED)


# Every class off, so a test can enable exactly one and attribute the result
# to it. Injectors that only work in combination would hide each other's bugs.
ALL_OFF = {
    "late_rate": 0.0,
    "duplicate_rate": 0.0,
    "out_of_order_rate": 0.0,
    "skewed_machines": 0,
    "schema_drift_at": None,
    "corrupt_rate": 0.0,
}


# --- Determinism -------------------------------------------------------------


def test_injection_is_deterministic(clean: list) -> None:
    a, log_a, _ = _inject(clean)
    b, log_b, _ = _inject(clean)
    assert log_a.summary() == log_b.summary()
    assert [r.event.event_id if r.event else r.raw for r in a] == [
        r.event.event_id if r.event else r.raw for r in b
    ]


def test_injection_does_not_mutate_the_clean_stream(clean: list) -> None:
    """The manifest is computed from `clean` â€” mutating it would corrupt truth."""
    before = [e.model_copy(deep=True) for e in clean]
    _inject(clean)
    assert [e.to_wire() for e in clean] == [e.to_wire() for e in before]


def test_all_disorder_off_is_a_passthrough(clean: list) -> None:
    records, log, _ = _inject(clean, **ALL_OFF)
    assert len(records) == len(clean)
    assert log.summary()["late"] == 0
    assert log.summary()["corrupt"] == 0


# --- Rates land where configured --------------------------------------------


def test_late_rate_is_near_configured(clean: list) -> None:
    _, log, _ = _inject(clean, **{**ALL_OFF, "late_rate": 0.02})
    assert 0.012 <= len(log.late_event_ids) / len(clean) <= 0.030


def test_duplicate_rate_is_near_configured(clean: list) -> None:
    _, log, _ = _inject(clean, **{**ALL_OFF, "duplicate_rate": 0.01})
    assert 0.005 <= len(log.duplicated_event_ids) / len(clean) <= 0.018


def test_corrupt_rate_is_near_configured(clean: list) -> None:
    _, log, _ = _inject(clean, **{**ALL_OFF, "corrupt_rate": 0.01})
    assert 0.005 <= len(log.corrupt_event_ids) / len(clean) <= 0.018


def test_exactly_the_configured_number_of_machines_are_skewed(clean: list) -> None:
    _, log, _ = _inject(clean, **{**ALL_OFF, "skewed_machines": 3})
    assert len(log.skewed_machines) == 3


# --- Per-class properties ----------------------------------------------------


def test_late_events_publish_after_they_occur(clean: list) -> None:
    records, log, _ = _inject(clean, **{**ALL_OFF, "late_rate": 0.05, "late_max_hours": 6.0})
    late = [r.event for r in records if r.event and r.event.event_id in log.late_event_ids]
    assert late
    for event in late:
        delay = (event.publish_time - event.event_time).total_seconds()
        assert 0 < delay <= 6 * 3600 + 60


def test_duplicates_are_byte_identical_apart_from_publish_time(clean: list) -> None:
    """A broker retry resends the same bytes â€” that is why event_id dedupes."""
    records, log, _ = _inject(clean, **{**ALL_OFF, "duplicate_rate": 0.05})
    by_id: dict[str, list] = {}
    for record in records:
        if record.event:
            by_id.setdefault(record.event.event_id, []).append(record.event)

    duplicated = [v for k, v in by_id.items() if k in log.duplicated_event_ids]
    assert duplicated
    for copies in duplicated:
        first = copies[0].to_wire()
        for other in copies[1:]:
            wire = other.to_wire()
            assert wire["payload"] == first["payload"]
            assert wire["event_time"] == first["event_time"]
            assert wire["machine_id"] == first["machine_id"]


def test_duplicate_count_matches_the_log_exactly(clean: list) -> None:
    """The ledger's dedupe assertion depends on this being exact."""
    records, log, _ = _inject(clean, **{**ALL_OFF, "duplicate_rate": 0.05})
    extra = sum(log.duplicated_event_ids.values())
    assert len(records) == len(clean) + extra


def test_clock_skew_moves_event_time_only_for_chosen_machines(clean: list) -> None:
    records, log, _ = _inject(clean, **{**ALL_OFF, "skewed_machines": 2, "skew_max_seconds": 90})
    original = {e.event_id: e.event_time for e in clean}
    skewed = set(log.skewed_machines)
    assert skewed

    for record in records:
        event = record.event
        assert event is not None
        if event.machine_id in skewed:
            continue
        assert event.event_time == original[event.event_id], "unskewed machine was moved"


def test_clock_skew_stays_within_bounds(clean: list) -> None:
    records, log, _ = _inject(clean, **{**ALL_OFF, "skewed_machines": 2, "skew_max_seconds": 90})
    original = {e.event_id: e.event_time for e in clean}
    for record in records:
        event = record.event
        assert event is not None
        if event.machine_id not in log.skewed_machines:
            continue
        drift = abs((event.event_time - original[event.event_id]).total_seconds())
        assert drift <= 90 * 1.1 + 1  # base offset plus the wander term


def test_schema_drift_is_a_cutover_not_a_scatter(clean: list) -> None:
    """A deployment flips everything after a point. A scatter would be easier
    to tolerate and would not resemble anything that happens in practice."""
    records, _, _ = _inject(clean, **{**ALL_OFF, "schema_drift_at": 0.5})
    events = sorted((r.event for r in records if r.event), key=lambda e: e.event_time)
    versions = [e.schema_version for e in events]
    first_v2 = versions.index(SCHEMA_V2)
    assert set(versions[:first_v2]) == {SCHEMA_V1}
    assert set(versions[first_v2:]) == {SCHEMA_V2}


def test_v2_renames_the_operator_field_on_cycles(clean: list) -> None:
    """The renamed field is the half that actually tests conforming."""
    records, _, _ = _inject(clean, **{**ALL_OFF, "schema_drift_at": 0.0})
    cycles = [
        r.event for r in records if r.event and r.event.event_type is EventType.CYCLE
    ]
    assert cycles
    for event in cycles:
        assert "operator_badge" in event.payload
        assert "operator" not in event.payload
        assert "line_id" in event.payload


def test_out_of_order_only_reorders_within_a_machine(clean: list) -> None:
    """Reordering across machines would prove nothing â€” nothing is entitled to
    that ordering. Within a key is the guarantee a pipeline actually leans on."""
    records, _, _ = _inject(clean, **{**ALL_OFF, "out_of_order_rate": 0.20})
    per_machine: dict[str, list[str]] = {}
    for record in records:
        assert record.event is not None
        per_machine.setdefault(record.machine_id, []).append(record.event.event_id)

    clean_per_machine: dict[str, list[str]] = {}
    for event in clean:
        clean_per_machine.setdefault(event.machine_id, []).append(event.event_id)

    for machine, ids in per_machine.items():
        assert sorted(ids) == sorted(clean_per_machine[machine]), "an event changed machine"


def test_corrupt_records_keep_their_partition_key(clean: list) -> None:
    """The broker still has the key even when the value is garbage."""
    records, log, _ = _inject(clean, **{**ALL_OFF, "corrupt_rate": 0.05})
    corrupt = [r for r in records if r.is_corrupt]
    assert corrupt
    assert len(corrupt) == len(log.corrupt_event_ids)
    for record in corrupt:
        assert record.machine_id
        assert record.event is None


# --- Manifest ----------------------------------------------------------------


def test_manifest_counts_match_the_clean_stream(clean: list) -> None:
    manifests = build_manifest(clean)
    assert sum(m.event_count for m in manifests) == len(clean)
    assert sum(m.cycle_count for m in manifests) == sum(
        1 for e in clean if e.event_type is EventType.CYCLE
    )
    assert sum(m.defect_count for m in manifests) == sum(
        1 for e in clean if e.event_type is EventType.DEFECT
    )


def test_manifest_is_unaffected_by_disorder(clean: list) -> None:
    """The manifest describes what happened, not what the broker saw.

    If injection could move these numbers, the manifest would be grading the
    publish path against itself.
    """
    before = [m.to_row() for m in build_manifest(clean)]
    _inject(clean)
    after = [m.to_row() for m in build_manifest(clean)]
    assert before == after


def test_windows_are_fifteen_minutes_and_aligned(clean: list) -> None:
    for manifest in build_manifest(clean):
        assert manifest.window_start.minute % 15 == 0
        assert manifest.window_start.second == 0
        assert manifest.window_start.microsecond == 0


def test_checksum_is_order_independent() -> None:
    """The pipeline may reorder within a window; grading order would be wrong."""
    ids = ["c", "a", "b"]
    assert checksum_event_ids(ids) == checksum_event_ids(sorted(ids))
    assert checksum_event_ids(ids) == checksum_event_ids(list(reversed(ids)))


def test_checksum_detects_a_swapped_event() -> None:
    """Counts alone would net to zero on a loss-plus-phantom. This is why the
    checksum exists."""
    assert checksum_event_ids(["a", "b", "c"]) != checksum_event_ids(["a", "b", "d"])


def test_manifest_is_deterministic(canon: Canon) -> None:
    a = build_manifest(generate(canon, seed=SEED, start=START, days=1))
    b = build_manifest(generate(canon, seed=SEED, start=START, days=1))
    assert [m.to_row() for m in a] == [m.to_row() for m in b]

