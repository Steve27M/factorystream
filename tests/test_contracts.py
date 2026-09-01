"""The contract registry, and the limits of what it can promise.

Half these tests assert that the checker works. The other half assert that it
does **not** catch a class of change it might be assumed to catch, because a
registry that quietly implies full coverage converts an unknown risk into a
believed-absent one - which is worse than having no registry at all.
"""

from __future__ import annotations

import json

import pytest

from factorystream.contracts import (
    SEMANTIC_CHANGES,
    VERSIONS,
    Compatibility,
    compare,
    load,
    report,
    validate,
)


def event(version: int = 1, **payload_overrides: object) -> dict:
    payloads = {
        1: {"work_order": "WO-1", "sku": "SKU-1", "unit_seq": 3,
            "duration_s": 64.2, "operator": "OP-7"},
        2: {"work_order": "WO-1", "sku": "SKU-1", "unit_seq": 3,
            "duration_s": 64.2, "operator_badge": "OP-7", "line_id": "A"},
        3: {"work_order": "WO-1", "sku": "SKU-1", "unit_seq": 3,
            "duration_ms": 64200, "operator_badge": "OP-7", "line_id": "A"},
    }
    payload = {**payloads[version], **payload_overrides}
    return {
        "event_id": "6f1c8f4e-1f9a-4c2b-9f3d-2b7a1e5c8d90",
        "event_type": "cycle",
        "machine_id": "MW1-A-01",
        "event_time": "2026-03-02T08:00:00+00:00",
        "publish_time": "2026-03-02T08:00:01+00:00",
        "schema_version": version,
        "payload": payload,
    }


# --- the schemas load and validate -----------------------------------------


@pytest.mark.parametrize("version", VERSIONS)
def test_every_version_loads_and_is_self_consistent(version: int) -> None:
    schema = load(version)
    assert schema["properties"]["schema_version"]["const"] == version
    assert json.dumps(schema)  # serialisable, so publishable as a contract


@pytest.mark.parametrize("version", VERSIONS)
def test_a_well_formed_event_validates_against_its_own_version(version: int) -> None:
    assert validate(event(version)) == []


def test_an_event_is_checked_against_the_version_it_declares() -> None:
    """A v2 event is not a broken v1 event.

    The stream changes version mid-run by design, so validating everything
    against v1 would report the generator working correctly as a failure - the
    fastest way to get a check switched off.
    """
    assert validate(event(2)) == []
    assert validate(event(2), version=1) != []


def test_an_unknown_version_is_an_error_not_a_pass() -> None:
    assert validate(event(1) | {"schema_version": 99}) != []


def test_a_missing_required_field_is_caught() -> None:
    bad = event(1)
    del bad["payload"]["operator"]
    assert any("operator" in e for e in validate(bad))


def test_an_unknown_top_level_key_is_caught() -> None:
    """`additionalProperties: false`, so a typo fails rather than vanishing."""
    assert validate(event(1) | {"machien_id": "MW1-A-01"}) != []


def test_a_malformed_machine_id_is_caught() -> None:
    assert validate({**event(1), "machine_id": "NOT-A-MACHINE"}) != []


# --- compatibility classification ------------------------------------------


def test_a_rename_breaks_both_directions() -> None:
    """v1 -> v2 renamed `operator`, and a rename is a removal plus an addition.

    That is exactly why renames hurt: a consumer on either schema is broken by
    data written against the other.
    """
    diff = compare(1, 2)
    assert diff.compatibility is Compatibility.NONE
    assert diff.is_breaking
    kinds = {(c.kind, c.path) for c in diff.changes}
    assert ("removed_field", "payload.operator") in kinds
    assert ("added_field", "payload.operator_badge") in kinds


def test_an_added_optional_field_breaks_nothing() -> None:
    line_id = next(c for c in compare(1, 2).changes if c.path == "payload.line_id")
    assert not line_id.breaks_forward
    assert not line_id.breaks_backward


def test_the_v3_unit_change_is_reported_as_breaking() -> None:
    diff = compare(2, 3)
    assert diff.compatibility is Compatibility.NONE
    paths = {c.path for c in diff.changes}
    assert paths == {"payload.duration_s", "payload.duration_ms"}


def test_report_names_both_pairs_and_the_blind_spot() -> None:
    text = report()
    assert "v1 -> v2" in text and "v2 -> v3" in text
    assert "NOT detectable" in text


# --- the part the registry cannot do ---------------------------------------


def test_a_unit_change_that_keeps_the_name_passes_every_check() -> None:
    """The whole argument for `SEMANTIC_CHANGES`, asserted rather than claimed.

    `duration_s` carrying milliseconds is wrong by a factor of a thousand. It
    is still a number, still positive, still named what it was named. JSON
    Schema describes shape; a unit is meaning; nothing here can see it.

    This test passing is the *bad* news, and it is written down so the limit
    cannot be forgotten by anyone reading the green suite.
    """
    seconds = event(1, duration_s=64.2)
    milliseconds = event(1, duration_s=64200.0)
    assert validate(seconds) == []
    assert validate(milliseconds) == []


def test_a_redefined_enum_meaning_passes_every_check() -> None:
    """Same field, same type, same allowed values, different meaning."""
    ok = {**event(1), "event_type": "state_change"}
    ok["payload"] = {"from_state": "RUN", "to_state": "IDLE", "reason": None}
    assert validate(ok) == []


def test_the_blind_spots_are_documented_not_merely_known() -> None:
    assert len(SEMANTIC_CHANGES) >= 3
    assert all(isinstance(s, str) and len(s) > 40 for s in SEMANTIC_CHANGES)
