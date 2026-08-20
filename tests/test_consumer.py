"""Consumer parsing and batching. No broker required.

The commit discipline itself needs a running Redpanda and is covered by the
kill-test in `make test-integration` (Phase 2 deliverable: kill the consumer
mid-batch, restart, prove no loss and no duplicates in bronze). What is testable
here is everything that decides *what* gets written and *when* — which is where
the correctness actually lives.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import pytest

from factorystream.consumer.consumer import (
    BATCH_ROWS,
    PartitionBuffer,
    parse_message,
)
from factorystream.consumer.writer import Record, Reject

T0 = datetime(2026, 3, 2, 6, 30, 0, tzinfo=UTC)


def _wire(**overrides: object) -> bytes:
    payload: dict[str, object] = {
        "event_id": "3f2b1c9a-0000-4000-8000-000000000001",
        "event_type": "cycle",
        "machine_id": "MW1-A-01",
        "event_time": T0.isoformat(),
        "publish_time": T0.isoformat(),
        "schema_version": 1,
        "payload": {"unit_seq": 4, "duration_s": 92.1},
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


# --- Parsing: valid ---------------------------------------------------------


def test_a_well_formed_message_becomes_a_record() -> None:
    result = parse_message(_wire(), b"MW1-A-01", partition=3, offset=417)
    assert isinstance(result, Record)
    assert result.machine_id == "MW1-A-01"
    assert result.kafka_partition == 3
    assert result.kafka_offset == 417
    assert result.event_time == T0


def test_schema_v2_parses_without_special_casing() -> None:
    """Bronze takes both versions as landed; conforming is silver's job."""
    result = parse_message(
        _wire(schema_version=2, payload={"operator_badge": "OP-1042", "line_id": "A"}),
        b"MW1-A-01",
        0,
        1,
    )
    assert isinstance(result, Record)
    assert result.schema_version == 2
    assert result.payload["operator_badge"] == "OP-1042"


# --- Parsing: quarantine, never drop ---------------------------------------


def test_truncated_json_is_quarantined_not_raised() -> None:
    """The generator injects these on purpose — a consumer that raised here
    would die on its own test data."""
    result = parse_message(b'{"event_id": "abc", "event_ty', b"MW1-B-02", 2, 8817)
    assert isinstance(result, Reject)
    assert "JSONDecodeError" in result.error
    assert result.kafka_offset == 8817


def test_a_corrupt_message_keeps_the_partition_key() -> None:
    """The broker still has the key even when the value is garbage. That is how
    a real corrupt message arrives, and quarantine provenance depends on it."""
    result = parse_message(b"{", b"MW1-B-02", 2, 100)
    assert isinstance(result, Reject)
    assert result.machine_id == "MW1-B-02"


def test_invalid_utf8_is_quarantined() -> None:
    result = parse_message(b"\xff\xfe\x00garbage", b"MW1-A-03", 1, 5)
    assert isinstance(result, Reject)
    assert "UnicodeDecodeError" in result.error


def test_valid_json_that_is_not_an_object_is_quarantined() -> None:
    result = parse_message(b'"just a string"', None, 0, 1)
    assert isinstance(result, Reject)
    assert "not an object" in result.error


def test_a_missing_required_field_is_quarantined_with_the_field_named() -> None:
    payload = json.loads(_wire())
    del payload["event_time"]
    result = parse_message(json.dumps(payload).encode(), b"MW1-A-01", 0, 1)
    assert isinstance(result, Reject)
    assert "event_time" in result.error


def test_an_unparseable_timestamp_is_quarantined() -> None:
    result = parse_message(_wire(event_time="not-a-timestamp"), b"MW1-A-01", 0, 1)
    assert isinstance(result, Reject)
    assert isinstance(result.error, str)


def test_a_reject_without_a_key_still_records_its_offset() -> None:
    """Offset provenance is what makes a quarantined record replayable."""
    result = parse_message(b"{", None, 4, 999)
    assert isinstance(result, Reject)
    assert result.machine_id is None
    assert result.kafka_partition == 4
    assert result.kafka_offset == 999


def test_the_raw_payload_survives_into_quarantine() -> None:
    """Quarantine is evidence, not a dead-letter dump — the ledger asserts the
    count and a human has to be able to see what arrived."""
    result = parse_message(b'{"broken": ', b"MW1-A-01", 0, 1)
    assert isinstance(result, Reject)
    assert result.raw.startswith('{"broken"')


# --- Batching ---------------------------------------------------------------


def test_a_buffer_is_due_at_the_row_threshold() -> None:
    buffer = PartitionBuffer()
    assert not buffer.due()
    buffer.records = [object()] * BATCH_ROWS  # type: ignore[list-item]
    assert buffer.due()


def test_a_buffer_is_due_on_the_time_threshold_even_when_small() -> None:
    """Otherwise a quiet partition never flushes and its data sits in memory
    until shutdown — invisible until the process dies and loses it."""
    buffer = PartitionBuffer()
    buffer.records = [object()]  # type: ignore[list-item]
    buffer.first_seen = time.monotonic() - 31.0
    assert buffer.due()


def test_an_empty_buffer_is_never_due() -> None:
    """A flush of nothing would write an empty object and commit an offset that
    covers no work."""
    buffer = PartitionBuffer()
    buffer.first_seen = time.monotonic() - 3600
    assert not buffer.due()


def test_rejects_alone_can_make_a_buffer_due() -> None:
    """A partition receiving only corrupt data must still flush and commit, or
    it stalls forever at the same offset."""
    buffer = PartitionBuffer()
    buffer.rejects = [object()]  # type: ignore[list-item]
    buffer.first_seen = time.monotonic() - 31.0
    assert buffer.due()


def test_reset_clears_both_lists_and_restarts_the_clock() -> None:
    buffer = PartitionBuffer()
    buffer.records = [object()]  # type: ignore[list-item]
    buffer.rejects = [object()]  # type: ignore[list-item]
    buffer.first_seen = time.monotonic() - 100
    buffer.reset()
    assert buffer.pending == 0
    assert not buffer.due()


@pytest.mark.parametrize("rows,rejects,expected", [(0, 0, 0), (3, 0, 3), (0, 2, 2), (3, 2, 5)])
def test_pending_counts_records_and_rejects_together(
    rows: int, rejects: int, expected: int
) -> None:
    """Both consume offsets, so both must count toward a flush."""
    buffer = PartitionBuffer()
    buffer.records = [object()] * rows  # type: ignore[list-item]
    buffer.rejects = [object()] * rejects  # type: ignore[list-item]
    assert buffer.pending == expected
