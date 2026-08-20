"""Bronze writer: idempotent naming, schema stability, partitioning.

The idempotency test is the one that matters. It is the property that makes
at-least-once delivery safe, and it is invisible until a replay silently
doubles the row count in production.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from factorystream.consumer.writer import (
    BRONZE_SCHEMA,
    BatchWriter,
    Record,
    Reject,
)

INGEST = datetime(2026, 8, 16, 14, 37, 12, tzinfo=UTC)
EVENT = datetime(2026, 8, 16, 14, 30, 0, tzinfo=UTC)


def _writer(tmp_path: Path) -> BatchWriter:
    return BatchWriter(root=str(tmp_path), ingest_clock=lambda: INGEST)


def _record(offset: int, partition: int = 3, **kw: object) -> Record:
    base: dict[str, object] = {
        "event_id": f"evt-{offset}",
        "event_type": "cycle",
        "machine_id": "MW1-A-01",
        "event_time": EVENT,
        "publish_time": EVENT,
        "schema_version": 1,
        "payload": {"unit_seq": offset, "duration_s": 92.4},
        "kafka_partition": partition,
        "kafka_offset": offset,
    }
    base.update(kw)
    return Record(**base)  # type: ignore[arg-type]


# --- Idempotent naming: the correctness property --------------------------


def test_key_encodes_partition_and_offset_range() -> None:
    key = BatchWriter.bronze_key(INGEST, partition=3, first_offset=100, last_offset=249)
    assert key == "bronze/dt=2026-08-16/hr=14/part-0003-000000000100-000000000249.parquet"


def test_replaying_the_same_batch_overwrites_rather_than_duplicates(tmp_path: Path) -> None:
    """The whole at-least-once safety argument, in one assertion.

    A crash between the S3 write and the offset commit produces re-delivery.
    Re-delivery must produce the same object at the same key — not a second
    file that doubles every downstream count.
    """
    writer = _writer(tmp_path)
    batch = [_record(o) for o in range(100, 110)]

    first = writer.write_bronze(batch)
    second = writer.write_bronze(batch)

    assert first is not None and second is not None
    assert first.key == second.key

    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 1, "a replay created a second object"
    assert pq.read_table(files[0]).num_rows == 10


def test_different_offset_ranges_produce_different_objects(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.write_bronze([_record(o) for o in range(100, 110)])
    writer.write_bronze([_record(o) for o in range(110, 120)])
    assert len(list(tmp_path.rglob("*.parquet"))) == 2


def test_same_offsets_on_different_partitions_do_not_collide(tmp_path: Path) -> None:
    """Offsets are per-partition, so the key must carry the partition too."""
    writer = _writer(tmp_path)
    writer.write_bronze([_record(o, partition=0) for o in range(0, 5)])
    writer.write_bronze([_record(o, partition=1) for o in range(0, 5)])
    assert len(list(tmp_path.rglob("*.parquet"))) == 2


def test_offsets_are_zero_padded_so_keys_sort_correctly() -> None:
    low = BatchWriter.bronze_key(INGEST, 0, 9, 9)
    high = BatchWriter.bronze_key(INGEST, 0, 100, 100)
    # Lexical order must match numeric order, or listing a prefix returns
    # batches in a misleading sequence.
    assert low < high


def test_a_batch_spanning_partitions_is_rejected(tmp_path: Path) -> None:
    """An offset range only identifies a batch within one partition.

    Allowing a mixed batch would make the key ambiguous and quietly break
    replay — two different batches could claim the same name.
    """
    writer = _writer(tmp_path)
    mixed = [_record(1, partition=0), _record(2, partition=1)]
    with pytest.raises(ValueError, match="single partition"):
        writer.write_bronze(mixed)


# --- Partitioning ----------------------------------------------------------


def test_partitions_on_ingest_time_not_event_time(tmp_path: Path) -> None:
    """Late data must land in the current partition, not reopen an old one.

    Partitioning on event time would mean a six-hour-late arrival writes into a
    partition closed hours ago — breaking immutability and the idempotent
    naming above. Silver re-keys to event time; bronze stays append-only.
    """
    writer = _writer(tmp_path)
    very_late = _record(1, event_time=datetime(2026, 8, 15, 3, 0, tzinfo=UTC))
    result = writer.write_bronze([very_late])

    assert result is not None
    # Ingest hour (14:37 on the 16th), not event hour (03:00 on the 15th).
    assert "dt=2026-08-16/hr=14" in result.key


def test_ingest_timestamp_is_stamped_on_every_row(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.write_bronze([_record(o) for o in range(3)])
    table = pq.read_table(next(tmp_path.rglob("*.parquet")))
    assert set(table.column("ingest_ts").to_pylist()) == {INGEST}


# --- Schema ----------------------------------------------------------------


def test_written_schema_matches_the_declared_schema(tmp_path: Path) -> None:
    """Explicit, not inferred.

    An inferred schema changes shape when a batch happens to be all-null in a
    column, and the parquet then no longer matches the Glue table — surfacing
    as an Athena error days later rather than at write time.
    """
    writer = _writer(tmp_path)
    writer.write_bronze([_record(1)])
    written = pq.read_schema(next(tmp_path.rglob("*.parquet")))
    assert written.names == BRONZE_SCHEMA.names
    for name in BRONZE_SCHEMA.names:
        assert written.field(name).type == BRONZE_SCHEMA.field(name).type


def test_payload_is_stored_as_canonical_json(tmp_path: Path) -> None:
    """Bronze keeps the payload raw; silver conforms it across schema versions."""
    writer = _writer(tmp_path)
    writer.write_bronze([_record(1, payload={"b": 2, "a": 1})])
    table = pq.read_table(next(tmp_path.rglob("*.parquet")))
    assert table.column("payload").to_pylist() == ['{"b":2,"a":1}']


def test_schema_v2_rows_land_alongside_v1(tmp_path: Path) -> None:
    """The drift cutover is a staging concern; bronze takes both versions."""
    writer = _writer(tmp_path)
    writer.write_bronze(
        [
            _record(1, schema_version=1, payload={"operator": "OP-1042"}),
            _record(2, schema_version=2, payload={"operator_badge": "OP-1042", "line_id": "A"}),
        ]
    )
    table = pq.read_table(next(tmp_path.rglob("*.parquet")))
    assert sorted(table.column("schema_version").to_pylist()) == [1, 2]


def test_empty_batch_writes_nothing(tmp_path: Path) -> None:
    assert _writer(tmp_path).write_bronze([]) is None
    assert list(tmp_path.rglob("*.parquet")) == []


# --- Quarantine ------------------------------------------------------------


def test_corrupt_payloads_are_written_with_offset_provenance(tmp_path: Path) -> None:
    """Quarantine-never-drop. The ledger later asserts this count exactly."""
    writer = _writer(tmp_path)
    result = writer.write_quarantine(
        [
            Reject(
                raw='{"event_id": "abc", "event_ty',
                error="JSONDecodeError: unterminated string",
                machine_id="MW1-B-02",
                kafka_partition=2,
                kafka_offset=8817,
            )
        ]
    )
    assert result is not None
    assert result.key.startswith("quarantine/dt=2026-08-16/")

    table = pq.read_table(Path(tmp_path) / result.key)
    row = table.to_pylist()[0]
    assert row["kafka_offset"] == 8817
    # The broker still had the key even though the value was garbage.
    assert row["machine_id"] == "MW1-B-02"
    assert row["raw"].startswith('{"event_id"')


def test_quarantine_partitions_by_day_only(tmp_path: Path) -> None:
    """Corrupt records are rare; hourly partitions would be mostly empty."""
    writer = _writer(tmp_path)
    result = writer.write_quarantine(
        [Reject(raw="{", error="x", machine_id=None, kafka_partition=0, kafka_offset=1)]
    )
    assert result is not None
    assert "hr=" not in result.key


# --- Manifests -------------------------------------------------------------


def _manifest_row(window: datetime, machine: str = "MW1-A-01") -> dict[str, object]:
    return {
        "window_start": window,
        "machine_id": machine,
        "event_count": 12,
        "cycle_count": 10,
        "defect_count": 1,
        "state_change_count": 1,
        "operator_scan_count": 0,
        "unit_count": 10,
        "cycle_duration_sum_s": 921.5,
        "event_id_checksum": "deadbeef" * 4,
        "corrupt_count": 0,
        "duplicate_extra_count": 0,
        "late_count": 0,
        "corrupt_cycle_count": 0,
        "corrupt_defect_count": 0,
        "corrupt_duration_sum_s": 0.0,
    }


def test_manifests_are_written_one_object_per_window(tmp_path: Path) -> None:
    """So a window can be re-stated independently of its neighbours."""
    writer = _writer(tmp_path)
    w1 = datetime(2026, 8, 16, 6, 0, tzinfo=UTC)
    w2 = datetime(2026, 8, 16, 6, 15, tzinfo=UTC)

    results = writer.write_manifests(
        [_manifest_row(w1, "MW1-A-01"), _manifest_row(w1, "MW1-A-02"), _manifest_row(w2)]
    )
    assert len(results) == 2
    assert sum(r.rows for r in results) == 3


def test_manifest_key_is_derived_from_the_window() -> None:
    key = BatchWriter.manifest_key(datetime(2026, 8, 16, 6, 15, tzinfo=UTC))
    assert key == "manifests/dt=2026-08-16/manifest-20260816T0615.parquet"


def test_manifest_rewrite_is_idempotent(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    rows = [_manifest_row(datetime(2026, 8, 16, 6, 0, tzinfo=UTC))]
    writer.write_manifests(rows)
    writer.write_manifests(rows)
    assert len(list((tmp_path / "manifests").rglob("*.parquet"))) == 1


def test_manifest_accepts_iso_strings_from_the_generator_cli(tmp_path: Path) -> None:
    """The generator serialises to JSONL, so timestamps arrive as strings."""
    writer = _writer(tmp_path)
    row = _manifest_row(datetime(2026, 8, 16, 6, 0, tzinfo=UTC))
    row["window_start"] = "2026-08-16T06:00:00+00:00"
    results = writer.write_manifests([row])
    assert len(results) == 1
