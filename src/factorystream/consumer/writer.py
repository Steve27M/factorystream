"""Bronze parquet writer with idempotent, replay-safe object naming.

The correctness property this module exists to provide:

    object key = bronze/dt=…/hr=…/part-{partition}-{first_offset}-{last_offset}.parquet

An offset range names the object. A replayed batch therefore **overwrites
itself** rather than appending a duplicate — which is what makes at-least-once
delivery safe to build on. The consumer commits offsets only *after* a
successful write, so a crash between write and commit produces re-delivery, and
re-delivery produces a byte-identical object at the same key.

That is the whole "effectively-once" claim, and it is worth being precise about
what it is not: delivery is at-least-once, and *processing* is made effectively
once by this naming plus dedupe in silver. Unqualified "exactly-once" is
marketing (decisions entry 3).

**Partitioning is on ingest time, not event time.** The disorder injector emits
events whose publish_time trails event_time by up to six hours; partitioning on
event time would mean writing into partitions closed hours ago, breaking both
immutability and the idempotency above. Silver re-keys to event time, which is
where that belongs.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# Explicit, not inferred. An inferred schema silently changes shape when a batch
# happens to contain only nulls in a column, and the resulting parquet no longer
# matches the Glue table — a class of bug that surfaces as an Athena error days
# later rather than at write time.
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("machine_id", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("publish_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("schema_version", pa.int32(), nullable=False),
        pa.field("payload", pa.string(), nullable=True),
        pa.field("ingest_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("raw", pa.string(), nullable=True),
        pa.field("error", pa.string(), nullable=False),
        pa.field("machine_id", pa.string(), nullable=True),
        pa.field("ingest_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
    ]
)

MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("window_start", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("machine_id", pa.string(), nullable=False),
        pa.field("event_count", pa.int32(), nullable=False),
        pa.field("cycle_count", pa.int32(), nullable=False),
        pa.field("defect_count", pa.int32(), nullable=False),
        pa.field("state_change_count", pa.int32(), nullable=False),
        pa.field("operator_scan_count", pa.int32(), nullable=False),
        pa.field("unit_count", pa.int32(), nullable=False),
        pa.field("cycle_duration_sum_s", pa.float64(), nullable=False),
        pa.field("event_id_checksum", pa.string(), nullable=False),
        # Per-window injection accounting. Without these, reconciliation can
        # only say "this window is short by one"; with them it says "short by
        # exactly the event we corrupted on purpose" — the difference between
        # a mystery and a proof.
        pa.field("corrupt_count", pa.int32(), nullable=False),
        pa.field("duplicate_extra_count", pa.int32(), nullable=False),
        pa.field("late_count", pa.int32(), nullable=False),
        # Corruption attributed per measure — a total alone leaves the ledger
        # unable to say what a window should have contained.
        pa.field("corrupt_cycle_count", pa.int32(), nullable=False),
        pa.field("corrupt_defect_count", pa.int32(), nullable=False),
        pa.field("corrupt_duration_sum_s", pa.float64(), nullable=False),
    ]
)


@dataclass(frozen=True, slots=True)
class Record:
    """One consumed message, already parsed and validated."""

    event_id: str
    event_type: str
    machine_id: str
    event_time: datetime
    publish_time: datetime
    schema_version: int
    payload: dict[str, Any]
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True, slots=True)
class Reject:
    """One message the consumer could not parse. Quarantined, never dropped."""

    raw: str
    error: str
    machine_id: str | None
    kafka_partition: int
    kafka_offset: int


@dataclass
class WriteResult:
    key: str
    rows: int
    bytes_written: int


@dataclass
class BatchWriter:
    """Writes batches to a local root or to S3, by the same code path.

    `root` is either a local directory or an `s3://bucket/prefix` URI. Keeping
    both behind one interface is not just convenience — it means the local
    development loop exercises the identical partitioning and naming logic that
    runs against S3, so a path bug cannot hide until deployment.
    """

    root: str
    ingest_clock: Any = field(default=None)

    def _now(self) -> datetime:
        # Injectable so tests can pin the ingest partition instead of writing
        # into whatever hour the suite happens to run in.
        return self.ingest_clock() if self.ingest_clock else datetime.now(UTC)

    # -- keys ---------------------------------------------------------------

    @staticmethod
    def bronze_key(
        ingest_ts: datetime, partition: int, first_offset: int, last_offset: int
    ) -> str:
        """The idempotency guarantee, expressed as a filename.

        Same offsets → same key → a replay overwrites rather than duplicates.
        """
        ts = ingest_ts.astimezone(UTC)
        return (
            f"bronze/dt={ts:%Y-%m-%d}/hr={ts:%H}/"
            f"part-{partition:04d}-{first_offset:012d}-{last_offset:012d}.parquet"
        )

    @staticmethod
    def quarantine_key(
        ingest_ts: datetime, partition: int, first_offset: int, last_offset: int
    ) -> str:
        ts = ingest_ts.astimezone(UTC)
        return (
            f"quarantine/dt={ts:%Y-%m-%d}/"
            f"part-{partition:04d}-{first_offset:012d}-{last_offset:012d}.parquet"
        )

    @staticmethod
    def manifest_key(window_start: datetime) -> str:
        ts = window_start.astimezone(UTC)
        return f"manifests/dt={ts:%Y-%m-%d}/manifest-{ts:%Y%m%dT%H%M}.parquet"

    # -- writes -------------------------------------------------------------

    def write_bronze(self, records: list[Record]) -> WriteResult | None:
        if not records:
            return None

        ingest_ts = self._now()
        partition = records[0].kafka_partition
        if any(r.kafka_partition != partition for r in records):
            # An offset range only identifies a batch within one partition.
            # Mixing them would make the key ambiguous and quietly break replay.
            raise ValueError("a bronze batch must come from a single partition")

        offsets = [r.kafka_offset for r in records]
        key = self.bronze_key(ingest_ts, partition, min(offsets), max(offsets))

        table = pa.table(
            {
                "event_id": [r.event_id for r in records],
                "event_type": [r.event_type for r in records],
                "machine_id": [r.machine_id for r in records],
                "event_time": [r.event_time for r in records],
                "publish_time": [r.publish_time for r in records],
                "schema_version": [r.schema_version for r in records],
                # Payload is re-serialised rather than passed through, so the
                # bytes in bronze are canonical JSON regardless of how the
                # producer spaced them.
                "payload": [json.dumps(r.payload, separators=(",", ":")) for r in records],
                "ingest_ts": [ingest_ts] * len(records),
                "kafka_partition": [r.kafka_partition for r in records],
                "kafka_offset": [r.kafka_offset for r in records],
            },
            schema=BRONZE_SCHEMA,
        )
        return self._write(table, key)

    def write_quarantine(self, rejects: list[Reject]) -> WriteResult | None:
        if not rejects:
            return None

        ingest_ts = self._now()
        partition = rejects[0].kafka_partition
        offsets = [r.kafka_offset for r in rejects]
        key = self.quarantine_key(ingest_ts, partition, min(offsets), max(offsets))

        table = pa.table(
            {
                "raw": [r.raw for r in rejects],
                "error": [r.error for r in rejects],
                "machine_id": [r.machine_id for r in rejects],
                "ingest_ts": [ingest_ts] * len(rejects),
                "kafka_partition": [r.kafka_partition for r in rejects],
                "kafka_offset": [r.kafka_offset for r in rejects],
            },
            schema=QUARANTINE_SCHEMA,
        )
        return self._write(table, key)

    def write_manifests(self, rows: list[dict[str, Any]]) -> list[WriteResult]:
        """Ground truth, written straight to storage by the generator.

        Grouped one object per window so a window can be re-stated
        independently, and so the ledger reads exactly the partitions it needs.
        """
        if not rows:
            return []

        by_window: dict[datetime, list[dict[str, Any]]] = {}
        for row in rows:
            start = row["window_start"]
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            by_window.setdefault(start, []).append({**row, "window_start": start})

        results: list[WriteResult] = []
        for window, group in sorted(by_window.items()):
            table = pa.table(
                {
                    name: [row[name] for row in group]
                    for name in MANIFEST_SCHEMA.names
                },
                schema=MANIFEST_SCHEMA,
            )
            results.append(self._write(table, self.manifest_key(window)))
        return results

    # -- storage ------------------------------------------------------------

    def _write(self, table: pa.Table, key: str) -> WriteResult:
        if self.root.startswith("s3://"):
            return self._write_s3(table, key)
        return self._write_local(table, key)

    def _write_local(self, table: pa.Table, key: str) -> WriteResult:
        path = Path(self.root) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path, compression="snappy")
        size = path.stat().st_size
        log.info("wrote bronze", extra={"key": key, "rows": table.num_rows, "bytes": size})
        return WriteResult(key=key, rows=table.num_rows, bytes_written=size)

    def _write_s3(self, table: pa.Table, key: str) -> WriteResult:
        import boto3  # imported here so the local path needs no cloud SDK

        bucket, _, prefix = self.root.removeprefix("s3://").partition("/")
        full_key = f"{prefix.rstrip('/')}/{key}" if prefix else key

        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer, compression="snappy")
        body = buffer.getvalue().to_pybytes()

        boto3.client("s3").put_object(Bucket=bucket, Key=full_key, Body=body)
        log.info(
            "wrote bronze to s3",
            extra={"key": full_key, "rows": table.num_rows, "bytes": len(body)},
        )
        return WriteResult(key=full_key, rows=table.num_rows, bytes_written=len(body))
