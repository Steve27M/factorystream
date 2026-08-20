"""Consumer group: poll → validate → batch → write parquet → **then** commit.

The ordering in that sentence is the entire correctness argument, and getting it
backwards is the classic data-loss bug:

- **Auto-commit is disabled.** With it on, the client commits on a timer, so a
  crash after the commit but before the S3 write loses every record in flight —
  silently, with no error anywhere.
- **Offsets are committed only after a durable write returns.** A crash between
  the write and the commit therefore produces *re-delivery*, not loss.
- **Re-delivery is absorbed by idempotent object naming.** The replayed batch
  has the same partition and offset range, so it writes to the same key and
  overwrites itself (`writer.BatchWriter`).

Together those give **at-least-once delivery with effectively-once processing**.
Not "exactly-once" — that phrase is marketing, and the README says so
(decisions entry 3).

One partition's records are never mixed into another's batch, because an offset
range only identifies a batch within a single partition.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from types import FrameType
from typing import Any

from factorystream.consumer.writer import BatchWriter, Record, Reject

log = logging.getLogger(__name__)

TOPIC = "factory.events"
GROUP_ID = "factorystream-bronze"
DEFAULT_BROKERS = "localhost:19092"

# Flush thresholds — whichever comes first. Large enough that objects are not
# tiny (Athena pays a per-file overhead and a lake of 4 KB files costs more in
# listing than in scanning), small enough that a crash loses little work.
BATCH_ROWS = 5000
BATCH_SECONDS = 30.0

POLL_TIMEOUT_S = 1.0


@dataclass
class PartitionBuffer:
    """Accumulates one partition's records until a flush is due."""

    records: list[Record] = field(default_factory=list)
    rejects: list[Reject] = field(default_factory=list)
    first_seen: float = field(default_factory=time.monotonic)

    @property
    def pending(self) -> int:
        return len(self.records) + len(self.rejects)

    def due(self) -> bool:
        if self.pending >= BATCH_ROWS:
            return True
        return self.pending > 0 and (time.monotonic() - self.first_seen) >= BATCH_SECONDS

    def reset(self) -> None:
        self.records = []
        self.rejects = []
        self.first_seen = time.monotonic()


@dataclass
class ConsumerStats:
    consumed: int = 0
    written: int = 0
    quarantined: int = 0
    objects: int = 0
    commits: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "consumed": self.consumed,
            "written": self.written,
            "quarantined": self.quarantined,
            "objects": self.objects,
            "commits": self.commits,
        }


def parse_message(
    raw_value: bytes, key: bytes | None, partition: int, offset: int
) -> Record | Reject:
    """Parse one message. Anything unparseable is quarantined, never dropped.

    The consumer must survive a corrupt payload — the generator injects them on
    purpose at a declared rate, and a consumer that raised here would die on its
    own test data.
    """
    machine_from_key = key.decode(errors="replace") if key else None

    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        return Reject(repr(raw_value[:200]), f"UnicodeDecodeError: {exc}",
                      machine_from_key, partition, offset)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        # The broker still has the partition key even though the value is
        # garbage — that is how a real corrupt message arrives.
        return Reject(text[:500], f"JSONDecodeError: {exc.msg}",
                      machine_from_key, partition, offset)

    if not isinstance(parsed, dict):
        return Reject(text[:500], "payload is not an object", machine_from_key, partition, offset)

    try:
        return Record(
            event_id=parsed["event_id"],
            event_type=parsed["event_type"],
            machine_id=parsed["machine_id"],
            event_time=datetime.fromisoformat(parsed["event_time"]),
            publish_time=datetime.fromisoformat(parsed["publish_time"]),
            schema_version=int(parsed.get("schema_version", 1)),
            payload=parsed.get("payload") or {},
            kafka_partition=partition,
            kafka_offset=offset,
        )
    except (KeyError, ValueError, TypeError) as exc:
        return Reject(text[:500], f"{type(exc).__name__}: {exc}",
                      parsed.get("machine_id") or machine_from_key, partition, offset)


class BronzeConsumer:
    """Owns the poll loop, the buffers, and the commit discipline."""

    def __init__(
        self,
        root: str,
        *,
        brokers: str = DEFAULT_BROKERS,
        topic: str = TOPIC,
        group_id: str = GROUP_ID,
        from_beginning: bool = False,
    ) -> None:
        self.writer = BatchWriter(root=root)
        self.topic = topic
        self.stats = ConsumerStats()
        self.buffers: dict[int, PartitionBuffer] = {}
        self._running = True

        from confluent_kafka import Consumer

        self.consumer = Consumer(
            {
                "bootstrap.servers": brokers,
                "group.id": group_id,
                # THE line. Auto-commit is the canonical restart-loses-data bug:
                # the client would commit on a timer, so a crash after the
                # commit and before the durable write loses everything in
                # flight, silently.
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest" if from_beginning else "latest",
                # Long enough that a slow S3 write does not trigger a rebalance
                # mid-batch, which would hand our un-committed offsets to
                # another consumer and duplicate the work.
                "max.poll.interval.ms": 300_000,
                "session.timeout.ms": 45_000,
            }
        )

    # -- lifecycle ----------------------------------------------------------

    def stop(self, *_: object) -> None:
        log.info("shutdown requested; flushing before exit")
        self._running = False

    def run(self, max_idle_polls: int | None = None) -> ConsumerStats:
        self.consumer.subscribe([self.topic])
        idle = 0
        # Joining a consumer group and being assigned partitions takes several
        # seconds, and every poll during that window returns None. Counting
        # those as idle made a bounded run exit BEFORE it was ever assigned
        # anything — consuming zero records and reporting success, which is the
        # worst possible way to fail. Idle only counts once we hold partitions.
        assigned = False

        try:
            while self._running:
                message = self.consumer.poll(POLL_TIMEOUT_S)

                if not assigned and self.consumer.assignment():
                    assigned = True
                    log.info(
                        "partitions assigned",
                        extra={"count": len(self.consumer.assignment())},
                    )

                if message is None:
                    self._flush_due()
                    if assigned:
                        idle += 1
                        if max_idle_polls is not None and idle >= max_idle_polls:
                            break
                    continue

                if message.error():
                    log.warning("broker error", extra={"error": str(message.error())})
                    continue

                idle = 0
                self.stats.consumed += 1

                partition = message.partition()
                offset = message.offset()
                if partition is None or offset is None:
                    # Should not happen for a real record, but the client types
                    # these as optional and a message we cannot place has no
                    # offset to commit and no partition to buffer under.
                    log.warning("message without partition or offset; skipping")
                    continue

                buffer = self.buffers.setdefault(partition, PartitionBuffer())

                key = message.key()
                key_text = key.decode(errors="replace") if key is not None else None

                value = message.value()
                if value is None:
                    # A tombstone: a real Kafka construct (null value, used for
                    # compacted-topic deletes). Nothing in this pipeline
                    # produces one, so its arrival is worth recording rather
                    # than silently skipping — and it still consumes an offset.
                    buffer.rejects.append(
                        Reject(
                            raw="",
                            error="null value (tombstone); unexpected on this topic",
                            machine_id=key_text,
                            kafka_partition=partition,
                            kafka_offset=offset,
                        )
                    )
                else:
                    parsed = parse_message(value, key, partition, offset)
                    if isinstance(parsed, Record):
                        buffer.records.append(parsed)
                    else:
                        buffer.rejects.append(parsed)

                if buffer.due():
                    self._flush_partition(partition)
        finally:
            # Flush and commit whatever is buffered before closing, so a clean
            # shutdown does not force a replay of work already done.
            self._flush_all()
            self.consumer.close()

        return self.stats

    # -- the commit discipline ---------------------------------------------

    def _flush_due(self) -> None:
        for partition in list(self.buffers):
            if self.buffers[partition].due():
                self._flush_partition(partition)

    def _flush_all(self) -> None:
        for partition in list(self.buffers):
            if self.buffers[partition].pending:
                self._flush_partition(partition)

    def _flush_partition(self, partition: int) -> None:
        """Write durably, and only then commit.

        If the write raises, we do **not** commit — the records stay buffered
        and the offsets stay uncommitted, so a restart re-delivers them. That is
        the desired behaviour and the reason this method does not swallow the
        exception.
        """
        buffer = self.buffers[partition]
        if not buffer.pending:
            return

        highest = max(
            [r.kafka_offset for r in buffer.records] + [r.kafka_offset for r in buffer.rejects]
        )

        bronze = self.writer.write_bronze(buffer.records)
        if bronze:
            self.stats.written += bronze.rows
            self.stats.objects += 1

        quarantine = self.writer.write_quarantine(buffer.rejects)
        if quarantine:
            self.stats.quarantined += quarantine.rows
            self.stats.objects += 1

        # Durable write returned. Only now is it safe to commit.
        self._commit(partition, highest)
        self.stats.commits += 1

        log.info(
            "flushed",
            extra={
                "partition": partition,
                "rows": len(buffer.records),
                "quarantined": len(buffer.rejects),
                "committed_offset": highest + 1,
            },
        )
        buffer.reset()

    def _commit(self, partition: int, highest_offset: int) -> None:
        from confluent_kafka import TopicPartition

        # Kafka commits the offset of the NEXT record to read, hence +1.
        # Committing `highest` instead would re-deliver the last record of every
        # batch forever — a subtle off-by-one that looks like harmless
        # duplication until the duplicate counts stop reconciling.
        self.consumer.commit(
            offsets=[TopicPartition(self.topic, partition, highest_offset + 1)],
            asynchronous=False,
        )

    def lag(self) -> dict[int, int]:
        """Per-partition lag — the freshness metric (DMLS Ch. 8)."""
        from confluent_kafka import TopicPartition

        out: dict[int, int] = {}
        for assignment in self.consumer.assignment():
            low, high = self.consumer.get_watermark_offsets(assignment, timeout=5.0)
            committed = self.consumer.committed(
                [TopicPartition(self.topic, assignment.partition)], timeout=5.0
            )[0]
            position = committed.offset if committed.offset >= 0 else low
            out[assignment.partition] = max(0, high - position)
        return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="local directory or s3://bucket/prefix")
    parser.add_argument("--brokers", default=DEFAULT_BROKERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default=GROUP_ID)
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="read the topic from offset 0 — the replay path",
    )
    parser.add_argument(
        "--stop-after-idle",
        type=int,
        default=None,
        metavar="POLLS",
        help="exit after N empty polls; for a bounded duty-cycle run",
    )
    args = parser.parse_args()

    consumer = BronzeConsumer(
        root=args.root,
        brokers=args.brokers,
        topic=args.topic,
        group_id=args.group,
        from_beginning=args.from_beginning,
    )

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handler(consumer))

    stats = consumer.run(max_idle_polls=args.stop_after_idle)

    print(f"consumed   {stats.consumed:>8,}")
    print(f"  bronze   {stats.written:>8,} rows")
    print(f"  quarantine {stats.quarantined:>6,} rows")
    print(f"  objects  {stats.objects:>8,}")
    print(f"  commits  {stats.commits:>8,}")
    return 0


def _handler(consumer: BronzeConsumer) -> Any:
    def handle(_signum: int, _frame: FrameType | None) -> None:
        consumer.stop()

    return handle


if __name__ == "__main__":
    sys.exit(main())
