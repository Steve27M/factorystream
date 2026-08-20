"""Direct loader: generator output straight into bronze, bypassing the broker.

**This is a bootstrap path, not the production path.** In the real design a
Kafka consumer group reads `factory.events`, validates, batches, writes parquet,
and only then commits offsets. That is Phase 2 and it needs a running broker.

This exists so the lake, the Glue catalog, and Athena can be proven end to end
before Redpanda is in the picture — and because it exercises the *same*
`BatchWriter`, the partitioning and idempotent-naming logic under test here is
the identical code the consumer will use. Only the feeder differs.

Honest about what it fakes: there are no Kafka offsets, so a synthetic
partition and monotonic offset are assigned by hashing `machine_id` the way the
broker's default partitioner would. Those columns therefore carry provenance
that is *structurally* right and not literally from a broker. Once the consumer
exists, this path stays useful for fixtures and for replaying a captured file.

    python -m factorystream.consumer.load --events out/events.jsonl \\
        --manifest out/manifest.jsonl --root s3://factorystream-lake-.../
"""

from __future__ import annotations

import argparse
import json
import logging
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any

from factorystream.consumer.writer import BatchWriter, Record, Reject

log = logging.getLogger(__name__)

# Matches the topic's partition count in docker-compose.yml. Keeping them equal
# means bronze written by this path is laid out the way the consumer would lay
# it out, so a later replay does not produce a differently-shaped lake.
PARTITIONS = 6

# Rows per parquet object. Small objects are the classic lake mistake — Athena
# pays a per-file overhead, and thousands of tiny files cost more in listing
# than in scanning. Large enough to be efficient, small enough that one batch
# failing does not lose a whole shift.
BATCH_ROWS = 5000


def partition_for(machine_id: str) -> int:
    """Mirror Kafka's default partitioner: crc32 of the key, modulo partitions."""
    return zlib.crc32(machine_id.encode()) % PARTITIONS


def parse_line(line: str, partition: int, offset: int) -> Record | Reject:
    """Parse one published record, quarantining anything unparseable.

    Corrupt payloads are the point, not an edge case — the generator injects
    them deliberately. A loader that raises here would fail on its own test
    data, which is exactly what the consumer must not do either.
    """
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as exc:
        return Reject(
            raw=line[:500],
            error=f"JSONDecodeError: {exc.msg}",
            machine_id=None,
            kafka_partition=partition,
            kafka_offset=offset,
        )

    if not isinstance(raw, dict):
        return Reject(line[:500], "payload is not an object", None, partition, offset)

    # The generator writes corrupt records as {"_key": ..., "_raw": ...} — the
    # broker keeps the partition key even when the value is garbage.
    if "_raw" in raw:
        return Reject(
            raw=str(raw.get("_raw"))[:500],
            error="corrupt payload from producer",
            machine_id=raw.get("_key"),
            kafka_partition=partition,
            kafka_offset=offset,
        )

    try:
        return Record(
            event_id=raw["event_id"],
            event_type=raw["event_type"],
            machine_id=raw["machine_id"],
            event_time=datetime.fromisoformat(raw["event_time"]),
            publish_time=datetime.fromisoformat(raw["publish_time"]),
            schema_version=int(raw.get("schema_version", 1)),
            payload=raw.get("payload") or {},
            kafka_partition=partition,
            kafka_offset=offset,
        )
    except (KeyError, ValueError, TypeError) as exc:
        return Reject(line[:500], f"{type(exc).__name__}: {exc}", raw.get("machine_id"),
                      partition, offset)


def load(events_path: Path, manifest_path: Path | None, root: str) -> dict[str, Any]:
    writer = BatchWriter(root=root)

    # One monotonic offset counter per partition, as a broker would.
    offsets: dict[int, int] = dict.fromkeys(range(PARTITIONS), 0)
    batches: dict[int, list[Record]] = {p: [] for p in range(PARTITIONS)}
    rejects: dict[int, list[Reject]] = {p: [] for p in range(PARTITIONS)}

    written = quarantined = objects = 0

    def flush(partition: int) -> None:
        nonlocal written, quarantined, objects
        if batches[partition]:
            result = writer.write_bronze(batches[partition])
            if result:
                written += result.rows
                objects += 1
            batches[partition] = []
        if rejects[partition]:
            result = writer.write_quarantine(rejects[partition])
            if result:
                quarantined += result.rows
                objects += 1
            rejects[partition] = []

    with events_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            # Partition by key where we have one; corrupt records still carry
            # theirs, and a truly unreadable line goes to partition 0.
            try:
                peek = json.loads(line)
                key = peek.get("machine_id") or peek.get("_key") or ""
            except json.JSONDecodeError:
                key = ""
            partition = partition_for(key) if key else 0

            offset = offsets[partition]
            offsets[partition] += 1

            parsed = parse_line(line, partition, offset)
            if isinstance(parsed, Record):
                batches[partition].append(parsed)
            else:
                rejects[partition].append(parsed)

            if len(batches[partition]) >= BATCH_ROWS:
                flush(partition)

    for partition in range(PARTITIONS):
        flush(partition)

    manifest_rows = 0
    manifest_objects = 0
    if manifest_path and manifest_path.exists():
        lines = manifest_path.read_text(encoding="utf-8").splitlines()
        rows = [json.loads(line) for line in lines if line.strip()]
        results = writer.write_manifests(rows)
        manifest_rows = sum(r.rows for r in results)
        manifest_objects = len(results)

    return {
        "root": root,
        "bronze_rows": written,
        "quarantined_rows": quarantined,
        "bronze_objects": objects,
        "manifest_rows": manifest_rows,
        "manifest_objects": manifest_objects,
    }


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=Path("out/events.jsonl"))
    parser.add_argument("--manifest", type=Path, default=Path("out/manifest.jsonl"))
    parser.add_argument(
        "--root",
        required=True,
        help="local directory or s3://bucket/prefix",
    )
    args = parser.parse_args()

    summary = load(args.events, args.manifest, args.root)

    print(f"loaded into {summary['root']}")
    print(f"  bronze     {summary['bronze_rows']:>8,} rows in {summary['bronze_objects']} objects")
    print(f"  quarantine {summary['quarantined_rows']:>8,} rows")
    print(
        f"  manifests  {summary['manifest_rows']:>8,} rows in "
        f"{summary['manifest_objects']} objects"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
