"""Publish generated events to the broker.

Keyed by `machine_id`, which buys **per-machine ordering within a partition and
nothing more** — exactly the guarantee Kafka provides. The out-of-order injector
exists to prove nothing downstream secretly assumes global order.

`acks=all` because a lost produce would make the manifest wrong through no fault
of the pipeline, and the whole project rests on the manifest being truth.

**Corrupt payloads are published as-is.** The consumer must survive them; a
producer that filtered them would be testing nothing.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

TOPIC = "factory.events"
DEFAULT_BROKERS = "localhost:19092"


def _producer(brokers: str) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": brokers,
            # Durability over throughput. At this volume the cost is invisible
            # and the guarantee is the point.
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 20,
            "batch.size": 64 * 1024,
        }
    )


def publish(
    events_path: Path, brokers: str = DEFAULT_BROKERS, topic: str = TOPIC
) -> dict[str, int]:
    producer = _producer(brokers)

    sent = failed = 0

    def on_delivery(err: Any, _msg: Any) -> None:
        nonlocal failed
        if err is not None:
            failed += 1
            log.error("delivery failed", extra={"error": str(err)})

    with events_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            # The key must survive even when the value is garbage — that is how
            # a corrupt message actually arrives at a consumer, and the
            # quarantine path depends on still knowing which machine it came
            # from.
            try:
                parsed = json.loads(line)
                key = parsed.get("machine_id") or parsed.get("_key") or ""
                value = json.dumps(parsed.get("_raw")) if "_raw" in parsed else line
                if "_raw" in parsed:
                    # Publish the corrupt bytes themselves, not a JSON wrapper
                    # around them.
                    value = str(parsed["_raw"])
            except json.JSONDecodeError:
                key, value = "", line

            producer.produce(
                topic,
                key=key.encode() if key else None,
                value=value.encode(),
                on_delivery=on_delivery,
            )
            sent += 1

            # Serve delivery callbacks as we go; without this the queue fills
            # and `produce` starts raising BufferError on a large file.
            if sent % 1000 == 0:
                producer.poll(0)

    producer.flush(30)
    return {"sent": sent, "failed": failed}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=Path("out/events.jsonl"))
    parser.add_argument("--brokers", default=DEFAULT_BROKERS)
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()

    if not args.events.exists():
        print(f"no events file at {args.events} — run `make generate` first", file=sys.stderr)
        return 1

    result = publish(args.events, args.brokers, args.topic)
    print(f"published {result['sent']:,} records to {args.topic}")
    if result["failed"]:
        print(f"  {result['failed']} delivery failures")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
