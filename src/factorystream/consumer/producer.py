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


def contract_check(events_path: Path) -> dict[str, Any]:
    """Validate a file of events against the contract version each declares.

    **A producer-side gate, run before anything is published.** Catching a
    contract violation here costs one exit code; catching it downstream costs a
    quarantine investigation, and catching it in silver costs a re-run.

    Corrupt records are excluded rather than reported. They are unparseable *by
    construction* - the disorder injector emits them on purpose at a declared
    rate - so counting them as contract failures would make the gate fire on
    the generator working correctly, which is the fastest way to get a check
    switched off.

    Note what this does **not** cover: a change that keeps a field's name and
    type but alters its meaning passes every check here. See
    `contracts.registry.SEMANTIC_CHANGES`.
    """
    from factorystream.contracts import validate

    checked = corrupt = 0
    by_version: dict[int, int] = {}
    failures: list[dict[str, Any]] = []

    with events_path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                corrupt += 1
                continue
            if not isinstance(parsed, dict) or "_raw" in parsed:
                corrupt += 1
                continue

            checked += 1
            version = parsed.get("schema_version")
            if isinstance(version, int):
                by_version[version] = by_version.get(version, 0) + 1

            errors = validate(parsed)
            if errors:
                failures.append(
                    {"line": lineno, "event_id": parsed.get("event_id"), "errors": errors[:3]}
                )

    return {
        "checked": checked,
        "corrupt_skipped": corrupt,
        "by_schema_version": dict(sorted(by_version.items())),
        "failures": failures,
        "ok": not failures,
    }


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
    parser.add_argument(
        "--skip-contract-check",
        action="store_true",
        help="publish without validating against the contract. For deliberately "
        "producing invalid data in a test; never in a normal run.",
    )
    parser.add_argument(
        "--contract-check-only",
        action="store_true",
        help="validate and report, publish nothing. This is what CI runs.",
    )
    args = parser.parse_args()

    if not args.events.exists():
        print(f"no events file at {args.events} — run `make generate` first", file=sys.stderr)
        return 1

    if not args.skip_contract_check:
        check = contract_check(args.events)
        versions = ", ".join(f"v{v}: {n:,}" for v, n in check["by_schema_version"].items())
        print(f"contract: {check['checked']:,} events checked ({versions})")
        print(f"          {check['corrupt_skipped']:,} corrupt records skipped, by design")
        if not check["ok"]:
            # Refuse to publish. A contract violation reaching the topic becomes
            # somebody else's quarantine investigation, and the cost of finding
            # it here is one exit code.
            print(f"          {len(check['failures'])} CONTRACT FAILURES", file=sys.stderr)
            for failure in check["failures"][:5]:
                print(f"            line {failure['line']}: {failure['errors']}", file=sys.stderr)
            return 2
        print("          all valid against their declared version")

    if args.contract_check_only:
        return 0

    result = publish(args.events, args.brokers, args.topic)
    print(f"published {result['sent']:,} records to {args.topic}")
    if result["failed"]:
        print(f"  {result['failed']} delivery failures")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
