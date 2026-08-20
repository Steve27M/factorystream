"""The kill-test: crash the consumer mid-batch, restart, prove nothing was lost.

**Requires a running broker** (`make broker`). Marked `integration` and excluded
from the default run, because a unit suite that needs Docker is a unit suite
people stop running.

This is the Phase 2 deliverable. Everything else about the commit discipline can
be argued from the code; only a real crash proves it. The specific failure being
ruled out is the canonical one:

    poll → write to storage → CRASH → restart

With auto-commit enabled, the client would have committed on a timer *before*
the write, and those records would be gone — silently, with no error anywhere.
With commit-after-durable-write, the crash leaves the offsets uncommitted, the
records are re-delivered, and idempotent object naming absorbs the repeat.

So the assertion is two-sided and both halves matter:
  * **no loss** — every published event_id is present afterwards
  * **no duplicates** — re-delivery did not double-count anything
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

BROKERS = os.environ.get("FS_BROKERS", "localhost:19092")
REPO = Path(__file__).resolve().parent.parent
PYTHON = REPO / ".venv" / "Scripts" / "python.exe"


def _broker_available() -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        md = AdminClient({"bootstrap.servers": BROKERS}).list_topics(timeout=5)
        return "factory.events" in md.topics
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _broker_available(),
        reason="no broker at FS_BROKERS — run `make broker` first",
    ),
]


def _published_event_ids(events_path: Path) -> set[str]:
    """The event_ids the producer actually sent, excluding corrupt payloads.

    Corrupt records have no parseable event_id by construction; they are
    accounted for separately, in quarantine.
    """
    ids: set[str] = set()
    with events_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and "event_id" in parsed:
                ids.add(parsed["event_id"])
    return ids


def _published_record_count(events_path: Path) -> int:
    """Parseable records the producer sent, including deliberate duplicates.

    Excludes corrupt payloads: those land in quarantine, not bronze.
    """
    count = 0
    with events_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and "event_id" in parsed:
                count += 1
    return count


def _bronze_event_ids(root: Path) -> list[str]:
    import pyarrow.parquet as pq

    ids: list[str] = []
    for path in glob.glob(str(root / "bronze" / "**" / "*.parquet"), recursive=True):
        ids.extend(pq.read_table(path).column("event_id").to_pylist())
    return ids


def _run_consumer(root: Path, group: str, *, kill_after: float | None = None) -> None:
    """Start the consumer; optionally kill it hard partway through.

    `kill()` rather than a signal, deliberately: a graceful shutdown would flush
    and commit, which is the behaviour we are specifically NOT testing. The
    crash has to be ungraceful to prove anything.
    """
    proc = subprocess.Popen(
        [
            str(PYTHON), "-m", "factorystream.consumer.consumer",
            "--root", str(root),
            "--brokers", BROKERS,
            "--group", group,
            "--from-beginning",
            "--stop-after-idle", "5",
        ],
        cwd=str(REPO),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if kill_after is not None:
        time.sleep(kill_after)
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=30)
        return

    proc.wait(timeout=180)


def test_crash_mid_run_loses_nothing_and_duplicates_nothing(tmp_path: Path) -> None:
    events = REPO / "out" / "events.jsonl"
    if not events.exists():
        pytest.skip("no out/events.jsonl — run `make generate` and `make publish` first")

    expected = _published_event_ids(events)
    assert expected, "no parseable events to compare against"

    root = tmp_path / "lake"
    group = f"killtest-{uuid.uuid4().hex[:8]}"

    # Crash it early, while batches are still in flight and uncommitted.
    _run_consumer(root, group, kill_after=2.0)

    # Restart on the SAME group. Uncommitted offsets are re-delivered.
    _run_consumer(root, group)

    landed = _bronze_event_ids(root)

    missing = expected - set(landed)
    assert not missing, f"{len(missing)} events lost across the crash"

    # Bronze is "raw as landed", so it legitimately contains the duplicate
    # copies the disorder injector published on purpose — removing those is
    # silver's job. What must NOT happen is the consumer adding duplicates of
    # its own on top, which is what a replay without idempotent naming would do.
    #
    # So the bar is: exactly as many rows as the producer sent, no more. An
    # earlier version of this test asserted zero duplicates outright and failed
    # on the 92 injected ones — measuring the generator, not the consumer.
    published = _published_record_count(events)
    assert len(landed) == published, (
        f"bronze holds {len(landed)} rows against {published} published; "
        f"the difference is consumer-introduced duplication"
    )


def test_a_clean_run_matches_a_crashed_and_resumed_run(tmp_path: Path) -> None:
    """The crash must not change the outcome, only the path taken to it."""
    events = REPO / "out" / "events.jsonl"
    if not events.exists():
        pytest.skip("no out/events.jsonl — run `make generate` and `make publish` first")

    clean_root = tmp_path / "clean"
    crashed_root = tmp_path / "crashed"

    _run_consumer(clean_root, f"clean-{uuid.uuid4().hex[:8]}")

    group = f"crash-{uuid.uuid4().hex[:8]}"
    _run_consumer(crashed_root, group, kill_after=2.0)
    _run_consumer(crashed_root, group)

    assert sorted(_bronze_event_ids(clean_root)) == sorted(_bronze_event_ids(crashed_root))


def test_offsets_are_committed_so_a_rerun_consumes_nothing(tmp_path: Path) -> None:
    """The other half of the contract: committed work is not redone."""
    root = tmp_path / "lake"
    group = f"commit-{uuid.uuid4().hex[:8]}"

    _run_consumer(root, group)
    first = len(_bronze_event_ids(root))
    assert first > 0

    # Same group, not from-beginning this time — everything is committed.
    proc = subprocess.run(
        [
            str(PYTHON), "-m", "factorystream.consumer.consumer",
            "--root", str(root), "--brokers", BROKERS,
            "--group", group, "--stop-after-idle", "4",
        ],
        cwd=str(REPO), capture_output=True, text=True, timeout=120,
    )
    assert "consumed          0" in proc.stdout.replace("\r", "")
    assert len(_bronze_event_ids(root)) == first
