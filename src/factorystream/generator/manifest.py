"""The ground-truth manifest — the thing the pipeline is judged against.

Real pipelines *assert* correctness; almost none can *prove* it, because ground
truth is unknowable once data has left the source. A synthetic generator knows
exactly what it emitted, so this module writes down that knowledge per window
and the reconciliation job later proves gold equals it, exactly.

**The manifest bypasses the pipeline entirely.** It is written straight to
storage by the generator. It is not derived from bronze, it is not a dbt model,
and nothing downstream may influence it — the moment the manifest is computed
from the same path it validates, it stops being evidence and becomes a tautology.

Two subtleties worth stating, because both are easy to get wrong:

**Windows use event_time, including skew.** A machine with a broken clock
reports the wrong time, and the manifest records what the machine *said* — not
what "really" happened in simulation. The pipeline sees the reported time, so
grading it against corrected time would fail it for the generator's sin.

**The manifest counts distinct events, not publishes.** Duplicates are recorded
separately in the injection log. Gold must contain the distinct count; silver
must have removed exactly the extras.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from factorystream.generator.events import Event, EventType, cycle_duration_seconds


class InjectionCounts(Protocol):
    """The subset of `disorder.InjectionLog` the manifest needs.

    A Protocol rather than an import so this module stays independent of the
    injector — the manifest is *truth*, and truth should not depend on the
    thing that corrupts it.
    """

    corrupt_event_ids: set[str]
    late_event_ids: set[str]
    duplicated_event_ids: dict[str, int]

# 15 minutes, matching the aggregate marts. A window is the smallest unit the
# ledger reports on, so it is also the finest granularity at which a break can
# be localised.
WINDOW_MINUTES = 15


def window_start(ts: datetime, minutes: int = WINDOW_MINUTES) -> datetime:
    """Floor a timestamp to its window. UTC throughout."""
    ts = ts.astimezone(UTC)
    floored = ts.replace(second=0, microsecond=0)
    return floored - timedelta(minutes=floored.minute % minutes)


@dataclass
class WindowManifest:
    """Exact truth for one machine in one 15-minute event-time window."""

    window_start: datetime
    machine_id: str

    event_count: int = 0
    cycle_count: int = 0
    defect_count: int = 0
    state_change_count: int = 0
    operator_scan_count: int = 0

    unit_count: int = 0
    cycle_duration_sum_s: float = 0.0

    # Checksum over sorted event_ids. Counts can coincidentally match while the
    # *set* differs — a lost event plus a phantom one nets to zero. The
    # checksum catches that; counts alone would not.
    event_id_checksum: str = ""

    # --- per-window injection accounting ---
    #
    # The spec requires each disorder class be checkable *by class*, not just in
    # aggregate. Without these, reconciliation can only say "this window is
    # short by one" — with them it says "short by exactly the one event we
    # corrupted on purpose", which is the difference between a mystery and a
    # proof.
    #
    # The exact expected identities, which the ledger asserts:
    #   deduped bronze rows = event_count - corrupt_count
    #   raw bronze rows     = event_count - corrupt_count + duplicate_extra_count
    corrupt_count: int = 0
    duplicate_extra_count: int = 0
    late_count: int = 0

    # Corruption attributed BY MEASURE, not just in total.
    #
    # A total alone is not enough and the ledger proved it: subtracting
    # `corrupt_count` from the expected event count while leaving the expected
    # unit count untouched made four windows look broken when the pipeline was
    # correct. A corrupted cycle event removes one event, one unit, AND its
    # duration from what the warehouse can possibly observe.
    corrupt_cycle_count: int = 0
    corrupt_defect_count: int = 0
    corrupt_duration_sum_s: float = 0.0

    def to_row(self) -> dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(),
            "machine_id": self.machine_id,
            "event_count": self.event_count,
            "cycle_count": self.cycle_count,
            "defect_count": self.defect_count,
            "state_change_count": self.state_change_count,
            "operator_scan_count": self.operator_scan_count,
            "unit_count": self.unit_count,
            # Rounded because float sums are compared across engines — Python,
            # Athena and DuckDB must agree, and they will not on the 15th digit.
            "cycle_duration_sum_s": round(self.cycle_duration_sum_s, 3),
            "event_id_checksum": self.event_id_checksum,
            "corrupt_count": self.corrupt_count,
            "duplicate_extra_count": self.duplicate_extra_count,
            "late_count": self.late_count,
            "corrupt_cycle_count": self.corrupt_cycle_count,
            "corrupt_defect_count": self.corrupt_defect_count,
            "corrupt_duration_sum_s": round(self.corrupt_duration_sum_s, 3),
        }


def build_manifest(
    events: list[Event], injections: InjectionCounts | None = None
) -> list[WindowManifest]:
    """Compute per-window, per-machine truth from the canonical event stream.

    `events` must be the **canonical** stream: after clock skew and schema
    drift (those mutate the record, and the pipeline sees the mutated version)
    but before duplication, reordering, lateness and corruption (those are
    transport phenomena). See `disorder.inject`, which returns exactly that.

    `injections` attributes each transport-level disorder to the window it
    affected, so reconciliation is exact rather than approximately right.
    """
    buckets: dict[tuple[datetime, str], WindowManifest] = {}
    ids: dict[tuple[datetime, str], list[str]] = defaultdict(list)

    for event in events:
        key = (window_start(event.event_time), event.machine_id)
        manifest = buckets.get(key)
        if manifest is None:
            manifest = WindowManifest(window_start=key[0], machine_id=key[1])
            buckets[key] = manifest

        manifest.event_count += 1
        ids[key].append(event.event_id)

        match event.event_type:
            case EventType.CYCLE:
                manifest.cycle_count += 1
                manifest.unit_count += 1
                duration = cycle_duration_seconds(event.payload)
                if duration is not None:
                    manifest.cycle_duration_sum_s += duration
            case EventType.DEFECT:
                manifest.defect_count += 1
            case EventType.STATE_CHANGE:
                manifest.state_change_count += 1
            case EventType.OPERATOR_SCAN:
                manifest.operator_scan_count += 1

    for key, manifest in buckets.items():
        manifest.event_id_checksum = checksum_event_ids(ids[key])

    if injections is not None:
        # Attribute each injected disorder to the window of the event it hit.
        # Walking the canonical stream is what makes this possible: the
        # injection log records event_ids, and only the canonical stream knows
        # which window each id belongs to.
        for event in events:
            key = (window_start(event.event_time), event.machine_id)
            manifest = buckets.get(key)
            if manifest is None:
                continue
            if event.event_id in injections.corrupt_event_ids:
                manifest.corrupt_count += 1
                # Attribute to the measures this event would have contributed to.
                if event.event_type is EventType.CYCLE:
                    manifest.corrupt_cycle_count += 1
                    duration = cycle_duration_seconds(event.payload)
                    if duration is not None:
                        manifest.corrupt_duration_sum_s += duration
                elif event.event_type is EventType.DEFECT:
                    manifest.corrupt_defect_count += 1
            if event.event_id in injections.late_event_ids:
                manifest.late_count += 1
            manifest.duplicate_extra_count += injections.duplicated_event_ids.get(
                event.event_id, 0
            )

    return sorted(buckets.values(), key=lambda m: (m.window_start, m.machine_id))


def checksum_event_ids(event_ids: list[str]) -> str:
    """Order-independent checksum over a set of event ids.

    Sorted before hashing so it does not depend on arrival order — the pipeline
    is entitled to reorder within a window, and grading it on ordering would
    test the wrong thing. Truncated to 32 hex chars: enough that a collision is
    not a practical concern, short enough to read in a diff.
    """
    digest = hashlib.sha256()
    for event_id in sorted(event_ids):
        digest.update(event_id.encode())
        digest.update(b"\x00")
    return digest.hexdigest()[:32]


def manifest_totals(manifests: list[WindowManifest]) -> dict[str, Any]:
    """Run-level rollup, for the CLI summary and the stage writeup."""
    return {
        "windows": len(manifests),
        "machines": len({m.machine_id for m in manifests}),
        "events": sum(m.event_count for m in manifests),
        "cycles": sum(m.cycle_count for m in manifests),
        "defects": sum(m.defect_count for m in manifests),
        "units": sum(m.unit_count for m in manifests),
        "cycle_duration_sum_s": round(sum(m.cycle_duration_sum_s for m in manifests), 3),
        "first_window": manifests[0].window_start.isoformat() if manifests else None,
        "last_window": manifests[-1].window_start.isoformat() if manifests else None,
    }
