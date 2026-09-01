"""The event contract.

Every event carries the five fields the pipeline's correctness depends on:

- `event_id`   — uuid, the dedupe key. The disorder injector republishes some.
- `event_time` — simulation time the thing happened.
- `publish_time` — wall time it reached the broker. Late arrivals are exactly
  the events where these two diverge, and keeping both is what makes
  event-time-vs-ingest-time discipline testable rather than assumed.
- `schema_version` — v1 or v2. The drift injector flips this mid-history.
- `machine_id`  — the partition key, giving per-machine ordering and nothing
  more, which is precisely the guarantee Kafka provides.

**Ordering guarantee, stated deliberately:** keying by `machine_id` buys
per-machine ordering within a partition. It does *not* buy global ordering, and
the out-of-order injector exists to prove nothing downstream secretly assumes
it.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field

SCHEMA_V1 = 1
SCHEMA_V2 = 2
SCHEMA_V3 = 3


class EventType(StrEnum):
    CYCLE = "cycle"
    DEFECT = "defect"
    STATE_CHANGE = "state_change"
    OPERATOR_SCAN = "operator_scan"


class Event(BaseModel):
    """One shop-floor event, as published.

    `model_config` forbids extras so a typo in a payload key fails at
    construction rather than silently vanishing into the lake.
    """

    model_config = {"extra": "forbid"}

    event_id: str
    event_type: EventType
    machine_id: str
    event_time: datetime
    publish_time: datetime
    schema_version: int = SCHEMA_V1
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        """Flat JSON-serialisable dict, as the producer publishes it.

        Timestamps are ISO 8601 with explicit offsets. A naive timestamp in a
        stream that deliberately injects clock skew would be indefensible.
        """
        return {
            "event_id": self.event_id,
            "event_type": str(self.event_type),
            "machine_id": self.machine_id,
            "event_time": self.event_time.isoformat(),
            "publish_time": self.publish_time.isoformat(),
            "schema_version": self.schema_version,
            "payload": self.payload,
        }


def new_event_id(rng_hex: str) -> str:
    """Deterministic event id derived from the seeded RNG.

    Deliberately **not** `uuid.uuid4()`: the generator's headline property is
    that the same seed produces a byte-identical stream, and a random uuid would
    break that on every run. `uuid.UUID(hex=...)` keeps the familiar shape while
    the bits come from the seeded stream.
    """
    return str(uuid.UUID(hex=rng_hex))


# --- Payload shapes ----------------------------------------------------------
# Documented as models for clarity and testability; carried on the wire as plain
# dicts inside `Event.payload`.


class CyclePayload(BaseModel):
    work_order: str
    sku: str
    unit_seq: int
    duration_s: float
    operator: str


class DefectPayload(BaseModel):
    work_order: str
    sku: str
    unit_seq: int
    defect_code: str
    inspector: str


class StateChangePayload(BaseModel):
    from_state: str
    to_state: str
    reason: str | None = None


class OperatorScanPayload(BaseModel):
    badge_id: str
    action: Literal["login", "logout", "changeover_start", "changeover_end"]
    work_order: str | None = None


def to_v2(payload: dict[str, Any], event_type: EventType) -> dict[str, Any]:
    """Apply the v2 schema change to a payload.

    The drift is deliberately of the two kinds that break naive pipelines
    differently:

    - **an added field** (`line_id`) — tolerable; a reader that ignores unknown
      keys survives it
    - **a renamed field** (`operator` -> `operator_badge` on cycle events) — not
      tolerable; a reader that assumes the old name silently produces nulls

    Silver has to conform both versions into one model, and the renamed field is
    the half that actually tests it.
    """
    out = dict(payload)
    out["line_id"] = out.get("line_id", "")
    if event_type is EventType.CYCLE and "operator" in out:
        out["operator_badge"] = out.pop("operator")
    return out


def cycle_duration_seconds(payload: dict[str, Any]) -> float | None:
    """The cycle duration in SECONDS, whatever version the payload is.

    One function, because the alternative is every reader knowing the contract's
    history - and the first reader that did not know it was the manifest writer,
    which summed `payload["duration_s"]` unconditionally. Under v3 that key does
    not exist, so ground truth silently recorded **zero** for every v3 cycle.

    The failure mode is the one worth noticing: nothing raised, no count was
    wrong, and the completeness ledger reported 129 windows `broken` with an
    `event_delta` of exactly 0 - the pipeline was correct and the thing grading
    it was not. Only a full v3 run through the warehouse could show that, which
    is why `tests/test_contracts_end_to_end.py` exists.

    Seconds is the canonical unit here because the manifest, silver and gold all
    speak it; v3 is the outlier and is converted at every boundary that reads it.
    """
    seconds = payload.get("duration_s")
    if isinstance(seconds, int | float):
        return float(seconds)
    millis = payload.get("duration_ms")
    if isinstance(millis, int | float):
        return float(millis) / 1000.0
    return None


def to_v3(payload: dict[str, Any], event_type: EventType) -> dict[str, Any]:
    """Apply the v3 schema change: `duration_s` becomes `duration_ms`.

    A **unit change**, chosen deliberately as the awkward case.

    The registry catches this one, because the field is also renamed - a
    removal plus an addition, breaking both directions. What it could not catch
    is the sibling change that keeps the name `duration_s` and puts
    milliseconds in it: same name, same type, still a positive number, and
    every downstream aggregate wrong by a factor of a thousand. JSON Schema
    describes shape; a unit is meaning. See `contracts.registry.SEMANTIC_CHANGES`.

    So the rename here is not cosmetic. It is the thing that makes the change
    *visible* to a mechanical check, and doing it the other way is the mistake
    the chapter exists to point at.

    Integer milliseconds, not float: the whole point of the move is that the
    new unit does not need a fractional part.
    """
    out = to_v2(payload, event_type)
    if event_type is EventType.CYCLE and "duration_s" in out:
        out["duration_ms"] = int(round(float(out.pop("duration_s")) * 1000))
    return out
