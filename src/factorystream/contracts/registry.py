"""The event contract, versioned, validated, and diffed for compatibility.

Phase 6, option 3. The generator already performs a schema cutover mid-run, so
this project has the one thing a contract-testing chapter needs and most
demonstrations lack: a stream that genuinely changes shape while it is being
consumed.

**What a schema registry is actually for.** Not documentation - a document
cannot fail a build. It is for answering one question mechanically, before a
change ships: *would this break a consumer that is already running?* The
schemas here are the answer's input; `compare` is the answer.

**And the thing this chapter exists to say: it does not catch everything.**
The v2 -> v3 change replaces `duration_s` (seconds, float) with `duration_ms`
(milliseconds, integer). A registry sees a removed field and an added one and
reports a breaking change, which is correct and useful. But consider the change
that keeps the *name* and changes only the unit - `duration_s` now carrying
milliseconds. Every schema check passes. Every type check passes. Every row
validates. The numbers are wrong by a factor of a thousand and nothing in the
toolchain can tell, because JSON Schema describes shape and a unit is meaning.

So `Change` carries a `detectable` flag, and `SEMANTIC_CHANGES` records the
ones a validator is structurally blind to. A registry that quietly implies full
coverage is worse than none, because it converts an unknown risk into a
believed-absent one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"

VERSIONS = (1, 2, 3)


class Compatibility(StrEnum):
    """How a consumer written against one version fares against another.

    `BACKWARD` - a consumer on the NEW schema can read data written against the
    OLD one. This is the direction that matters when the producer moves first.

    `FORWARD` - a consumer on the OLD schema can read data written against the
    NEW one. This is the direction that matters when consumers cannot all be
    upgraded at once, which in practice is always.
    """

    FULL = "full"
    BACKWARD = "backward"
    FORWARD = "forward"
    NONE = "none"


@dataclass(frozen=True)
class Change:
    """One difference between two versions of the contract."""

    kind: str
    path: str
    detail: str
    breaks_forward: bool
    breaks_backward: bool
    # False when a validator cannot see this change at all. Always stated
    # explicitly rather than left to be inferred from its absence.
    detectable: bool = True


# Changes that are real, consequential, and invisible to any structural check.
# Listed by hand because there is no mechanism that could derive them - that is
# the entire point of the list.
SEMANTIC_CHANGES: tuple[str, ...] = (
    "A unit change that keeps the field name and type (seconds -> milliseconds "
    "in `duration_s`) validates cleanly against every version of this contract "
    "and is wrong by a factor of 1000.",
    "A meaning change to an enum member (`state_change.to_state = 'IDLE'` "
    "redefined from 'powered but not cutting' to 'powered and unstaffed') "
    "validates cleanly and silently redistributes every downstream aggregate.",
    "A timezone convention change (event_time switching from machine-local to "
    "UTC) validates cleanly and moves every window boundary.",
)


@lru_cache(maxsize=8)
def load(version: int) -> dict[str, Any]:
    """The JSON Schema for one contract version."""
    if version not in VERSIONS:
        raise ValueError(f"no contract for version {version}; have {list(VERSIONS)}")
    path = CONTRACTS_DIR / f"event.v{version}.json"
    if not path.is_file():
        raise FileNotFoundError(f"contract missing at {path}")
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def validate(event: dict[str, Any], version: int | None = None) -> list[str]:
    """Validate one wire event, returning error strings rather than raising.

    A list, not an exception: the producer checks a whole batch and a single
    bad record should not hide the other nine.

    `version` defaults to the event's own `schema_version`, because validating
    a v2 event against v1 and calling it a failure would be checking the wrong
    thing - the stream is *supposed* to change version mid-run.
    """
    import jsonschema

    declared = version if version is not None else event.get("schema_version")
    if declared not in VERSIONS:
        return [f"schema_version {declared!r} is not a known contract version"]

    validator = jsonschema.Draft202012Validator(load(declared))
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in sorted(validator.iter_errors(event), key=lambda e: list(e.absolute_path))
    ]


def _cycle_payload_props(schema: dict[str, Any]) -> tuple[dict[str, Any], set[str]]:
    payload = schema["allOf"][0]["then"]["properties"]["payload"]
    return payload.get("properties", {}), set(payload.get("required", []))


@dataclass
class Diff:
    """The result of comparing two contract versions."""

    old: int
    new: int
    changes: list[Change] = field(default_factory=list)

    @property
    def compatibility(self) -> Compatibility:
        breaks_fwd = any(c.breaks_forward for c in self.changes)
        breaks_bwd = any(c.breaks_backward for c in self.changes)
        if not breaks_fwd and not breaks_bwd:
            return Compatibility.FULL
        if breaks_fwd and breaks_bwd:
            return Compatibility.NONE
        return Compatibility.BACKWARD if not breaks_bwd else Compatibility.FORWARD

    @property
    def is_breaking(self) -> bool:
        return self.compatibility is not Compatibility.FULL


def compare(old: int, new: int) -> Diff:
    """Diff two contract versions and classify what each change breaks.

    The rules applied, and why each one falls where it does:

    - **Added optional field** - breaks nothing. A reader ignoring unknown keys
      survives, and a reader on the new schema finds it absent but not required.
    - **Added required field** - breaks BACKWARD. A consumer on the new schema
      rejects old data that cannot possibly carry it.
    - **Removed required field** - breaks FORWARD. A consumer on the old schema
      still demands a field the new producer no longer sends.
    - **Type narrowed** (`number` -> `integer`) - breaks BACKWARD. Old data
      contains values the new schema refuses.

    A rename is not a distinct rule; it is a removal and an addition, and it
    breaks both directions, which is exactly why renames hurt.
    """
    old_props, old_req = _cycle_payload_props(load(old))
    new_props, new_req = _cycle_payload_props(load(new))
    diff = Diff(old=old, new=new)

    for name in sorted(set(new_props) - set(old_props)):
        required = name in new_req
        diff.changes.append(
            Change(
                kind="added_field",
                path=f"payload.{name}",
                detail="required in the new version" if required else "optional",
                breaks_forward=False,
                breaks_backward=required,
            )
        )

    for name in sorted(set(old_props) - set(new_props)):
        diff.changes.append(
            Change(
                kind="removed_field",
                path=f"payload.{name}",
                detail="required in the old version" if name in old_req else "was optional",
                breaks_forward=name in old_req,
                breaks_backward=False,
            )
        )

    for name in sorted(set(old_props) & set(new_props)):
        was, now = old_props[name].get("type"), new_props[name].get("type")
        if was != now:
            narrowed = (was, now) == ("number", "integer")
            diff.changes.append(
                Change(
                    kind="type_changed",
                    path=f"payload.{name}",
                    detail=f"{was} -> {now}",
                    breaks_forward=True,
                    breaks_backward=narrowed or was != now,
                )
            )

    return diff


def report() -> str:
    """A human-readable compatibility matrix for every adjacent version pair."""
    lines = ["contract compatibility", "=" * 70]
    for old, new in zip(VERSIONS, VERSIONS[1:], strict=False):
        diff = compare(old, new)
        lines.append(f"\nv{old} -> v{new}: {diff.compatibility.value.upper()}")
        for change in diff.changes:
            flags = []
            if change.breaks_backward:
                flags.append("breaks backward")
            if change.breaks_forward:
                flags.append("breaks forward")
            lines.append(
                f"  {change.kind:<14} {change.path:<26} {change.detail}"
                + (f"  [{', '.join(flags)}]" if flags else "")
            )
    lines += ["", "-" * 70, "NOT detectable by any structural check:"]
    lines += [f"  - {s}" for s in SEMANTIC_CHANGES]
    return "\n".join(lines)
