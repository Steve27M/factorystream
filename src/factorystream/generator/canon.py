"""Loader for the shared plant canon.

`plant/canon.yaml` describes one fictional factory that both FactoryStream and
WearWatch simulate — FactoryStream emits its shop-floor event stream, WearWatch
models the same machines' physics over OPC UA. Sharing the *canon* rather than
the simulator code gives narrative coherence without coupling two codebases that
have genuinely different requirements.

Every value in the canon is invented. That is a binding non-goal in both specs.
"""

from __future__ import annotations

from datetime import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

CANON_PATH = Path(__file__).resolve().parents[3] / "plant" / "canon.yaml"


class Break(BaseModel):
    start: time
    minutes: int = Field(gt=0)


class Shift(BaseModel):
    id: str
    label: str
    start: time
    end: time
    breaks: list[Break] = Field(default_factory=list)


class Machine(BaseModel):
    id: str
    line: str
    model: str
    kind: str
    nominal_cycle_s: float = Field(gt=0)
    # Ignored by FactoryStream — it has no physics layer. Carried so the canon
    # stays one file rather than two that drift apart.
    wear_sensitivity: float = Field(gt=0)
    installed: str

    @field_validator("installed", mode="before")
    @classmethod
    def _stringify_date(cls, v: Any) -> str:
        return str(v)


class Product(BaseModel):
    sku: str
    name: str
    line: str
    defect_base_rate: float = Field(ge=0, le=1)


class Line(BaseModel):
    id: str
    name: str
    area: str
    machines: list[str]


class DefectCode(BaseModel):
    code: str
    label: str
    wear_correlated: bool


class MachineState(BaseModel):
    id: int
    name: str


class WorkOrderShape(BaseModel):
    units_min: int = Field(gt=0)
    units_max: int = Field(gt=0)
    changeover_minutes: dict[str, int]
    split_probability: float = Field(ge=0, le=1)


class PlantMeta(BaseModel):
    id: str
    name: str
    enterprise: str
    site: str
    timezone: str


class Canon(BaseModel):
    """The whole plant, validated."""

    canon_version: int
    plant: PlantMeta
    shifts: list[Shift]
    lines: list[Line]
    machines: list[Machine]
    products: list[Product]
    work_orders: WorkOrderShape
    defect_codes: list[DefectCode]
    states: list[MachineState]
    operators: dict[str, list[str]]

    # -- lookups ------------------------------------------------------------

    def machine(self, machine_id: str) -> Machine:
        for m in self.machines:
            if m.id == machine_id:
                return m
        raise KeyError(f"no machine {machine_id!r} in the canon")

    def machines_on(self, line_id: str) -> list[Machine]:
        return [m for m in self.machines if m.line == line_id]

    def products_on(self, line_id: str) -> list[Product]:
        return [p for p in self.products if p.line == line_id]

    def state_id(self, name: str) -> int:
        for s in self.states:
            if s.name == name:
                return s.id
        raise KeyError(f"no state {name!r} in the canon")

    @property
    def badge_ids(self) -> list[str]:
        return self.operators["badge_ids"]

    def validate_integrity(self) -> None:
        """Cross-reference checks the schema alone cannot express.

        Called at load. A canon that references a machine no line owns, or a
        product on a line with no machines, would produce a simulation that
        silently generates nothing for that entity — the kind of bug that looks
        like a modelling choice until someone counts rows.
        """
        machine_ids = {m.id for m in self.machines}
        line_ids = {ln.id for ln in self.lines}

        for line in self.lines:
            unknown = set(line.machines) - machine_ids
            if unknown:
                raise ValueError(f"line {line.id} lists unknown machines: {sorted(unknown)}")

        for machine in self.machines:
            if machine.line not in line_ids:
                raise ValueError(f"machine {machine.id} is on unknown line {machine.line!r}")

        listed = {mid for ln in self.lines for mid in ln.machines}
        orphans = machine_ids - listed
        if orphans:
            raise ValueError(f"machines belong to no line: {sorted(orphans)}")

        for product in self.products:
            if product.line not in line_ids:
                raise ValueError(f"product {product.sku} is on unknown line {product.line!r}")
            if not self.machines_on(product.line):
                raise ValueError(
                    f"product {product.sku} is on line {product.line}, which has no machines"
                )

        if self.work_orders.units_min > self.work_orders.units_max:
            raise ValueError("work_orders.units_min exceeds units_max")

        if not self.badge_ids:
            raise ValueError("canon defines no operator badge ids")


def load_canon(path: Path | None = None) -> Canon:
    raw = yaml.safe_load((path or CANON_PATH).read_text(encoding="utf-8"))
    canon = Canon.model_validate(raw)
    canon.validate_integrity()
    return canon


@lru_cache(maxsize=1)
def get_canon() -> Canon:
    """Process-wide canon. Cached so validation runs exactly once."""
    return load_canon()
