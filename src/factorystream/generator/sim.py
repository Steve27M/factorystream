"""Discrete-event simulation of the plant.

A machine's day is a loop: pick up a work order, change over to it, run units
one at a time, occasionally scrap one, go idle when the shift ends. Each of
those transitions emits an event.

**Determinism is the headline property.** Same seed, same stream, byte for byte
(MLDP Ch. 1). Two rules make that hold, and both are easy to break by accident:

1. **Every machine draws from its own RNG**, seeded from the run seed and the
   machine id. A single shared RNG would make each machine's stream depend on
   how the scheduler interleaved the others.
2. **Nothing consults the wall clock or `uuid4()`.** Event ids come from the
   seeded stream (`events.new_event_id`), and `publish_time` is derived from
   simulated time plus a drawn latency, not from `datetime.now()`.

Determinism is not decoration here. The whole project's thesis is that gold
reconciles exactly against a ground-truth manifest, and that only means
something if the run can be reproduced.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from random import Random

from factorystream.generator.canon import Canon, Machine, Product, Shift
from factorystream.generator.events import (
    Event,
    EventType,
    new_event_id,
)

# Wall-clock lag between an event happening and reaching the broker, before the
# disorder injector gets involved. Small and boring by design — the interesting
# lateness is injected deliberately, not smuggled in here.
BASE_PUBLISH_LAG_MS = (40, 400)

# Fraction of cycle time a machine spends between units (load/unload).
INTERCYCLE_FRACTION = (0.05, 0.18)

# Chance a running machine drops into an unplanned stop on any given cycle.
UNPLANNED_STOP_PROBABILITY = 0.004
UNPLANNED_STOP_MINUTES = (3, 25)

STOP_REASONS = (
    "tool_change",
    "material_shortage",
    "alarm_spindle",
    "alarm_coolant",
    "quality_hold",
    "operator_absent",
)


def _machine_seed(run_seed: int, machine_id: str) -> int:
    """Stable per-machine seed.

    Hashing rather than `run_seed + index` so adding a machine to the canon
    does not reshuffle every other machine's stream. Python's `hash()` is
    salted per process and would not be reproducible — blake2b is.
    """
    digest = hashlib.blake2b(
        f"{run_seed}:{machine_id}".encode(), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big")


@dataclass
class WorkOrder:
    id: str
    sku: str
    units: int
    machine_id: str


@dataclass
class MachineSim:
    """One machine's state machine, advancing through simulated time."""

    machine: Machine
    canon: Canon
    rng: Random
    now: datetime
    state: str = "IDLE"
    order: WorkOrder | None = None
    unit_seq: int = 0
    orders_started: int = 0
    operator: str = ""
    events: list[Event] = field(default_factory=list)

    # -- emission -----------------------------------------------------------

    def _emit(self, event_type: EventType, payload: dict[str, object]) -> None:
        lag_ms = self.rng.randint(*BASE_PUBLISH_LAG_MS)
        self.events.append(
            Event(
                event_id=new_event_id(self.rng.getrandbits(128).to_bytes(16, "big").hex()),
                event_type=event_type,
                machine_id=self.machine.id,
                event_time=self.now,
                publish_time=self.now + timedelta(milliseconds=lag_ms),
                payload=dict(payload),
            )
        )

    def _transition(self, to_state: str, reason: str | None = None) -> None:
        if to_state == self.state:
            return
        self._emit(
            EventType.STATE_CHANGE,
            {"from_state": self.state, "to_state": to_state, "reason": reason},
        )
        self.state = to_state

    # -- behaviour ----------------------------------------------------------

    def start_shift(self, badge: str) -> None:
        self.operator = badge
        self._emit(EventType.OPERATOR_SCAN, {"badge_id": badge, "action": "login"})

    def end_shift(self) -> None:
        if self.operator:
            self._emit(
                EventType.OPERATOR_SCAN, {"badge_id": self.operator, "action": "logout"}
            )
        self._transition("IDLE", reason="shift_end")
        self.operator = ""

    def begin_order(self, order: WorkOrder) -> None:
        """Changeover, then start running. Changeover is a real cost, not a gap."""
        self.order = order
        self.unit_seq = 0
        self.orders_started += 1

        self._transition("CHANGEOVER", reason="new_work_order")
        self._emit(
            EventType.OPERATOR_SCAN,
            {
                "badge_id": self.operator,
                "action": "changeover_start",
                "work_order": order.id,
            },
        )

        spec = self.canon.work_orders.changeover_minutes
        minutes = self.rng.uniform(spec["min"], spec["max"])
        self.now += timedelta(minutes=minutes)

        self._emit(
            EventType.OPERATOR_SCAN,
            {
                "badge_id": self.operator,
                "action": "changeover_end",
                "work_order": order.id,
            },
        )
        self._transition("EXECUTE")

    def run_unit(self, product: Product) -> None:
        """One part: advance the clock, emit a cycle, maybe emit a defect."""
        assert self.order is not None

        # Cycle time varies around nominal. Lognormal-ish via a bounded normal:
        # real cycles have a floor and a long right tail, not a symmetric spread.
        nominal = self.machine.nominal_cycle_s
        duration = max(nominal * 0.82, self.rng.gauss(nominal, nominal * 0.06))

        self.now += timedelta(seconds=duration)
        self.unit_seq += 1

        self._emit(
            EventType.CYCLE,
            {
                "work_order": self.order.id,
                "sku": self.order.sku,
                "unit_seq": self.unit_seq,
                "duration_s": round(duration, 3),
                "operator": self.operator,
            },
        )

        if self.rng.random() < product.defect_base_rate:
            code = self.rng.choice(self.canon.defect_codes).code
            self._emit(
                EventType.DEFECT,
                {
                    "work_order": self.order.id,
                    "sku": self.order.sku,
                    "unit_seq": self.unit_seq,
                    "defect_code": code,
                    "inspector": self.operator,
                },
            )

        # Inter-cycle load/unload.
        self.now += timedelta(
            seconds=duration * self.rng.uniform(*INTERCYCLE_FRACTION)
        )

        if self.rng.random() < UNPLANNED_STOP_PROBABILITY:
            self._unplanned_stop()

    def _unplanned_stop(self) -> None:
        reason = self.rng.choice(STOP_REASONS)
        self._transition("HELD", reason=reason)
        self.now += timedelta(minutes=self.rng.uniform(*UNPLANNED_STOP_MINUTES))
        self._transition("EXECUTE")

    def order_complete(self) -> bool:
        return self.order is not None and self.unit_seq >= self.order.units


class PlantSimulator:
    """Runs the whole plant across a span of days, one machine at a time.

    Machines are simulated independently rather than in a global event queue.
    They share no state — no shared tooling, no shared operators mid-shift, no
    line-level blocking — so interleaving them would add coupling the model does
    not have, and would make each machine's stream depend on the others'
    scheduling. Independent simulation is both simpler and what keeps rule 1
    (per-machine RNG) meaningful.
    """

    def __init__(self, canon: Canon, seed: int, start: date, days: int) -> None:
        self.canon = canon
        self.seed = seed
        self.start = start
        self.days = days

    def run(self) -> list[Event]:
        events: list[Event] = []
        for machine in self.canon.machines:
            events.extend(self._run_machine(machine))
        # Global sort by event_time. The broker will not preserve this — that is
        # the point of the out-of-order injector — but the manifest is computed
        # in event time, so the generator's own view is ordered.
        events.sort(key=lambda e: (e.event_time, e.machine_id, e.event_id))
        return events

    def _run_machine(self, machine: Machine) -> list[Event]:
        rng = Random(_machine_seed(self.seed, machine.id))
        products = self.canon.products_on(machine.line)
        sim = MachineSim(
            machine=machine,
            canon=self.canon,
            rng=rng,
            now=datetime.combine(self.start, time(0, 0), tzinfo=UTC),
        )

        for day_offset in range(self.days):
            day = self.start + timedelta(days=day_offset)
            for shift in self.canon.shifts:
                self._run_shift(sim, day, shift, products, rng)

        return sim.events

    def _run_shift(
        self,
        sim: MachineSim,
        day: date,
        shift: Shift,
        products: list[Product],
        rng: Random,
    ) -> None:
        start_at = datetime.combine(day, shift.start, tzinfo=UTC)
        end_at = datetime.combine(day, shift.end, tzinfo=UTC)
        if end_at <= start_at:  # shift crosses midnight
            end_at += timedelta(days=1)

        sim.now = start_at
        sim.start_shift(rng.choice(self.canon.badge_ids))

        breaks = sorted(
            (
                (datetime.combine(day, b.start, tzinfo=UTC), timedelta(minutes=b.minutes))
                for b in shift.breaks
            ),
            key=lambda pair: pair[0],
        )
        next_break = 0

        while sim.now < end_at:
            # Take a break if one is due. Machines sit in HELD; the resulting
            # gap in cycle events is real and downstream aggregation must not
            # mistake it for a data loss.
            if next_break < len(breaks) and sim.now >= breaks[next_break][0]:
                _, duration = breaks[next_break]
                sim._transition("HELD", reason="scheduled_break")
                sim.now += duration
                sim._transition("EXECUTE")
                next_break += 1
                continue

            if sim.order is None or sim.order_complete():
                product = rng.choice(products)
                units = rng.randint(
                    self.canon.work_orders.units_min, self.canon.work_orders.units_max
                )
                sim.begin_order(
                    WorkOrder(
                        id=f"WO-{sim.machine.id}-{sim.orders_started + 1:04d}",
                        sku=product.sku,
                        units=units,
                        machine_id=sim.machine.id,
                    )
                )
                if sim.now >= end_at:
                    break
                continue

            product = next(p for p in products if p.sku == sim.order.sku)
            sim.run_unit(product)

        sim.end_shift()


def generate(canon: Canon, seed: int, start: date, days: int) -> list[Event]:
    return PlantSimulator(canon, seed, start, days).run()


def iter_events(events: list[Event]) -> Iterator[dict[str, object]]:
    """Wire-format stream, as the producer would publish it."""
    for event in events:
        yield event.to_wire()
