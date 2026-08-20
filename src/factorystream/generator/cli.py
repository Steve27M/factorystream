"""Generate a run: clean events, injected disorder, and the ground-truth manifest.

    python -m factorystream.generator.cli --config plant/scenario.yaml --out out/

Writes three artefacts, and the separation between them is the whole design:

- `events.jsonl`   — what the producer publishes, disorder and all
- `manifest.jsonl` — what actually happened, computed from the clean stream
- `injections.json`— what was done to the stream, per disorder class

The manifest never sees the publish path. That is what makes it evidence rather
than a tautology.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from factorystream.generator.canon import load_canon
from factorystream.generator.disorder import DisorderConfig, inject
from factorystream.generator.manifest import build_manifest, manifest_totals
from factorystream.generator.sim import generate


class RunConfig(BaseModel):
    model_config = {"extra": "forbid"}

    seed: int = 42
    start_date: date
    days: int = Field(default=1, gt=0)


class Scenario(BaseModel):
    model_config = {"extra": "forbid"}

    run: RunConfig
    disorder: DisorderConfig = Field(default_factory=DisorderConfig)


def load_scenario(path: Path) -> Scenario:
    return Scenario.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def run(scenario: Scenario, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    canon = load_canon()

    clean = generate(
        canon,
        seed=scenario.run.seed,
        start=scenario.run.start_date,
        days=scenario.run.days,
    )

    # Inject first, then build the manifest from the CANONICAL stream it
    # returns — post clock-skew and schema-drift, pre duplication/corruption.
    #
    # The ordering is subtle and was wrong at first. Building the manifest from
    # the pristine pre-injection events meant skewed machines reported one
    # window while the manifest recorded another, and reconciliation failed on
    # exactly those two machines. The manifest must describe what the machine
    # *reported*, because that is what the pipeline sees.
    records, log, canonical = inject(clean, scenario.disorder, scenario.run.seed)
    manifests = build_manifest(canonical, injections=log)

    events_path = out_dir / "events.jsonl"
    with events_path.open("w", encoding="utf-8") as fh:
        for record in records:
            if record.is_corrupt:
                # Written raw, exactly as the broker would carry it: an
                # unparseable value under a valid partition key.
                fh.write(
                    json.dumps({"_key": record.machine_id, "_raw": record.raw}) + "\n"
                )
            else:
                assert record.event is not None
                fh.write(json.dumps(record.event.to_wire()) + "\n")

    manifest_path = out_dir / "manifest.jsonl"
    with manifest_path.open("w", encoding="utf-8") as fh:
        for manifest in manifests:
            fh.write(json.dumps(manifest.to_row()) + "\n")

    injections_path = out_dir / "injections.json"
    injections_path.write_text(json.dumps(log.summary(), indent=1), encoding="utf-8")

    totals = manifest_totals(manifests)
    return {
        "seed": scenario.run.seed,
        "clean_events": len(clean),
        "published_records": len(records),
        "manifest": totals,
        "injections": log.summary(),
        "paths": {
            "events": str(events_path),
            "manifest": str(manifest_path),
            "injections": str(injections_path),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("plant/scenario.yaml"))
    parser.add_argument("--out", type=Path, default=Path("out"))
    parser.add_argument("--seed", type=int, help="override the scenario seed")
    args = parser.parse_args()

    scenario = load_scenario(args.config)
    if args.seed is not None:
        scenario.run.seed = args.seed

    summary = run(scenario, args.out)

    totals = summary["manifest"]
    inj = summary["injections"]
    print(f"seed {summary['seed']}")
    print(f"  clean events      {summary['clean_events']:>8,}")
    print(f"  published records {summary['published_records']:>8,}")
    print(f"  windows           {totals['windows']:>8,}  ({totals['machines']} machines)")
    print(f"  cycles / defects  {totals['cycles']:>8,} / {totals['defects']:,}")
    print("  injected:")
    print(f"    late                  {inj['late']:>6,}")
    print(
        f"    duplicate copies      {inj['duplicate_extra_copies']:>6,} "
        f"across {inj['duplicate_distinct_events']:,} events"
    )
    print(f"    out-of-order          {inj['out_of_order']:>6,}")
    print(f"    schema v2             {inj['schema_v2']:>6,}")
    print(f"    corrupt               {inj['corrupt']:>6,}")
    print(f"    skewed machines       {list(inj['skewed_machines'])}")
    print(f"  -> {summary['paths']['events']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
