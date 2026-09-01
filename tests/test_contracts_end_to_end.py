"""v3 through the warehouse: generate, land, build, and check the numbers.

The Phase 6 writeup originally had to say v3 did not flow through the warehouse,
because `transform/profiles.yml` defined only an Athena target and there was no
way to *run* the SQL that would conform it. Shipping a conforming `coalesce`
with a comment asserting it worked would have been a claim about unexecuted
code.

The DuckDB target removed that excuse, so this test exists: it builds a lake
containing v3 events and asserts the warehouse conforms them correctly.

**The assertion that matters is the unit, not the column.** A conformance that
coalesced `duration_ms` into `duration_s` without dividing would populate the
column, satisfy every not-null test, and be wrong by a factor of a thousand -
the exact failure the v3 chapter is about.

Two assertions cover it from different directions, deliberately. One compares
the v3 mean duration against the v1 mean, where a missing division shows up as
a ratio near 1000. The other asserts the completeness ledger still closes, which
is the stronger check because the ledger grades against the *manifest* - ground
truth written by the generator before the pipeline saw anything. That is the one
that caught the real bug here, and it was in the manifest writer rather than in
the warehouse.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

pytest.importorskip("duckdb", reason="needs the DuckDB target")

# Excluded from the default run. It generates a shift, lands a lake and runs a
# full dbt build - minutes, not milliseconds - and it needs `dbt deps` to have
# installed dbt_utils first. Running inside the plain unit step is exactly how
# it failed on its first CI run.
pytestmark = pytest.mark.warehouse


def _run(args: list[str], cwd: Path) -> None:
    result = subprocess.run(
        [sys.executable, *args], cwd=str(cwd), capture_output=True, text=True, timeout=600
    )
    if result.returncode != 0:
        raise AssertionError(
            f"{' '.join(args)} failed ({result.returncode})\n"
            f"{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
        )


@pytest.fixture(scope="module")
def v3_warehouse(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A generated run with the v3 cutover on, landed, and built with dbt.

    Everything lives under the test's own tmp dir - its own scenario, lake and
    warehouse - so the repo's published run stays exactly as documented.
    """
    import os

    import yaml

    work = tmp_path_factory.mktemp("v3")
    out, lake, warehouse = work / "out", work / "lake", work / "warehouse.duckdb"
    out.mkdir()

    # The published scenario with one field changed, so this run differs from
    # the documented one in exactly one respect.
    scenario = yaml.safe_load((REPO / "plant" / "scenario.yaml").read_text(encoding="utf-8"))
    scenario.setdefault("disorder", {})["schema_v3_at"] = 0.75
    config = work / "scenario-v3.yaml"
    config.write_text(yaml.safe_dump(scenario), encoding="utf-8")

    _run(
        ["-m", "factorystream.generator.cli", "--config", str(config), "--out", str(out)],
        cwd=REPO,
    )
    _run(
        ["-m", "factorystream.consumer.load", "--root", str(lake),
         "--events", str(out / "events.jsonl"), "--manifest", str(out / "manifest.jsonl")],
        cwd=REPO,
    )

    # dbt takes the lake location and the warehouse path from the environment,
    # so both point at this test's copies rather than the repo's.
    env = dict(
        os.environ,
        FS_LAKE_DIR=str(lake).replace("\\", "/"),
        FS_DUCKDB_PATH=str(warehouse),
    )
    result = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "build", "--target", "duckdb",
         "--profiles-dir", "."],
        cwd=str(REPO / "transform"), capture_output=True, text=True, env=env, timeout=900,
    )
    if result.returncode != 0:
        raise AssertionError(f"dbt build failed\n{result.stdout[-3000:]}")

    return warehouse, out


def test_the_run_actually_contained_v3_events(v3_warehouse: tuple[Path, Path]) -> None:
    """Guard against the test passing because nothing changed."""
    _, out = v3_warehouse
    injections = json.loads((out / "injections.json").read_text(encoding="utf-8"))
    assert injections["schema_v3"] > 0, "the v3 cutover produced no events"


def test_all_three_versions_reach_the_warehouse(v3_warehouse: tuple[Path, Path]) -> None:
    import duckdb

    warehouse, _ = v3_warehouse
    conn = duckdb.connect(str(warehouse), read_only=True)
    versions = {
        v for (v,) in conn.execute("select distinct schema_version from slv_events").fetchall()
    }
    assert versions == {1, 2, 3}, f"expected all three versions, got {versions}"


def test_v3_durations_are_conformed_to_seconds_not_milliseconds(
    v3_warehouse: tuple[Path, Path],
) -> None:
    """The assertion the whole chapter is for.

    A conformance that forgot to divide would leave v3 durations 1000x the v1
    ones - still populated, still positive, still passing every not-null and
    accepted-values test in the project.
    """
    import duckdb

    warehouse, _ = v3_warehouse
    conn = duckdb.connect(str(warehouse), read_only=True)
    rows = conn.execute(
        """
        select schema_version, avg(duration_s)
        from slv_events
        where event_type = 'cycle' and duration_s is not null
        group by schema_version order by schema_version
        """
    ).fetchall()
    by_version = dict(rows)
    assert set(by_version) == {1, 2, 3}

    v1, v3 = by_version[1], by_version[3]
    # Same population of machines and SKUs, so the means should be close. A
    # missing division would make this ratio ~1000.
    assert 0.5 < v3 / v1 < 2.0, (
        f"v3 mean duration {v3:.1f}s against v1 {v1:.1f}s - "
        f"ratio {v3 / v1:.1f}, which is the missing-unit-conversion signature"
    )


def test_the_ledger_still_reconciles_with_three_schema_versions(
    v3_warehouse: tuple[Path, Path],
) -> None:
    """Conforming a third version must not cost completeness."""
    import duckdb

    warehouse, _ = v3_warehouse
    conn = duckdb.connect(str(warehouse), read_only=True)
    total, broken = conn.execute(
        "select count(*), sum(case when status = 'broken' then 1 else 0 end) "
        "from completeness_ledger"
    ).fetchone()
    assert total > 0
    assert broken == 0, f"{broken} of {total} windows broken with v3 in the stream"
