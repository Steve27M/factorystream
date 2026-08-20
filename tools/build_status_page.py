"""Render the completeness ledger as a static status page.

The ledger is the project's thesis, and a thesis nobody can see is a claim. This
turns it into a page a reviewer can open: every window since the first run,
green or not, with the disorder that was injected into each.

Self-contained by design — no CDN, no runtime fetch, no JavaScript framework.
The page is a snapshot of a query result, so it should keep working in five
years when whatever CDN it depended on has moved.

Reads Athena directly rather than going through dbt, because the page is a
*view* of the warehouse and adding a dbt run to a rendering step would couple
publication to a build.

    python tools/build_status_page.py --out docs/status/index.html
"""

from __future__ import annotations

import argparse
import html
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

WORKGROUP = "plant-platform"
DATABASE = "factorystream"
REGION = "us-east-1"


def query(sql: str) -> list[dict[str, str]]:
    """Run one Athena query and return rows as dicts."""
    import boto3

    athena = boto3.client("athena", region_name=REGION)
    execution_id = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        QueryExecutionContext={"Database": DATABASE},
    )["QueryExecutionId"]

    for _ in range(120):
        time.sleep(1.0)
        state = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        if state["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
    else:
        raise TimeoutError(f"query did not settle: {sql[:80]}")

    if state["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(state["Status"].get("StateChangeReason", "query failed"))

    result = athena.get_query_results(QueryExecutionId=execution_id)["ResultSet"]
    rows = result["Rows"]
    if not rows:
        return []

    header = [d.get("VarCharValue", "") for d in rows[0]["Data"]]
    return [
        dict(zip(header, (d.get("VarCharValue", "") for d in row["Data"]), strict=False))
        for row in rows[1:]
    ]


def gather() -> dict[str, Any]:
    summary = query(
        "select status, count(*) as n from completeness_ledger group by status"
    )
    by_status = {row["status"]: int(row["n"]) for row in summary}

    totals = query(
        """
        select
            count(*)                              as windows,
            count(distinct machine_id)            as machines,
            min(cast(window_start as varchar))    as first_window,
            max(cast(window_start as varchar))    as last_window,
            sum(injected_corrupt)                 as corrupt,
            sum(injected_duplicate_extra)         as duplicate_extra,
            sum(injected_late)                    as late,
            sum(observed_late)                    as late_observed,
            sum(expected_event_count)             as expected_events,
            sum(observed_event_count)             as observed_events
        from completeness_ledger
        """
    )[0]

    per_machine = query(
        """
        select
            machine_id,
            count(*)                                   as windows,
            count_if(status = 'reconciled')            as reconciled,
            count_if(status = 'pending')               as pending,
            count_if(status = 'broken')                as broken,
            sum(observed_event_count)                  as events
        from completeness_ledger
        group by machine_id
        order by machine_id
        """
    )

    broken = query(
        """
        select machine_id, cast(window_start as varchar) as window_start,
               event_delta, unit_delta, defect_delta, root_cause
        from completeness_ledger
        where status = 'broken'
        order by window_start
        limit 50
        """
    )

    recent = query(
        """
        select cast(window_start as varchar) as window_start, machine_id, status,
               observed_event_count, injected_corrupt, injected_duplicate_extra,
               injected_late
        from completeness_ledger
        order by window_start desc, machine_id
        limit 40
        """
    )

    return {
        "by_status": by_status,
        "totals": totals,
        "per_machine": per_machine,
        "broken": broken,
        "recent": recent,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def render(data: dict[str, Any]) -> str:
    by_status = data["by_status"]
    totals = data["totals"]
    reconciled = by_status.get("reconciled", 0)
    pending = by_status.get("pending", 0)
    broken = by_status.get("broken", 0)
    total = reconciled + pending + broken

    # The headline sentence, and the one claim the whole project is built to
    # support. It states the failure honestly when there is one.
    if broken:
        verdict = f"{broken} window{'s' if broken != 1 else ''} did not reconcile"
        verdict_class = "bad"
    elif total == 0:
        verdict = "no windows yet"
        verdict_class = "warn"
    else:
        verdict = f"Every one of {total:,} windows reconciles exactly"
        verdict_class = "good"

    def esc(value: Any) -> str:
        return html.escape(str(value))

    machine_rows = "\n".join(
        f"<tr><td class=mono>{esc(m['machine_id'])}</td>"
        f"<td class=num>{_int(m['windows']):,}</td>"
        f"<td class='num good'>{_int(m['reconciled']):,}</td>"
        f"<td class=num>{_int(m['pending']):,}</td>"
        f"<td class='num {'bad' if _int(m['broken']) else ''}'>{_int(m['broken']):,}</td>"
        f"<td class=num>{_int(m['events']):,}</td></tr>"
        for m in data["per_machine"]
    )

    broken_section = ""
    if data["broken"]:
        rows = "\n".join(
            f"<tr><td class=mono>{esc(b['machine_id'])}</td>"
            f"<td class=mono>{esc(b['window_start'])}</td>"
            f"<td class=num>{esc(b['event_delta'])}</td>"
            f"<td class=num>{esc(b['unit_delta'])}</td>"
            f"<td>{esc(b.get('root_cause') or '')}</td></tr>"
            for b in data["broken"]
        )
        broken_section = f"""
  <h2>Broken windows</h2>
  <p class=note>Each row carries its own root cause. A break is an incident,
     not a metric — it fails the build.</p>
  <table>
    <tr><th>machine</th><th>window</th><th>event Δ</th><th>unit Δ</th><th>root cause</th></tr>
    {rows}
  </table>"""

    recent_rows = "\n".join(
        f"<tr><td class=mono>{esc(r['window_start'])}</td>"
        f"<td class=mono>{esc(r['machine_id'])}</td>"
        f"<td><span class='pill {esc(r['status'])}'>{esc(r['status'])}</span></td>"
        f"<td class=num>{_int(r['observed_event_count']):,}</td>"
        f"<td class=num>{_int(r['injected_corrupt'])}</td>"
        f"<td class=num>{_int(r['injected_duplicate_extra'])}</td>"
        f"<td class=num>{_int(r['injected_late'])}</td></tr>"
        for r in data["recent"]
    )

    return f"""<!doctype html>
<meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>FactoryStream — completeness ledger</title>
<style>
  :root {{
    --bg:#fbfbfa; --fg:#1c1c1a; --muted:#6b6b66; --line:#e3e3df;
    --good:#1a7f4b; --bad:#b3261e; --warn:#9a6700; --card:#fff;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme=light]) {{
      --bg:#16161a; --fg:#e8e8e4; --muted:#9a9a94; --line:#2c2c31;
      --good:#4ade80; --bad:#f87171; --warn:#fbbf24; --card:#1d1d22;
    }}
  }}
  * {{ box-sizing:border-box }}
  body {{ margin:0; padding:2.5rem 1.25rem 4rem; background:var(--bg); color:var(--fg);
    font:16px/1.6 ui-sans-serif,-apple-system,Segoe UI,sans-serif; }}
  main {{ max-width:60rem; margin:0 auto }}
  h1 {{ font-size:1.5rem; margin:0 0 .25rem }}
  h2 {{ font-size:1.05rem; margin:2.5rem 0 .5rem; letter-spacing:.01em }}
  .sub {{ color:var(--muted); margin:0 0 2rem }}
  .verdict {{ font-size:1.6rem; font-weight:650; line-height:1.25;
    padding:1.5rem; border-radius:.6rem; background:var(--card);
    border:1px solid var(--line); border-left:4px solid currentColor; }}
  .verdict.good {{ color:var(--good) }}
  .verdict.bad  {{ color:var(--bad) }}
  .verdict.warn {{ color:var(--warn) }}
  .verdict small {{ display:block; font-size:.85rem; font-weight:400;
    color:var(--muted); margin-top:.6rem }}
  .grid {{ display:grid; gap:.75rem; grid-template-columns:repeat(auto-fit,minmax(9rem,1fr));
    margin-top:1rem }}
  .stat {{ background:var(--card); border:1px solid var(--line); border-radius:.5rem;
    padding:.9rem 1rem }}
  .stat b {{ display:block; font-size:1.35rem; font-variant-numeric:tabular-nums }}
  .stat span {{ color:var(--muted); font-size:.78rem; letter-spacing:.03em;
    text-transform:uppercase }}
  table {{ width:100%; border-collapse:collapse; margin-top:.5rem; font-size:.9rem }}
  th,td {{ text-align:left; padding:.45rem .6rem; border-bottom:1px solid var(--line) }}
  th {{ color:var(--muted); font-weight:600; font-size:.75rem;
    text-transform:uppercase; letter-spacing:.04em }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums }}
  .mono {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:.85em }}
  .good {{ color:var(--good) }} .bad {{ color:var(--bad) }}
  .pill {{ font-size:.72rem; padding:.12rem .5rem; border-radius:1rem;
    border:1px solid currentColor }}
  .pill.reconciled {{ color:var(--good) }}
  .pill.pending {{ color:var(--warn) }}
  .pill.broken {{ color:var(--bad) }}
  .note {{ color:var(--muted); font-size:.88rem; margin:.25rem 0 .75rem }}
  .wrap {{ overflow-x:auto }}
  footer {{ margin-top:3rem; padding-top:1rem; border-top:1px solid var(--line);
    color:var(--muted); font-size:.82rem }}
</style>
<main>
  <h1>FactoryStream — completeness ledger</h1>
  <p class=sub>Every 15-minute window, reconciled against generator ground truth.</p>

  <div class="verdict {verdict_class}">
    {esc(verdict)}
    <small>Ground truth is written directly to storage by the generator and never
    travels the pipeline — so this is a comparison against known truth, not a
    self-check.</small>
  </div>

  <div class=grid>
    <div class=stat><b class=good>{reconciled:,}</b><span>reconciled</span></div>
    <div class=stat><b>{pending:,}</b><span>pending</span></div>
    <div class=stat><b class="{'bad' if broken else ''}">{broken:,}</b><span>broken</span></div>
    <div class=stat><b>{_int(totals['machines'])}</b><span>machines</span></div>
    <div class=stat><b>{_int(totals['observed_events']):,}</b><span>events in gold</span></div>
  </div>

  <h2>Disorder injected on purpose</h2>
  <p class=note>The pipeline is fed deliberately corrupted, duplicated and late
     data at declared rates. These are the numbers it had to survive.</p>
  <div class=grid>
    <div class=stat><b>{_int(totals['corrupt']):,}</b><span>corrupt → quarantined</span></div>
    <div class=stat><b>{_int(totals['duplicate_extra']):,}</b><span>duplicate copies</span></div>
    <div class=stat><b>{_int(totals['late']):,}</b><span>late arrivals</span></div>
    <div class=stat><b>{_int(totals['expected_events']):,}</b><span>expected events</span></div>
    <div class=stat><b>{_int(totals['observed_events']):,}</b><span>observed events</span></div>
  </div>

  <h2>By machine</h2>
  <div class=wrap>
  <table>
    <tr><th>machine</th><th class=num>windows</th><th class=num>reconciled</th>
        <th class=num>pending</th><th class=num>broken</th><th class=num>events</th></tr>
    {machine_rows}
  </table>
  </div>
{broken_section}
  <h2>Most recent windows</h2>
  <div class=wrap>
  <table>
    <tr><th>window</th><th>machine</th><th>status</th><th class=num>events</th>
        <th class=num>corrupt</th><th class=num>dup</th><th class=num>late</th></tr>
    {recent_rows}
  </table>
  </div>

  <footer>
    Generated {esc(data['generated_at'])} ·
    window range {esc(totals.get('first_window', '?'))} → {esc(totals.get('last_window', '?'))} ·
    every event synthetic, from a seeded generator. The plant is fictional.
  </footer>
</main>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("docs/status/index.html"))
    parser.add_argument("--json", type=Path, help="also dump the raw query results")
    args = parser.parse_args()

    data = gather()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(data), encoding="utf-8")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(data, indent=1), encoding="utf-8")

    by_status = data["by_status"]
    print(f"status page -> {args.out}")
    print(
        f"  reconciled {by_status.get('reconciled', 0):,} · "
        f"pending {by_status.get('pending', 0):,} · "
        f"broken {by_status.get('broken', 0):,}"
    )
    return 1 if by_status.get("broken", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
