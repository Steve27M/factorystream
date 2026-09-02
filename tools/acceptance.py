"""Phase 5 acceptance: destroy the app layer, rebuild it, replay, and re-grade.

**What this proves, and what it does not.** Not that Terraform works - that was
Phase 0 and is already evidenced. It proves the *reconciliation is
deterministic across a rebuild from nothing*: same seed, same generator, same
manifest, a lake built from an empty bucket, and 523 of 523 windows still
exact. That is a claim about the pipeline end to end rather than about
infrastructure, and it is the one worth timing.

**Scope: `infra/terraform/app/` only.** The account module is deliberately out
of scope. It holds the budgets, the Cost and Usage Report, and the shared
Athena workgroup - all of which cover a sibling project too - so destroying it
would remove cost guardrails from something else in order to time a rebuild
here. Worse, the CUR has no backfill: the period it is absent is a permanent
hole in billing history.

So this exercises the layer that holds the data and leaves the layer that holds
the safety net standing. That is most of the acceptance value and none of the
risk.

    python tools/acceptance.py --confirm

Refuses to run without `--confirm`, because its first act is `terraform
destroy`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "infra" / "terraform" / "app"

WORKGROUP = "plant-platform"
DATABASE = "factorystream"
REGION = "us-east-1"
LAKE_BUCKET = "factorystream-lake-867207177469"
ATHENA_RESULTS = "plant-platform-athena-results-867207177469"


def run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None,
        timeout: int = 1800) -> str:
    merged = {**os.environ, **(env or {})}
    result = subprocess.run(
        cmd, cwd=str(cwd), env=merged, capture_output=True, text=True, timeout=timeout
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd[:3])} failed ({result.returncode})\n"
            f"{result.stdout[-3000:]}\n{result.stderr[-2000:]}"
        )
    return result.stdout


def athena(sql: str) -> list[str]:
    import boto3

    client = boto3.client("athena", region_name=REGION)
    qid = client.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        QueryExecutionContext={"Database": DATABASE},
    )["QueryExecutionId"]
    while True:
        state = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if state["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.0)
    if state["State"] != "SUCCEEDED":
        raise RuntimeError(f"athena: {state.get('StateChangeReason', '')[:200]}")
    rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [d.get("VarCharValue") for d in rows[1]["Data"]]


def bucket_object_count(bucket: str) -> int:
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("s3", region_name=REGION)
    try:
        total = 0
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            total += len(page.get("Contents", []))
        return total
    except ClientError:
        # The bucket is gone, which after a destroy is the correct answer.
        return -1


class Timer:
    """Wall-clock per phase, because 'timed' is half the deliverable."""

    def __init__(self) -> None:
        self.phases: dict[str, float] = {}

    def phase(self, name: str, fn: Any) -> Any:
        print(f"\n=== {name} ===")
        started = time.monotonic()
        result = fn()
        self.phases[name] = round(time.monotonic() - started, 1)
        print(f"    {self.phases[name]}s")
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", action="store_true",
                        help="required: this destroys the app layer")
    parser.add_argument("--out", type=Path,
                        default=ROOT / "evidence" / "acceptance-run.json")
    args = parser.parse_args(argv)

    if not args.confirm:
        print(__doc__)
        print("refusing to run without --confirm")
        return 2

    dbt_env = {
        "FS_LAKE_BUCKET": LAKE_BUCKET,
        "FS_ATHENA_RESULTS": ATHENA_RESULTS,
        "AWS_REGION": REGION,
    }
    timer = Timer()
    report: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat()}

    # --- before -----------------------------------------------------------
    before = timer.phase("before: grade the live stack", lambda: {
        "ledger": athena(
            "select count(*), sum(case when status='broken' then 1 else 0 end) "
            "from completeness_ledger"),
        "bronze": athena("select count(*) from bronze_events"),
        "objects": bucket_object_count(LAKE_BUCKET),
    })
    print(f"    ledger {before['ledger']}  bronze {before['bronze']}  "
          f"objects {before['objects']}")
    report["before"] = before

    # --- destroy ----------------------------------------------------------
    timer.phase("destroy the app layer", lambda: run(
        ["terraform", "destroy", "-auto-approve"], APP))
    gone = bucket_object_count(LAKE_BUCKET)
    print(f"    bucket object count now {gone} (-1 means the bucket is gone)")
    if gone != -1:
        raise RuntimeError(f"lake bucket still has {gone} objects after destroy")
    report["destroyed"] = {"lake_bucket_present": False}

    # --- rebuild ----------------------------------------------------------
    timer.phase("rebuild from Terraform", lambda: run(
        ["terraform", "apply", "-auto-approve"], APP))

    # --- replay -----------------------------------------------------------
    # The same generator output, re-landed into an empty lake. Deterministic
    # from the seed, so this is a replay rather than a fresh run.
    timer.phase("replay into the empty lake", lambda: run(
        [str(ROOT / ".venv" / "Scripts" / "python.exe"), "-m",
         "factorystream.consumer.load", "--root", f"s3://{LAKE_BUCKET}/"], ROOT))

    # --- rebuild the warehouse -------------------------------------------
    timer.phase("dbt build on Athena", lambda: run(
        [str(ROOT / ".venv" / "Scripts" / "python.exe"), "-m", "dbt.cli.main",
         "build", "--target", "dev", "--profiles-dir", "."],
        ROOT / "transform", env=dbt_env))

    # --- after ------------------------------------------------------------
    after = timer.phase("after: grade the rebuilt stack", lambda: {
        "ledger": athena(
            "select count(*), sum(case when status='broken' then 1 else 0 end) "
            "from completeness_ledger"),
        "bronze": athena("select count(*) from bronze_events"),
        "objects": bucket_object_count(LAKE_BUCKET),
    })
    print(f"    ledger {after['ledger']}  bronze {after['bronze']}  "
          f"objects {after['objects']}")
    report["after"] = after

    identical = (before["ledger"] == after["ledger"]
                 and before["bronze"] == after["bronze"])
    report.update({
        "phases_seconds": timer.phases,
        "total_seconds": round(sum(timer.phases.values()), 1),
        "reconciliation_identical": identical,
        "scope": "infra/terraform/app only; the account module (budgets, CUR, "
                 "shared workgroup) was deliberately left standing",
        "finished_at": datetime.now(UTC).isoformat(),
    })

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n" + "=" * 60)
    print(f"  before {before['ledger']}   after {after['ledger']}")
    print(f"  IDENTICAL: {identical}")
    print(f"  total {report['total_seconds']}s")
    print(f"\nwrote {args.out}")
    return 0 if identical else 1


if __name__ == "__main__":
    raise SystemExit(main())
