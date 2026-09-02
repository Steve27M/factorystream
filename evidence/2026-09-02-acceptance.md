# Evidence — Phase 5 acceptance: destroy, rebuild, replay, re-grade

**Date:** 2026-09-02 · **Account:** `867207177469` · **Region:** `us-east-1`
· Machine-readable: [`acceptance-run.json`](acceptance-run.json)

The Phase 5 deliverable: tear the app layer down, bring it back from Terraform,
replay the stream, and confirm the reconciliation still closes. Executed by
`tools/acceptance.py --confirm`, which refuses to run without the flag because
its first act is `terraform destroy`.

---

## Result

```
                        before          after
  ledger windows          523             523
  broken                    0               0
  bronze rows           4,243           4,243
  lake objects             82              82
```

**IDENTICAL: true.** Not "close" and not "within tolerance" — the same numbers,
from a lake that did not exist in between.

## Timing

| phase | seconds |
|---|---|
| grade the live stack | 13.9 |
| **destroy the app layer** | **7.7** |
| rebuild from Terraform | 60.5 |
| replay into the empty lake | 33.3 |
| dbt build on Athena | 56.7 |
| grade the rebuilt stack | 11.3 |
| **total** | **183.4** |

Three minutes from a live warehouse to an empty account and back to the same
answer.

The destroy being the fastest phase is worth noticing: **taking infrastructure
down is trivial and putting it back is not**, which is exactly why the rebuild
is the half worth timing.

## What this proves, and what it does not

**Not** that Terraform works. That was Phase 0 and is already evidenced by an
apply from zero.

It proves the **reconciliation is deterministic across a rebuild from nothing**.
The bucket was deleted — object count went to "bucket does not exist", asserted
rather than assumed — and the Glue tables went with it. What came back was
rebuilt from the same seed, the same generator output, the same manifest, and it
graded identically.

That is a property of the pipeline end to end rather than of the infrastructure,
and it is the strongest form of the project's claim: the numbers are not merely
correct, they are *reproducible from scratch*.

## Scope: the app layer only

`infra/terraform/account/` was deliberately left standing. It holds the budgets,
the Cost and Usage Report, and the shared `plant-platform` Athena workgroup —
and all three cover a sibling project as well, so destroying them would remove
cost guardrails from something else in order to time a rebuild here.

The CUR is the specific reason it would have been a bad trade: **it has no
backfill**, so any period it is absent is a permanent hole in billing history.

This exercises the layer that holds the data and leaves the layer that holds the
safety net alone. Most of the acceptance value, none of the risk.
