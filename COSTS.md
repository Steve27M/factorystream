# Costs

Shared AWS account `867207177469`, used by **FactoryStream** and **WearWatch**.
One account, two projects, one budget — so this file lives here and WearWatch
links to it rather than keeping a second set of numbers that would drift.

Publishing the number, and the engineering that keeps it small, is the point.
Under-spend is a result, not an apology.

---

## Starting position — verified 2026-08-16

Measured, not assumed. Every line came from a read-only API call.

| Fact | Value | How |
|---|---|---|
| Credit balance | **$199.97** | Billing console |
| Spend, Jun 2026 | $0.00 | `ce get-cost-and-usage` |
| Spend, Jul 2026 | $0.00 | `ce get-cost-and-usage` |
| Spend, Aug 2026 (partial) | $0.00 | `ce get-cost-and-usage` |
| Billable resources, all 17 enabled regions | **none** | `tools/teardown_verify.py --all-regions` |
| Services not even enrolled | Redshift, MSK, Kinesis | same |
| Pre-existing budget | "My Monthly Cost Budget", $10 | signup default |
| CUR enabled | **no** — see Open items | `cur describe-report-definitions` |
| OIDC provider | none | `iam list-open-id-connect-providers` |

An empty account is the ideal starting line: nothing legacy to inherit, and any
future non-zero line item is unambiguously ours.

## ⏳ Free Plan ends **2026-09-09** — 21 days remaining as of 2026-08-19 UTC

**This is the binding constraint, not the balance.** The plan stops regardless
of remaining credits, so the $199.97 is bounded by time rather than by money.

What it changes:

- **The cloud phases are short, deliberate evidence runs — not a place to live.**
  Development happens locally at $0 and continues indefinitely after the plan
  ends. Both projects already run entirely offline today.
- **Deploy early, not late.** Everything is Terraform, so a cloud run is
  repeatable: apply, land data, capture evidence, destroy, and re-run if the
  evidence is thin. A single deployment attempt on 7 September with no recovery
  time is the version that fails.
- **Final teardown lands 2026-09-04 at the latest**, leaving five days of
  buffer. `teardown_verify.py` is the proof, not the intention.

WearWatch's repatriation thesis is unusually well suited to this: its design was
always "use managed services while credits last, then migrate to a $0 local twin
with a parity proof." A hard deadline makes that real rather than hypothetical.

---

## Budget

| Line | Planned | Hard cap |
|---|---|---|
| **FactoryStream** | | |
| S3 (lake, manifests, athena-results) | $3 | $8 |
| Athena (bytes scanned) + Glue | $5 | $12 |
| **WearWatch** | | |
| Ingestion — IoT Core, Firehose, S3 | $8 | $15 |
| Athena + Glue | $4 | $10 |
| SageMaker spot training campaigns | $25 | $40 |
| ECR + Lambda inference | $4 | $8 |
| **Shared** | | |
| CUR storage, budgets, misc | $1 | $3 |
| Contingency | $10 | — |
| **Total** | **$60** | **$96** |

Against $199.97 that leaves roughly $100 of headroom — deliberate, because the
first cloud estimate anyone writes is wrong and the interesting question is by
how much.

**Exceeding a hard cap is a stop-and-redesign event**, recorded in the relevant
`docs/decisions.md`, not a quiet overage.

### Where the money actually goes

Worth stating plainly, because the intuition is usually wrong: **S3 and Glue at
this volume are pennies.** The two lines that can genuinely hurt are

- **SageMaker training** — bounded by using spot instances and short campaigns,
  and by the binding non-goal against a real-time endpoint (an idle endpoint is
  ~$40–50/month and would eat the entire budget doing nothing).
- **Athena bytes scanned** — bounded by partitioning, columnar storage, no
  `SELECT *` in any model, and a workgroup-level per-query scan limit. An
  unpartitioned full-table scan is how a $5 month becomes a $60 month.

Everything else is rounding error at this scale.

---

## Guardrails

Built in Phase 0, **before any billable resource exists**. That ordering is the
whole point — a budget alarm added after the spend is a postmortem.

- **AWS Budgets** at 50% / 90% of the monthly figure, email notification
- **Cost and Usage Report** exporting to a dedicated S3 prefix — the raw
  evidence source for the postmortem below
- **Athena workgroup** with a per-query bytes-scanned cap
- **`tools/teardown_verify.py`** run at the end of every working session
- **No NAT Gateway, ever** — banned in both specs; the classic silent burner
- **No long-lived AWS keys** — CI authenticates by GitHub OIDC role assumption

---

## Session ledger

One row per working session. `teardown_verify` output is the evidence that the
session ended clean, not a claim that it did.

| Date | Session | Spend to date | Teardown verify | Notes |
|---|---|---|---|---|
| 2026-08-16 | Account audit; CLI configured | $0.00 | clean, 17 regions | Empty account confirmed. No resources created. |
| 2026-08-16 | **Phase 0 applied** — 17 resources | $0.00 | clean, 17 regions | Budgets, CUR, OIDC, Athena workgroup. All free at rest. Evidence: `evidence/2026-08-16-phase0.md`. **CUR now recording.** |
| 2026-08-19 | **Lake applied + first exact reconciliation** | $0.00 | clean, 17 regions | 9 resources (S3 lake, Glue db + 3 tables). Loaded 4,247 records → 72 objects, 578 KB. **523/523 windows reconcile exactly** against ground truth. Evidence: `evidence/2026-08-19-first-reconciliation.md`. Athena scanned <1 MB across 5 queries. |

---

## Open items

1. ~~Record the Free Plan end date~~ — **2026-09-09**, recorded above.
2. ~~Enable CUR~~ — **done 2026-08-16**, recording to
   `plant-platform-cur-867207177469` with `RESOURCES` detail.
3. ~~Avoid colliding with the signup budget~~ — ours use the `plant-platform-`
   prefix; "My Monthly Cost Budget" ($10) is left untouched and is not ours.
4. **Confirm what happens at the plan boundary.** Account closure and conversion
   to paid are very different outcomes. If it converts and anything is still
   running, that becomes a real charge against a real card.
5. **Verify SageMaker spot pricing** before the $25 training line is quoted as
   fact rather than estimate. Marked `[VERIFY]` in the WearWatch spec.

## Postmortem

*Written at the end of the cloud phases: line-item actuals against the table
above, with commentary on the variances. Under-spend is a result.*
