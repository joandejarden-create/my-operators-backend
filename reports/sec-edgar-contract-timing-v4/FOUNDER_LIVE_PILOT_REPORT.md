# V4 Controlled Schema Activation + 20-Row Live Research Pilot

**Date:** 2026-11-20  
**PR:** https://github.com/joandejarden-create/my-operators-backend/pull/499  
**Prior HEAD:** `c173bfbf3`  

## Hard boundaries (honored)

- HPC hotels created: **0**
- HPC fields modified: **0**
- Full 481 migration: **not executed**
- Customer publication: **blocked**
- Merge / deploy: **not performed**

---

## Schema

| Item | Result |
|---|---|
| Contracts table ID | `tblMjUCBtXF2o3r9u` |
| Milestones table ID | `tbljzAYjYs3dyHWZk` |
| Required contract fields | 28/28 present |
| Required milestone fields | 19/19 present |
| Live field counts (incl. Airtable inverses) | Contracts 32 · Milestones 21 |
| Link-graph checks | **12/12 OK** |
| Publication State fields | Present; writers set `CUSTOMER_BLOCKED` |
| Meta ops | 9 recorded in `SCHEMA_APPLY_OPERATIONS.json` |
| Read-back | `SCHEMA_APPLY_READBACK.json` · **ok: true** |

Apply order executed: Contracts → self-links → Milestones → self-link → Inventory geo → Findings links.

---

## Research persistence (20-row pilot)

| Metric | First apply | Idempotency rerun |
|---|---:|---:|
| Attempted | 20 | 20 |
| Rejected | 0 | 0 |
| Contracts created | **20** | **0** |
| Milestones created | **20** | **0** |
| Inventory created/reused | created on first pass | reused |
| Findings created | on first pass | unchanged / links refreshed |
| Independent read-back OK | **20** | **20** |
| CUSTOMER_BLOCKED verified | yes | yes |
| Contract↔Milestone links | yes | yes |
| HPC created/modified | **0** | **0** |

Artifacts:
- `RESEARCH_PILOT_APPLY.json`
- `RESEARCH_PILOT_IDEMPOTENCY.json`
- `RESEARCH_PILOT_CHECKPOINT.json`

Cycle demonstrated:

**Research Evidence → Discovery Inventory → Hotel Contract → Contract Milestone → Linked Findings → Independent Airtable Read-Back**  
Repeat → **no duplicates**.

---

## Canonical HPC

| | |
|---|---|
| HPC records created | **0** |
| HPC fields modified | **0** |
| CREATE_READY (review only) | **18** |
| Pending verification (queue) | **7** |
| Review artifact | `CREATE_READY_FOUNDER_REVIEW.json` / `.md` |

No CREATE apply. Per-row founder approval still required.

---

## Governance

| | |
|---|---|
| Research spend (this activation) | ~$0 (schema Meta + Airtable writes; Exa not re-spent) |
| Publication | CUSTOMER_BLOCKED on inventory / contracts / milestones |
| Promotion boundary | Intact (`HIF_APPLY_PROMOTION` unused) |
| Schema gate | `HIF_SCHEMA_APPLY=1` |
| Research gate | `HIF_APPLY_RESEARCH=1` |
| Unexpected writes | None observed |

---

## Remaining founder decisions

1. Per-row approve / reject / hold the **18 CREATE_READY** hotels (see founder review table).  
2. Authorize broader migration only after reviewing this pilot.  
3. IC merge of PR #499 when ready — **not** requested by this activation.
