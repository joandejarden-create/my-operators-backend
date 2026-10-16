# Reusable Cursor prompt — Full GDI E2E (standard entry)

Copy-paste for the **next hotel**. Do **not** invent a hotel-specific repair script or overlay that bypasses the shared contract.

---

You are continuing Dealality Group Demand Intelligence under the **Full GDI E2E contract** (`gdi-full-e2e-contract-v1.5`).

**Repository:** `my-operators-backend`  
**Standard entry point only:**

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --authorization-ref "<hotel/product ledger auth id>" \
  --budget-usd <N> \
  --authorize-paid-discovery \
  [--authorize-surfe] \
  [--authorize-publish --target-env production] \
  [--dry-run] \
  [--resume-run <runId>] \
  --out-dir reports/gdi-full-e2e-contract
```

Read-only status (never mutates the source journal):

```bash
node scripts/gdi-full-e2e-status-v1.mjs \
  --hotel <canonicalHotelId> \
  --run-dir <path-to-run> \
  --out-dir reports/gdi-full-e2e-contract
```

**Non-negotiables (Mayflower / San Diego / Renaissance → shared evaluator):**

1. Every expected segment axis has exactly one applicability decision; empty coverage ≠ done.  
2. Applicable segments need declared-depth evidence — plan_only / label “researched” cannot COMPLETE.  
3. Association contacts do not satisfy planner discovery.  
4. Sports and educational axes need **supported independent** assessment — absence of a proxy-admission flag is insufficient.  
5. Contact depth is mandatory even when an organization fallback exists — disposition the queue; never stamp EXHAUSTED without documented attempts/sources.  
6. Structured hotel-fit + complete drawers **before authorized publication writes**; never_executed ≠ provisional/unknown after assessment.  
7. Canonical writers use the **use-case suitability contract** — `whyThisMatters` / `proposedHotelUse` alone do not establish travel, recurring activity, or qualified demand; **no** post-import eligibility repair.  
8. Reconcile duplicates from **canonical persist outputs**; new records ≠ new opportunities; report stored / facing / unique separately; preserve customer history + publication metadata.  
9. Required deliverables + local + production verification are part of delivery.  
10. COMPLETE derives from current evidence — manual “passed” cannot override failed machine checks.  
11. Resume re-runs stale/unfinished verify without repeating paid research.  
12. Hotel/product budgets stay separate; `--dry-run` overrides all paid/write flags.  
13. Delivery verify fails closed: valid schema, hotel identity, publication manifest reconcile, list/detail, no provider leaks, observed build identity when intended SHA is asserted — HTTP 200 / empty JSON / caller SHA alone ≠ passed.  
14. Budget reconcile reads ledger authorization **before** comparing `--authorization-ref`; never mint allowance from CLI.  
15. Geography checks must preserve `localConnection`; hotel catchment defines foreign markets (no Bethesda/NYC substitution).  
16. PE schema/link persistence and local↔production parity are part of the standard path where PE packages exist.  
17. Internal provider metadata (`webhound`, `researchProvider`, `WEBHOUND_*`, cost fields) must never reach customer list/detail/share DTOs.  
18. **Lessons capture is required** (stage `capture_lessons` / P). For every defect or correction in this task: update shared implementation + regression test + `LESSON_REGISTER.md` + this runbook/prompt. Track Documented / Implemented (shared path) / Tested / Executed successfully / Production-verified separately. Do not claim full incorporation from a status label alone. Do not defer lessons to another founder prompt or leave them only in a hotel-specific report. A hotel-specific repair is not a process improvement until the shared path prevents recurrence or detects and routes it. Closeout must list: lessons incorporated this run · remaining gaps · whether next hotel inherits each correction automatically.

**Coordinate** with active Renaissance / Tampa / ADP / release owners. Do not interrupt research jobs, duplicate provider submissions, or overwrite their journals. Do not activate new hotels or mint budget via free-text. Do not re-pay completed Parallel jobs or fabricate stage evidence.

**Prove** with `npm run test:gdi-full-e2e-contract` and a dry-run coordinator evaluate before any paid/live publish.

Lesson register: `reports/gdi-full-e2e-contract/LESSON_REGISTER.md`.  
Operator runbook: `reports/gdi-full-e2e-contract/OPERATOR_RUNBOOK.md`.

---
