# Reusable Cursor prompt — Full GDI E2E (standard entry)

Copy-paste for the **next hotel**. Do **not** invent a hotel-specific repair script or overlay that bypasses the shared contract.

---

You are continuing Dealality Group Demand Intelligence under the **Full GDI E2E contract** (`gdi-full-e2e-contract-v1.2`).

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

**Non-negotiables (Renaissance lessons — already in shared evaluator):**

1. Every expected segment axis has exactly one applicability decision; empty coverage ≠ done.  
2. Applicable segments need declared-depth evidence — plan_only / label “researched” cannot COMPLETE.  
3. Association contacts do not satisfy planner discovery.  
4. Sports and educational axes need **independent** assessment (no exhibitor/sports/medical proxy).  
5. Contact depth is mandatory even when an organization fallback exists — disposition the queue.  
6. Structured hotel-fit + complete drawers before claiming publication COMPLETE.  
7. Canonical writers must include eligibility research fields — **no** post-import eligibility repair.  
8. Reconcile duplicates; new records ≠ new opportunities; preserve customer history.  
9. Required deliverables + local + production verification are part of delivery.  
10. COMPLETE derives from current evidence — manual “passed” cannot override failed machine checks.  
11. Resume re-runs stale/unfinished verify without repeating paid research.  
12. Hotel/product budgets stay separate; `--dry-run` overrides all paid/write flags.

**Coordinate** with active Renaissance / Tampa / ADP / release owners. Do not interrupt research jobs, duplicate provider submissions, or overwrite their journals. Do not activate new hotels or mint budget via free-text.

**Prove** with `npm run test:gdi-full-e2e-contract` and a dry-run coordinator evaluate before any paid/live publish.

Lesson register: `reports/gdi-full-e2e-contract/LESSON_REGISTER.md`.

---
