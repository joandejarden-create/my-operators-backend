# GDI Full E2E Contact Research Recovery — Buyer-WHO Orchestration

**Status:** READY FOR FOUNDER REVIEW — $0 implementation + certification  
**Date:** 2026-11-24  
**Branch:** `fix/gdi-buyer-who-e2e-recovery-v1` (stacked on PR **#583**)  
**Paid calls this task:** **0** (no Exa / Findymail / Surfe / Airtable / publish / deploy)

---

## 1. Exact root-cause code corrections

| Bug | Fix |
|-----|-----|
| `runRestoredOrsDeepResearchPass` used `deepResult.opportunity` (undefined) → discarded `after.opportunity` + buyer-WHO | `resolveOpportunityFromDeepResearchResult()` reads `deepResult.after.opportunity` |
| `buyerWhoFallback` never persisted onto opportunity/ORS | `attachBuyerWhoFallbackToOpportunity()` |
| After Exa seed, only fixture researcher → buyer-WHO ineffective | When live-authorized + budget, attach `createLiveDeepResearchResearcher` (or injected mock) for `ensureBuyerWhoResearchExecuted` |
| No explicit BUYER_WHO stage in chain observability | Emit `BUYER_WHO_RESEARCH` (+ live researcher attach / fixture reasons) |
| Junk strings as primaryContact | `isPlausibleBuyerPersonName` + strip in `finalizeContactPass` |
| LIVE_NEW vs prior inventory contact loss | `gdi-opportunity-contact-lineage-reuse-v1.js` + materialize wiring |

Primary file: `lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js`

---

## 2. Existing buyer-WHO modules now connected

- `ensureBuyerWhoResearchExecuted` (`autonomous-deep-research-recovery-v1.js`)
- `runPublicBuyerIntelligencePass` (`commercial-depth-buyer-intelligence-v1.js`)
- `drainRelatedEntityFollowUpsThroughOrs` / extract helpers
- `createLiveDeepResearchResearcher` (authorized live only; fail-closed without auth/budget)
- `#583` presentation + org-route gate preserved (not duplicated)

---

## 3. Actual execution-path certification ($0 mocked)

`npm run test:gdi-buyer-who-e2e-recovery-v1` → **CERTIFICATION_PASS**

Also `#583` suite remains **CERTIFICATION_PASS**.

Mocked traces: `reports/gdi-buyer-who-e2e-recovery-v1/EXECUTION_TRACES_MOCKED.json`

Example TRACE_1 stages: `DEEP_RESEARCH_ELIGIBILITY` → `DEEP_RESEARCH` → **`BUYER_WHO_RESEARCH`** with `buyerWhoFallback.executed=true` and researcher.execute calls > 0.

Certifies:

- Buyer-WHO stage invoked (not merely CONTACT_RESEARCH / HTTP harvest)
- Authorized live does not silently require fixture-only when researcher injected / live-authorized
- Junk names rejected
- Homepage org-route rejected for finalization
- Named person without email completes
- Lineage reuse across IDs
- Paid limits fail-closed without auth
- UI empty-state has no View Research button
- Bethesda/Cambridge IDs not hardcoded into helpers

---

## 4. Original ↔ LIVE_NEW identity reconciliation

Module: `gdi-opportunity-contact-lineage-reuse-v1.js`

- `buildDemandClusterKey(org|programme/title)`
- Reuses prior named contact when cluster matches and current primary missing/junk
- Refuses unrelated orgs and conflicting dated titles
- Wired into `gdi-design-hotels-v3-deep-materialize.mjs` (future merges)
- Does **not** delete historical IDs

---

## 5–7. Design Hotels $0 reassessment (saved evidence only)

`ZERO_DOLLAR_REASSESS_PLAN.json` (plan only — **STAGED pointers not rewritten**):

| Metric | Value |
|--------|------:|
| Recovered named via lineage at $0 | **0** |
| Strict named finalization-ready (existing) | **15** |
| Still need live buyer-WHO | **77** (org-route language without named completion in sampled staged set) |

Honest read: most of the 113 gaps never had a verified person in prior inventory under a matching cluster — orchestration restore enables future paid buyer-WHO; it does not invent names at $0.

---

## 8. Best-contact tile/drawer regression

Preserved from **#583** (CI green, MERGEABLE):

- Named person on tiles without email
- No Contact Research Required on completed tiles
- Neutral STAGED empty copy
- Strict org-route validation

---

## 9. Remaining buyer research gaps

~77–113 opportunities still lack a verified named buyer after $0 reuse. They require **authorized live buyer-WHO** (existing module path now connected), not a new engine.

---

## 10. Proposed incremental Exa budget (only if authorized)

From reassess plan:

- ~**$0.15**/gap opportunity (bounded buyer queries)
- Suggested ceiling ≈ **$11.6** for remaining gaps (fail-closed ledgers unchanged)
- Findymail/Surfe remain HOLD

---

## 11. PR and CI

- **Depends on / stacks:** PR [#583](https://github.com/joandejarden-create/my-operators-backend/pull/583) (all checks green, MERGEABLE) — IC should merge #583 then this, or merge this stack.
- This PR: opened from `fix/gdi-buyer-who-e2e-recovery-v1`
- Local: `test:gdi-buyer-who-e2e-recovery-v1` PASS; `test:gdi-best-suggested-contact-restore-v1` PASS; `dealality:ci-precheck` to be run before push

---

## 12. Explicit confirmation — no paid calls

No Exa, Findymail, Surfe, Airtable opportunity/contact writes, CURRENT mutation, customer publication, or production deployment occurred in this task.

Historical V3 STAGED snapshots preserved; corrected staging is **plan-only** (`gdi_dh_buyer_who_recovery_*` prefix proposed).

---

### Final acceptance (this PR)

The Full E2E restore path **demonstrably emits and persists BUYER_WHO_RESEARCH** with live/mocked researcher.execute — success is **not** certified merely because CONTACT_RESEARCH or official-source HTTP ran.

**AWAITING_INTEGRATION_CONTROLLER**
