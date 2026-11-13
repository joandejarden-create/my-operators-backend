# GDI Opportunity Type Classification Regression — Founder Report

**Date:** 2026-11-13  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Contract focus:** Customer-facing Opportunity Type must map every visible row to the approved demand-segment taxonomy; never surface Other / Unclassified as a filter.

---

## Verdict

**Fixed at source.** ACVNU Renal Week 2027 is restored to **Association / Conference / Events**. Bethesda still has **67** unique opportunities. Other / Unclassified is gone from the Opportunity Type dropdown. Market Scope remains **All 67 / Hotel 61 / Competitive 10**.

---

## Exact root cause

**Both data-related and mapping-related.**

1. **Data / remediation overwrite:** `applyCmiLedgerRemediation` in `cmi-evidence-ledger-v1.js` wrote the inventory-population label `COMPETITIVE_MARKET_INTELLIGENCE` into `opportunityType` for CMI ledger rows (including ACVNU). That field is a **demand** type on the customer surface, not a Market Scope / inventory population.
2. **Mapping fallthrough:** `classifyOpportunityTypeFilter` treated the polluted `opportunityType` as unknown demand → **OTHER_UNCLASSIFIED**, which then appeared as **Other / Unclassified (1)** in the Opportunity Type dropdown.

Original research evidence already identified ACVNU as an association conference (American College of Veterinary Nephrology & Urology) with overflow housing at The Bethesdan — not an unclassified / “other” demand segment.

| Field | Before (broken) | After (restored) |
|-------|-----------------|------------------|
| `opportunityType` | `COMPETITIVE_MARKET_INTELLIGENCE` | `OVERFLOW_HOUSING` (commercial mechanism) |
| `demandSegmentId` | missing / ignored | `associations_conferences_events` |
| Filter category | `OTHER_UNCLASSIFIED` | `ASSOCIATION_CONFERENCE_GROUP` |
| Filter label | Other / Unclassified | Association / Conference / Events |
| Market Scope | Competitive (unchanged) | Competitive (unchanged) |

---

## Bethesda inventory confirmation

| Check | Result |
|-------|--------|
| Unique opportunities | **67** |
| Unclassified / Other count | **0** |
| ACVNU under Association filter | **Yes** (browser + API) |
| Other / Unclassified in dropdown | **Absent** |
| Market Scope | All **67** / Hotel **61** / Competitive **10** |
| Evidence / contacts / sales feedback | Unchanged |

API type mix (sums to 67): Scaling 22 · Association 19 · Local Catering 7 · Preferred Lodging 5 · + remaining approved segments (no Other).

---

## Additional records / hotels

- **Bethesda customer inventory:** only ACVNU was customer-visible as Other; fix restores that one row. Related CMI ledger remediations (NADO, ICON, CASSS) now also set explicit `demandSegmentId` without overwriting demand `opportunityType` with CMI.
- **Multi-hotel FS audit:** 18 other hotel folders still contain rows that classify as Other when scored with the same classifier (`reports/gdi-opportunity-type-classification-v1/00-multi-hotel-audit.json`). Those are **flagged for remediation**, not hidden, and Bethesda publication stays clean.

---

## Files and functions corrected

| Path | Change |
|------|--------|
| `lib/.../commercial-consistency/cmi-evidence-ledger-v1.js` | ACVNU (+ related CMI) remediations set `demandSegmentId`; `applyCmiLedgerRemediation` **never** writes `COMPETITIVE_MARKET_INTELLIGENCE` into demand `opportunityType` |
| `lib/.../opportunity-type-filter-v1.js` | Ignore inventory-population values as demand types; overflow → Association; medical/scientific aliases; omit Other from `opportunityTypeFilterOptions`; `findUnclassifiedCustomerVisibleOpportunities`; `isCustomerFacingOpportunityTypeFilter` |
| `public/js/.../dealality-gdi-ui.js` | Mirror classifier + skip rendering Other option |
| `scripts/test-gdi-opportunity-type-classification-v1.mjs` | Regression lock |
| `package.json` | `test:gdi-opportunity-type-classification-v1` |
| `public/group-demand-intelligence.html` | Cache-bust `gdi-opp-type-classification-v1` |

---

## Prevention

1. Customer-visible rows must classify into an approved filter (test asserts Bethesda = 0 unclassified).
2. Taxonomy alignment via shared `opportunity-type-filter-v1` + UI mirror.
3. CMI remediations preserve demand types; inventory population stays on Market Scope / `commercialActionability`.
4. Other is never a customer-facing filter option; invalid rows fail internal validation helpers instead of becoming dropdown choices.
5. Multi-hotel audit JSON lists remaining unclassified inventories for follow-up remediation.

---

## Test results

```
npm run test:gdi-opportunity-type-classification-v1  → PASS (5/5)
npm run test:gdi-regression-recovery-v1           → PASS
npm run test:gdi-market-scope-toggle-v1           → PASS (7/7)
```

**Browser (local :8080):** Opportunity Type = All types (67) with no Other option; filter Association shows **ACVNU Renal Week 2027**; Market Scope All / Hotel / Competitive = 67 / 61 / 10 on Reset View.

Artifacts: `reports/gdi-opportunity-type-classification-v1/`
