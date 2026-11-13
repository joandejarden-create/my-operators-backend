# GDI — Portfolio-Wide Opportunity Type Remediation

**Date:** 2026-11-13  
**Branch:** `fix/gdi-regression-recovery-v1`  
**Authorization:** Local remediation + regression only — **not published / not deployed**

---

## Verdict

Bethesda remains clean (**67** opportunities, **0** unclassified, Market Scope **All 67 / Hotel 61 / Competitive 10**).

Across the portfolio, **182 / 252** previously unclassified opportunities now map to the approved demand-segment taxonomy via documented research aliases and lifecycle guards. **70** remain flagged for research remediation (not hidden, not force-classified). **Other / Unclassified never appears in customer-facing Opportunity Type dropdowns.**

---

## Root-cause mix (original 252)

| Failure class | Approx share | Description |
|---------------|--------------|-------------|
| Lifecycle pollution | ~136 | `opportunityType` set to `FUTURE_CYCLE` / `FUTURE_WATCH` (commercial status) instead of demand segment |
| Legacy free-text segments | ~78 | Hunter labels like `Housing / Overflow`, `wedding`, `business`, `Marine / Boat Show` |
| Weak / missing research fields | ~rest | `Discovered`, null segment, no recoverable axis |

**Not a Bethesda-only frontend bug** — shared mapping + research write patterns.

---

## What was restored

### Classification layer (read-path; no evidence/contact mutation)

New module: `lib/group-demand-intelligence/legacy-demand-segment-alias-map-v1.js`

- Documented free-text research segment → canonical demand segment map
- Treats lifecycle types (`FUTURE_CYCLE`, `FUTURE_WATCH`, CMI populations, etc.) as **non-demand**
- Recovers Association from Overflow/Housing labels + `EXHIBITOR BLOCK` research title contracts
- Leaves unresolved rows as internal `OTHER_UNCLASSIFIED` (validation flag only)

Wired through:

- `classifyOpportunityTypeFilter` (`opportunity-type-filter-v1.js`)
- `toOpportunityListDto` (normalize before filter category)
- Dropdown options still omit Other

### Future research protection

- `applyLiveCommercialQuality` — preserves existing `demandSegmentId` when lifecycle types change
- `saveOpportunitiesCanonical` / `upsertSingleOpportunity` — `preserveDemandSegmentOnResearchUpdate` merge guard so research updates cannot silently erase valid demand segments

---

## Portfolio validation

| Metric | Value |
|--------|------:|
| Hotels audited | **35** |
| Opportunities audited | **973** |
| Originally unclassified | **252** |
| Corrected (now valid taxonomy) | **182** |
| Still requiring research | **70** |
| Hotels still with ≥1 unresolved | **15** |
| Other in any customer dropdown | **No** |
| Bethesda 67 / unclassified | **67 / 0** |

Artifacts:

- Baseline: `00-multi-hotel-audit.json`
- Detail: `02-unclassified-detail.json`
- Post-remediation: `03-portfolio-remediation-audit.json`, `00-multi-hotel-audit-post-remediation.json`

Re-run: `npm run gdi:opportunity-type-portfolio-audit`

---

## Still requiring research (70)

These keep authorized visibility; they fail internal classification readiness until researchers attach a real demand segment / axis. Typical patterns:

- `segment: Discovered` with no research axis / mechanism label
- Blank segment + `FUTURE_WATCH` only
- Ambiguous free-text not in the documented alias map

Listed in `03-portfolio-remediation-audit.json` → `stillRequiringResearch`.

---

## Regression protection

| Suite | Result |
|-------|--------|
| `npm run test:gdi-opportunity-type-classification-v1` | PASS (incl. portfolio recovery ≥100 + Bethesda 67/61/10) |
| `npm run test:gdi-regression-recovery-v1` | PASS (now **chains** classification tests) |

---

## Files

| Path | Role |
|------|------|
| `lib/.../legacy-demand-segment-alias-map-v1.js` | Documented aliases + inference + persist guard |
| `lib/.../opportunity-type-filter-v1.js` | Classifier v4 uses alias map / lifecycle NON_DEMAND |
| `lib/.../opportunity-list-dto.js` | Normalize demand segment on list projection |
| `lib/.../live-commercial-quality-v1.js` | Preserve demandSegmentId |
| `lib/.../opportunity-persistence.js` | Preserve on save/upsert |
| `scripts/test-gdi-opportunity-type-classification-v1.mjs` | Regression lock |
| `scripts/gdi-opportunity-type-portfolio-audit-v1.mjs` | Portfolio audit CLI |

---

## Explicit non-changes

- No Airtable/production publish
- No deletion or suppression of unresolved rows
- No inventing categories from titles alone (except documented research contracts: EXHIBITOR/PARTNER BLOCK)
- No changes to contacts, evidence, sales feedback, or Bethesda Market Scope eligibility
