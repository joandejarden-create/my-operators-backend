# Bethesda Marriott — GDI Full E2E Opportunity Quality Certification

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Contract:** `gdi-opportunity-quality-contract-v1`  
**Orchestrator:** `gdi-opportunity-full-e2e-quality-orchestrator-v1`  
**Generated:** see `00-orchestration-summary.json`

## Safety

- Paid research: **NOT AUTHORIZED** (proposal only; prior Exa allowance exhausted)
- Canonical `opportunities.json` writes: **NOT APPLIED** (dry-run manifest only; founder approval required)
- Historical research-stage: protected merge applied where remediations existed
- Customer publication: **OFF**
- Sales Ready product status: **not auto-promoted**

## 1. Full 88-record E2E execution coverage

All **88** raw IDs loaded and processed with per-ID checkpoints under `checkpoints/`.

| Inventory | Count |
|-----------|------:|
| Canonical | 59 |
| Historical research-stage | 29 |
| Union processed | 88 |

## 2. Unique commercial opportunity count

**83** unique commercial opportunities (4 linked research + 1 quarantined suppressed from actionable list).

## 3. Record-level commercial certification (after projected)

| Outcome | Count |
|---------|------:|
| SALES_READY_ELIGIBLE | 1 |
| ACCOUNT_DEVELOPMENT_ELIGIBLE | 27 |
| PARTNERSHIP_WATCH | 7 |
| OVERFLOW_WATCH | 7 |
| RESEARCH_CONTINUATION | 30 |
| LINKED_RESEARCH | 4 |
| QUARANTINED | 1 |
| DISQUALIFIED | 11 |

Research integrity: **87 PASS** · **1 QUARANTINED**.

## 4. Before / after title, description, category

| Metric | Before | After (projected) |
|--------|-------:|------------------:|
| Title OK | 88 | 88 |
| Category OK | (see `03-before-after-quality-summary.json`) | same |
| Description OK | (see summary) | same |

Titles were previously repaired (#186/#187). Quarantined credential row preserved without cosmetic rewrite.

## 5. Before / after verified named-contact coverage

| Metric | Before | After projected |
|--------|-------:|----------------:|
| Named relevant primary | 63 | 63 |
| Direct person emails | (summary) | (summary) |
| Org route coverage | (summary) | (summary) |

Live named coverage already reflects prior 12 PASS patches + historical zero-paid enrichments. No regression.

## 6. Evidence and hotel-fit quality

Hotel-fit honesty and demand-thesis axes evaluated per record. Thin hotel-fit / speculative demand remain the primary continuation drivers (see record-level `01-record-level-reconciliation.json`).

## 7. Maturity outcomes

Engine eligibility on proposed bodies is reported separately from product labels. Product maturity fields were **not** mutated. Account Development is not forced through Sales Ready criteria.

## 8. Duplicate and quarantine

- CTN / NCI families linked (see `04-commercial-event-family-crosswalk.json`)
- Quarantined: `gdi_opp_series_if_prompted_enter_meeting_id_250_658_877__2027`
- Raw 88 IDs preserved; no research history deleted

## 9. Historical research-stage remediations

Protected versioned merge applied (`historicalPersist.writeApplied=true`). No production publication.

## 10. Canonical remediation manifest awaiting approval

`02-canonical-remediation-dry-run-manifest.json` — **0 outstanding field patches** after quarantine no-rewrite fix (prior 12 contact patches already live on store SHA `6a97feda…`).

**Do not overwrite `opportunities.json` without explicit founder authorization.**

## 11. Remaining blockers + smallest proposed paid budget

See `05-proposed-paid-research-batch.json`:

- Provider: Exa Agent (existing Full E2E contact deep research)
- Calls: **3**
- Ceiling: **USD $12**
- IDs: NADO WashCon 2028, NADO 2027 overflow, High Risk High Reward symposium, NCI RNA Biology primary, AMWA 2028
- **NOT AUTHORIZATION — STOP before paid**

## 12. Durable automated quality-gate integration

- Contract: `lib/group-demand-intelligence/gdi-opportunity-quality-contract-v1.js`
- Orchestrator: `lib/group-demand-intelligence/gdi-opportunity-full-e2e-quality-orchestrator-v1.js`
- Publication readiness: `assertPublicationWriteReadiness` fail-closed via quality gate
- Customer surface: quarantine / `TITLE_SOURCE_INVALID` never keepActive
- Weekly refresh downstream: quality gate review step (auto-publish remains OFF)

## 13. Tests and customer-publication isolation

- `npm run test:gdi-bethesda-88-full-e2e-opportunity-quality-v1`
- Covers 88 IDs, contract axes, quarantine, linked family, AD vs SR separation, anti-clobber, canonical write refusal, other-hotel non-regression

## 14–15. GitHub / local SHA / server

Filled at PR merge time in the chat founder report.
