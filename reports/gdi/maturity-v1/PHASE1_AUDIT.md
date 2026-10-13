# GDI Maturity Funnel — Phase 1 Audit Report

**Date:** 2026-10-05  
**Mode:** Phase 1 only (no expansion engine, no speculative W Rome opportunities)  
**Storage:** payload-first (`opportunityPayloadJson` / filesystem opportunities) — no new Airtable columns

---

## 1. FILES CHANGED

| Path | Role |
| --- | --- |
| `lib/group-demand-intelligence/gdi-evidence-taxonomy-v1.js` | Evidence type enum + claimKind bridge |
| `lib/group-demand-intelligence/gdi-maturity-v1.js` | Canonical maturity SoT + workflow separation |
| `lib/group-demand-intelligence/gdi-maturity-qualified-v1.js` | QUALIFIED evaluator + cohort/lodging fields |
| `lib/group-demand-intelligence/customer-readiness-gate-v1.js` | `isGdiMaturityActionable` alias |
| `lib/group-demand-intelligence/feature-flag.js` | `GDI_MATURITY_FUNNEL_V1`, `GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1` |
| `lib/group-demand-intelligence/customer-visibility.js` | Prepared QUALIFIED visibility (flagged OFF) |
| `lib/group-demand-intelligence/opportunity-list-dto.js` | Maturity / cohort / lodging DTO fields |
| `lib/group-demand-intelligence/index.js` | Public exports |
| `scripts/gdi-maturity-backfill-v1.mjs` | Dry-run classifier |
| `scripts/test-gdi-maturity-*-v1.mjs` | Phase 1 test pack (5 scripts) |
| `scripts/test-gdi-card-tile-runtime-smoke.mjs` | Stub `readinessPillHtml` for extract smoke |
| `.env.example` | New flag docs |
| `package.json` | npm script wiring |

---

## 2. DATA MODEL ADDED (payload fields)

- `gdiMaturityState` · `gdiMaturityReason` · `gdiMaturityEvaluatedAt` · `gdiMaturityEvidenceSummary`
- `parentDemandSignalId` · `parentDemandSignalLabel` · `parentDemandSignalType`
- `travelingCohortType` · `travelingCohortSummary` · `travelingCohortEvidence` · `travelingCohortConfidence`
- `lodgingControlHypothesis` · `lodgingControlSummary` · `lodgingControlConfidence` · `lodgingControlEvidence`
- `modeledRoomsMin/Max` · `modeledNightsMin/Max` · `modeledDemandBasis` · `modeledAncillary`
- `lodgingVerified` · `headcountVerified` · `evidenceItems` · `missingValidation`
- `buyerResearchStatus` · `recommendedNextAction` · `salesWorkflowState`

No Airtable schema changes.

---

## 3. MATURITY TRANSITION TABLE

| From | To | Rule |
| --- | --- | --- |
| — | SIGNAL | Generator/campaign shell, or no named account |
| SIGNAL | CANDIDATE | Named account present; QUALIFIED bar not met |
| CANDIDATE | QUALIFIED | All QUALIFIED requirements (cohort + lodging hypothesis + evidence + fit + honesty) |
| QUALIFIED | ACTIONABLE | Strict Ready gate (`isGdiCustomerOpportunityReady`) |
| any | demotion | Allowed on re-evaluation (no forced promotion) |

Sales workflow is **not** advanced by maturity. Default `UNTOUCHED`.

---

## 4. ACTIONABLE PARITY RESULT

**PASS — 162/162 (100%)** across Bethesda + W Rome + YOTEL.

`oldReady === newActionable` with zero mismatches.  
Artifact: `reports/gdi/maturity-v1/actionable-parity.json`

---

## 5. BACKFILL DRY-RUN RESULTS

Focus hotels only (162 rows). **No writes.**

| Hotel | Total | Old Ready | Old Visible | SIGNAL | CANDIDATE | QUALIFIED | ACTIONABLE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Bethesda (`recLuxvwwxID7U2B8`) | 54 | 28 | 28 | 0 | 26 | 0 | 28 |
| W Rome (`rece0or38cxo3Fymb`) | 13 | 0 | 5 | 0 | 13 | 0 | 0 |
| YOTEL (`recrPQcZg7SFARRb2`) | 95 | 2 | 2 | 1 | 92 | 0 | 2 |

Artifacts: `backfill-dry-run.md` · `backfill-dry-run.csv` · `backfill-dry-run-summary.json`

---

## 6–8. BEFORE / AFTER MATURITY COUNTS

| Hotel | Before (customer-visible / Ready) | After maturity (dry-run) |
| --- | --- | --- |
| **W Rome** | 5 visible via expansion pilot flag; 0 Ready | 0 QUALIFIED / 0 ACTIONABLE under canonical evaluator; 13 CANDIDATE |
| **YOTEL** | 2 Ready / 2 visible | 2 ACTIONABLE · 92 CANDIDATE · 1 SIGNAL |
| **Bethesda** | 28 Ready / 28 visible | 28 ACTIONABLE · 26 CANDIDATE |

W Rome remaining at **0** canonical QUALIFIED/ACTIONABLE customer-visible rows under Phase 1 defaults is **acceptable** (no speculative refill). Pilot visibility remains behind `GDI_WROME_EXPANSION_PILOT_V0` only.

---

## 9–10. ROWS THAT BECAME QUALIFIED

**None** under the Phase 1 evaluator.

Why: existing production rows lack required `travelingCohortEvidence[]` + explicit lodging-control hypothesis + QUALIFIED account class bundle. Ready survivors go straight to **ACTIONABLE**. Shells stay **CANDIDATE/SIGNAL**.

---

## 11. LEGACY FIELD CONTRADICTIONS

| Observation | Notes |
| --- | --- |
| W Rome pilot rows store `gdiMaturityState=QUALIFIED` | Canonical re-eval → CANDIDATE (missing cohort evidence objects for Phase 1 contract). Pilot visibility path still uses stored stamp + pilot flag — intentional Phase 0 bridge. |
| `newlyHidden: 5` in dry-run visibility delta | Artifact of stamping CANDIDATE onto pilot QUALIFIED rows while simulating global QUALIFIED flag — **not** a production hide. Keep `GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1=0`. |
| `customerFacingState` / `bookingWindowStatus` / `funnelStage` | Retained; `gdiMaturityState` is SoT when funnel flag is used. |

No Ready/ACTIONABLE contradictions.

---

## 12. REGRESSION RESULTS

| Suite | Result |
| --- | --- |
| `test:gdi-maturity-transitions-v1` | PASS |
| `test:gdi-maturity-qualified-honesty-v1` | PASS |
| `test:gdi-maturity-actionable-parity-v1` | PASS (162) |
| `test:gdi-maturity-workflow-separation-v1` | PASS |
| `test:gdi-maturity-generator-isolation-v1` | PASS |
| `test:gdi-customer-visibility` | PASS |
| `test:gdi-readiness-visibility-convergence-v1` | PASS |
| `test:gdi-live-visibility-integrity-v1` | PASS |
| `test:gdi-card-presentation-regression` | PASS |

---

## 13. RISKS / FOLLOW-UP

1. Keep **`GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1=0`** until Phase 2 (0 accidental QUALIFIED today, but flag stays safe-default).
2. W Rome pilot QUALIFIED stamp ≠ canonical QUALIFIED — Phase 2 may bridge pilot corpus onto Phase 1 evaluator (add `travelingCohortEvidence[]`) without lowering gates.
3. Phase 3: hotel-fit profile weights (hook already present via `hotelFitProfile.evaluate`).
4. Phase 4: expansion engine + gold-set metrics (`createEmptyMaturityMetricsScaffold()` reserved).
5. Do **not** enable global QUALIFIED visibility merely to refill W Rome card count.

---

## Gate / honesty checklist

| Check | Result |
| --- | --- |
| Gate bypass used? | **NO** |
| Speculative accounts created? | **NO** |
| ACTIONABLE thresholds lowered? | **NO** |
| ADP / share tokens changed? | **NO** |
| Airtable columns added? | **NO** |
| Expansion engine generalized? | **NO** |

**STOP** — Phase 1 complete. Awaiting founder review before expansion engine.
