# Operator Intelligence — Calibration Baseline

**Date:** 2026-08-03  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b`  
**Mode:** Calibration cohort + research process (local only; no Airtable writes; no production enablement)

---

## Feature-flag state

| Flag | State |
| ---- | ----- |
| `OPERATOR_FIT_ENGINE_V2` | Default **off** (`0`) — must remain off |
| My Deals wiring | **Blocked** |

---

## Active operator universe (pre-calibration)

| Metric | Value |
| ------ | ----: |
| Active operators | **24** |
| Ranking Ready | **0** |
| Conditionally Rankable | **3** (Arbor, Cenote Azul, Hotel Equities) |
| Research Required | **21** |
| Evidence coverage (avg) | **0%** |
| Eligibility coverage (avg) | **55.2%** |
| Differentiation coverage (avg) | **34.7%** |

Airtable completeness (from prior enrichment readiness): structured Active Countries ~8%; Management Structures ~12.5%; Conversion 0%; claim-level evidence 0%.

---

## Tests run (pre-change)

| Suite | Result |
| ----- | ------ |
| `npm run test:operator-fit-v2` | **Pass** |
| `npm run test:operator-fit-readiness` | **Pass** |
| `npm run operator-fit-v2-shadow-comparison` | **Pass** (generic #1 2→0; sparse>69 4→0) |
| `npm run operator-fit-data-readiness` | **Pass** |
| `npm run operator-fit-taxonomy-validation` | **Pass** (0 taxonomy issues) |
| `npm run operator-fit-evidence-validation` | **Pass** (24/24 evidence issues) |
| `npm run operator-fit-real-deal-shadow` | **Pass** — Deal A/B/C production pool **0** |
| Phase 5E validator | **Pass** |
| Companies validator | **Pass** |
| OAS snapshot page test | **2 pre-existing FAIL** (My Deals contract strings) — not fixed |

---

## Current Top-5 / synthetic behavior

- Real deals A/B/C: production Ranking Ready pool **0**; owner Top-5 not credible.
- Synthetic shadow: v2 suppresses generic-first and sparse inflation vs legacy OAS.

---

## Files expected to change / add

**Add:** `docs/audits/operator-intelligence-*`, `docs/architecture/operator-intelligence-governance-model.md`, `docs/data/operator-intelligence-*`, `docs/process/operator-intelligence-research-operating-process.md`, `docs/reviews/operator-intelligence-calibration-founder-review.md`, `data/operator-intelligence/calibration-cohort/**`, `lib/operator-intelligence/**`, calibration scripts/tests, reports, calibration UI routes.

**Protected:** `scoreOperatorMatchForDeal`, Brand Match v2, owner intake, OAS weights, Airtable schema/writers, Target List, ODR-as-shortlist, default-on flag, My Deals production wiring.

---

## Explicit non-goals

Airtable writes/schema apply, researching all 24 operators, My Deals enablement, shortlist persistence, pathway matrix, AI-as-evidence, founder field-by-field approval for routine facts.
