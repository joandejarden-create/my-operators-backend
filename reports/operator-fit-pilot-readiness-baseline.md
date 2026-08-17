# Operator Fit — Pilot Readiness Baseline

**Date:** 2026-08-04  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b`  
**Feature flag:** `OPERATOR_FIT_ENGINE_V2` unset / default **off**

---

## Tests (pre-write)

| Suite | Result |
| ----- | ------ |
| `test:operator-fit-v2` | Pass |
| `test:operator-fit-readiness` | Pass |
| `test:operator-intelligence-calibration` | Pass |
| `test:operator-intelligence-airtable-wave-2` | Pass |
| `operator-fit-v2-shadow-comparison` | Pass (generic-first v2=0) |
| OAS snapshot page | **2 pre-existing FAIL** (out of scope) |

---

## Current state (entering gate)

| Item | State |
| ---- | ----- |
| Active operator universe | ~24 Active candidates |
| Calibration Airtable | Applied (Group A + Claims + Case Studies) |
| Wave 2 Airtable | Research complete; **not** fully persisted (pre-gate) |
| Market Presence | Architecture recommended; **not** yet table |
| Cenote residual | Active Countries=`[Mexico]` vs overlay Claimed Capability |
| Deal A / B / C Ranking Ready | 2 / **0** / 5 |
| Argentina in Active Countries (scan) | **0 operators** |

---

## Expected writes this gate

1. Create `Operator Intelligence - Market Presence`  
2. Migrate calibration + Wave 2 geography → Market Presence rows  
3. Persist Wave 2 Group A fields / claims / comps (deduped)  
4. Cenote: keep Mexico-only Active Countries; Claimed Capability Market Presence SoT for eligibility  

## Protected

Legacy OAS · Brand Match v2 · owner intake · My Deals unwired · feature flag off · no production Shortlist · no ODR-as-shortlist · no threshold weakening for Deal B.
