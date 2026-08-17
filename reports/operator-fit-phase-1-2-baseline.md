# Operator Fit Engine Phase 1–2 — Baseline

**Date:** 2026-08-03  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b`  
**Mode:** Controlled implementation beside legacy OAS (`operatorFitEngineV2`, default off)

---

## Repository verification (matches audit)

| Area | Path | Status |
| ---- | ---- | ------ |
| Legacy OAS score | `api/my-deals.js` → `scoreOperatorMatchForDeal` | Confirmed active; **protected** |
| Factor helpers | `lib/operator-alignment-scoring-factors.js` | Protected |
| Weights | `lib/operator-alignment-scoring-weight-config.js` | Protected |
| Company snapshot | `lib/operator-alignment-company-utils.js` | Protected |
| OAS API | `api/operator-alignment-snapshot.js` | Protected |
| Brand Match v2 | `api/match-score-server.js`, `lib/brand-match-scoring-weight-config.js` | Protected |
| Operator load | `api/lib/operator-setup-new-base-read.js`, `loadActiveOperatorCandidatesForAlignment` | Confirmed |
| Factor bars UI | `public/js/match-score-breakdown-ui.js` | Reuse pattern |
| OAS/BAS cards | `public/js/operator-alignment-snapshot.js`, BAS CSS | Reuse tokens |
| Target List | `api/target-list.js` | **Brand-only** — cannot safely store operators |
| Feature-flag pattern | Env `*_ENABLED=1` / `=0` (e.g. `OPERATOR_SETUP_USE_NEW_BASE_WRITER`, `PARTNER_INTELLIGENCE_*`) | Follow same |

---

## Feature-flag convention (this feature)

- Env: `OPERATOR_FIT_ENGINE_V2=0` (default off)  
- Semantic name: `operatorFitEngineV2`  
- Server-controlled only; client may read status from API, not enable the engine itself  

---

## Baseline tests (pre-implementation)

| Test | Result |
| ---- | ------ |
| `node scripts/validate-operator-alignment-phase-5e.mjs` | Pass |
| `node scripts/validate-operator-alignment-companies.mjs` | Pass |
| `node scripts/test-operator-alignment-snapshot-page.mjs` | **2 pre-existing FAIL**: My Deals operator-alignment action string; compact preview renderer string — **not fixed in this assignment** |

## Post-implementation verification (2026-08-03)

| Test | Result |
| ---- | ------ |
| `npm run test:operator-fit-v2` | **Pass** |
| `npm run operator-fit-v2-shadow-comparison` | Wrote reports; generic #1 legacy 2→v2 0; sparse>69 legacy 4→v2 0 |
| Phase 5E / companies validators | Pass (unchanged) |
| OAS page test | Same 2 pre-existing failures |

**Airtable writes:** none  
**Legacy OAS / Brand Match v2 / intake mappings:** unchanged  
**Default flag:** `OPERATOR_FIT_ENGINE_V2=0`


---

## Files expected to change / add

**Add:** `lib/operator-fit/**`, `api/operator-fit-v2.js`, `public/js/operator-fit-v2-cards.js`, `public/css/operator-fit-v2.css`, `public/operator-fit-alignment.html`, `scripts/test-operator-fit-v2.mjs`, `scripts/operator-fit-v2-shadow-comparison.mjs`, ADR, this baseline, `.env.example` flag line, `server.js` route registration (minimal), optional My Deals hook behind flag.

**Protected (must not change logic):**  
`scoreOperatorMatchForDeal` body/weights, Brand Match v2, owner intake field bindings, Target List schema, Airtable field names.

---

## Explicit non-goals this phase

Airtable backfills/writes, pathway matrix, sensitivity UI, OAS deprecation, intake changes, AI scoring.
