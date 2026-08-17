# Operator Fit — Internal Pilot Baseline

**Date:** 2026-08-04  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b4e22a35052e450d00c5e2f1b9e417c040`

---

## Feature flags

| Flag | Baseline |
| ---- | -------- |
| `OPERATOR_FIT_ENGINE_V2` | **0** (owner off) |
| `OPERATOR_FIT_ENGINE_V2_SHADOW` | unset / off |
| `OPERATOR_FIT_INTERNAL_PILOT` | **0** (enable locally for UI/API) |
| `OPERATOR_FIT_PILOT_DEAL_ALLOWLIST` | unset until pilot session |
| `OPERATOR_FIT_INTERNAL_PILOT_ALLOW_PRODUCTION` | **0** |

## Current internal routes

| Route | Role |
| ----- | ---- |
| `/internal/operator-fit-calibration.html` | Calibration cohort review |
| `/internal/operator-fit-data-readiness.html` | Data readiness |
| `/internal/operator-fit-pilot.html` | **New** advisor pilot workflow |
| `/api/operator-fit/v2/*` | Owner-gated Fit v2 (flag off → 404) |
| `/api/support/operator-fit-internal-pilot/*` | **New** admin + pilot allowlist APIs |

## Current real-deal results (pre-pilot gate)

| Deal | Production Ranking Ready | Notes |
| ---- | -----------------------: | ----- |
| Deal A (Peru urban) | 2 | Highgate, GHL |
| Deal B (Argentina leisure) | 0 | Wave 3 research-stage candidates |
| Deal C (Mexico complex) | 5 | Ranking Ready depth |

Source: `reports/operator-fit-pilot-readiness-real-deals.json`

## Readiness universe

Active Operator Setup Masters + Operator Intelligence overlays (calibration + Wave 2). Market Presence strong types gate geography eligibility.

## Argentina research-stage state (pre-onboarding)

| Operator | Prior ID | Master |
| -------- | -------- | ------ |
| Álvarez Argüelles Hoteles | `research_alvarez_arguelles` | none |
| Tremun Hoteles | `research_tremun` | none |
| AADESA | `research_aadesa` | none |

Wave 3 cohort under `data/operator-intelligence/wave-3-cohort/`.

## Existing Airtable structures (relevant)

- Operator Setup — Master / Profile / Platform / Commercial / Governance  
- Operator Intelligence — Claims / Sources / Case Studies (Wave 2)  
- Operator Intelligence — Market Presence  
- **Operator Fit - Shortlist** — to be ensured (not ODR)  
- Operator Deal Requests — **untouched** (not shortlist)

## Pre-existing failures (out of scope)

Two known OAS My Deals contract failures in `test-operator-alignment-snapshot-page.mjs` — do not block internal pilot unless pilot depends on them (it does not).

## Files expected to change (this phase)

- `lib/operator-fit/internal-pilot-access.js`, `shortlist.js`, `shortlist-store.js`, `shortlist-compare.js`, `ranking-difference.js`, `ranking-change-validations.js`, `pilot-events.js`  
- `api/operator-fit-internal-pilot.js` · `server.js` · `.env.example`  
- `public/internal/operator-fit-pilot.html`  
- Scripts: onboard, ensure shortlist, evaluate, tests  
- Docs/reports under `docs/` and `reports/`

## Production-protected modules (do not casually change)

- Legacy OAS scoring (`scoreOperatorMatchForDeal`)  
- Brand Match v2  
- Owner intake  
- My Deals owner navigation / Fit wiring  
- OAS weights  

## Test suites to re-run

`test:operator-fit-v2` · `test:operator-fit-readiness` · `test:operator-intelligence-calibration` · `test:operator-intelligence-airtable-wave-2` · `test:operator-fit-pilot-readiness` · `test:operator-fit-internal-pilot` · Phase 5E / companies validators where available · relevant OAS tests (known failures remain out of scope)
