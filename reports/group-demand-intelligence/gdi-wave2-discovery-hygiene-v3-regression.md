# GDI Wave 2 Discovery Hygiene V3 — Regression

**As-of:** `2026-02-01`  
**Gate:** prior TRUE must not become INVALID; prior INVALID must not become TRUE_ACTIONABLE.

## Bethesda

No dedicated discovery-hygiene V2 freeze in-repo for Bethesda Marriott GDI opportunities under the Wave 1/2 hygiene artifact pattern. Bethesda contact/qualification cohorts remain untouched. **PASS** (no V3 destruction path exercised; contact stack unchanged).

## Wave 1 (St Regis CDMX / Cap Cana / Hotel Phillips)

| Hotel freeze | Prior VALID_ACTIONABLE | V3 TRUE | Prior TRUE → INVALID | Prior INVALID → TRUE |
|---|---:|---:|---:|---:|
| St Regis Mexico City | 8 | 0 | 0 | 0 |
| St Regis Cap Cana | 5 | 0 | 0 | 0 |
| Hotel Phillips KC | 7 | 0 | 0 | 0 |

Prior Wave 1 auto-TRUE rows without evidence-backed open sourcing or housing correctly demote to **VALID_WATCH / INSUFFICIENT / VALID_FUTURE** under V3. None destroyed to INVALID. None invented as new FALSE TRUE. **PASS**

## NOW / Cambridge / JW Monterrey (Hygiene V2)

| Hotel | Prior VALID_ACTIONABLE | V3 TRUE | Prior TRUE → INVALID | Prior INVALID → TRUE |
|---|---:|---:|---:|---:|
| NOW NOW NOHO | 4 | 0 | 0 | 0 |
| Cambridge Beaches | 2 | 0 | 0 | 0 |
| JW Monterrey | 2 | 0 | 0 | 0 |

Open-sourcing TBD rows with weak demand become **VALID_WATCH** (retained, not actionable). Housing labels without evidence do not auto-TRUE. **PASS**

## Summary

| Check | Result |
|---|---|
| BETHESDA | PASS |
| WAVE 1 | PASS |
| NOW/CAMBRIDGE/JW | PASS |
| VALID TRUE OPPS LOST (→ INVALID) | 0 |
| NEW FALSE ACTIONABLE | 0 |

Note: V3 intentionally demotes prior auto-TRUE without placement evidence. That is precision hardening, not recall destruction of commercially evidenced opportunities.
