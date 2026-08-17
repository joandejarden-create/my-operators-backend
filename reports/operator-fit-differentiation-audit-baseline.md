# Operator Fit — Differentiation Audit Baseline

**Date:** 2026-08-04  
**Mode:** Audit only — no scoring / UI / Airtable / enablement changes

## Environment

| Item | Value |
| ---- | ----- |
| Branch | `app-shell-left-nav` |
| Commit | `3c88c0b4e22a35052e450d00c5e2f1b9e417c040` |
| Engine version | from `OPERATOR_FIT_ENGINE_VERSION` |
| `OPERATOR_FIT_ENGINE_V2` | **0** (disabled) |
| `OPERATOR_FIT_INTERNAL_PILOT` | **0** |
| My Deals wired to Fit v2 | **No** |

## Tests run (baseline)

| Suite | Result |
| ----- | ------ |
| `test:operator-fit-v2` | Passed (incl. legacy OAS numeric/fee checks) |
| `test:operator-fit-readiness` | Passed |
| `test:operator-fit-internal-pilot` | Passed |
| `test:operator-fit-corrected-owner-pilot-gate` | Passed |
| `test:operator-fit-final-internal-pilot` | Passed |
| `validate-operator-alignment-phase-5e.mjs` | Passed |
| `test-company-type-normalize.mjs` | Passed |
| Differentiation audit harness | Wrote `reports/operator-fit-differentiation-audit.json` |

## Deal C project inputs (current pilot evaluation path)

| Field | Value |
| ----- | ----- |
| Country | Mexico |
| Chain scale | Upper Upscale |
| Building type | Mixed-Use |
| Development type | New Build |
| Preferred brands on path | **[]** (empty) |
| Preferred structures on path | **[]** (empty) |
| Market Presence requirement | Active country operations required |

## Santa Fe / Highgate arithmetic (identical)

| Step | Santa Fe | Highgate |
| ---- | -------: | -------: |
| Operator–Project raw | 59.0 | 59.0 |
| Structure layer | unknown (0 / denom 15) | unknown (0 / denom 15) |
| Brand layer | N/A (skipped) | N/A (skipped) |
| Primary before risk | 48.6 | 48.6 |
| Execution risk | −10 | −10 |
| Raw / Displayed | 38.6 | 38.6 |
| Evidence Strength | Strong | Strong |
| Coverage | 71.6% | 71.6% |
| Ceiling | none | none |
| Band | Limited | Limited |
| Eligibility | With Conditions | With Conditions |
| Readiness | Ranking Ready | Ranking Ready |
| Engine rank | **1** | **2** |

## Ranking logic (production)

1. Eligibility preference → 2. Displayed → 3. Evidence → 4. Coverage → 5. Lower risk → 6. `candidateId` localeCompare  

Tie-break: `reckyv9O0Y3auYpJJ`.localeCompare(`recLjxtxIIVJaGbXK`) = **−1** → Santa Fe first.

## Owner-facing explanations (differ)

- **Santa Fe Why:** Active country Mexico · Supports Upper Upscale · GSF Mexico third-party managed hotels (portfolio)  
- **Highgate Why:** Active country Mexico · Supports Upper Upscale · The Ocean Club, a Luxury Collection Resort (DR)  

Full objects: `reports/operator-fit-differentiation-audit.json` → `dealC.santaFe` / `dealC.highgate`.
