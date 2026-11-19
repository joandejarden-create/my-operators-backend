# Brand Explorer Phase 2 — Factory recovery + HPC pilot validation

**Status:** Staging / founder review  
**Branch intent:** `feat/be-phase2-factory-hpc-pilot-v1`  
**Date:** 2026-11-19

## Authorized scope

- Recover original ten-tab Brand Explorer factory + canonical validation contracts (PR #66 path)
- Read-only Airtable / HPC identity + footprint integration
- Representative pilot brands across affiliation / operating models
- Staging validation only

## Explicitly NOT authorized

- Mass population
- Paid research
- Production Airtable writes
- Automatic customer publication
- Weakening MASTER / quality gates to force a pass

## What was recovered

| Layer | Location |
|-------|----------|
| Product shell (10 tabs) | `configs/brand-explorer-product-shell.json` |
| Presentation / cardinality / geometry / lessons | `configs/brand-explorer-*.json` |
| Canonical engine | `lib/brand-explorer/canonical/*` |
| Tab contracts | `lib/partner-intelligence/brand-explorer-tab-contracts.js` |
| Doctor / build / audit / backscan / test | `scripts/brand-explorer-*.mjs` |
| Phase 1 SoT (merged #435) | `lib/brand-explorer/be-airtable-integration-sot-v1.js` |
| Phase 2 HPC footprint | `lib/brand-explorer/be-hpc-footprint-summary-v1.js` |

Materialization remains **disabled**. Canonical build refuses `--apply`.

## HPC rules (binding)

- Designated authority: Hotel Property Census (`Radar Status` ∈ Open / Pipeline)
- Transitional: Hotel Census until HPC brand rollup is certified
- Brand Basics `rec…` ids remain canonical identity
- Company-reported Brand Setup footprint stays separately attributed
- ADP / GDI brand-level contracts remain `RESERVED_READ_ONLY` (no speculative metrics)

### Certification semantics (founder correction)

| `dataOrigin` / state | Meaning |
|----------------------|---------|
| `synthetic_fixture` / `SYNTHETIC_FIXTURE_VALIDATION` | Unit/pipeline fixture only — **never** `HPC_BRAND_ROLLUP_CERTIFIED` |
| `live_airtable` / `LIVE_UNVERIFIED_INVENTORY` | Live rows matched; gates incomplete or independent cert not recorded |
| `live_airtable` / `LIVE_PARTIAL_COVERAGE` | Live fetch truncated / pagination incomplete |
| `HPC_BRAND_ROLLUP_CERTIFIED` | Live + identity + complete pagination + status filter + dedupe + documented coverage + `independentCertification=true` |

Matching HPC rows alone is **not** sufficient for certification.

### Pilot scoring

Only **contract/schema compliance** is scored in staging. Profile field completeness, evidence-based factual accuracy, brand-specific content quality, rendered application validation, and live HPC verification are `NOT_VALIDATED` until proven with live evidence / rendered-app checks.

### Legacy write safety

Apply-capable historical npm scripts are wrapped with `scripts/brand-explorer-legacy-guard.mjs` (fail-closed on `--apply` / write stages). Break-glass: `BRAND_EXPLORER_ALLOW_LEGACY_APPLY=1`. Scripts are not deleted.

## Pilots

See `BE_PHASE2_PILOT_BRANDS` in `be-hpc-footprint-summary-v1.js` (Autograph, Ascend, Curio, Tribute, Mama Shelter, Preferred).

## Commands

```bash
npm run brand-explorer:doctor
npm run brand-explorer:test
npm run test:be-airtable-integration-sot-v1
npm run test:be-hpc-footprint-summary-v1
npm run be-phase2-pilot-validate-v1
# optional read-only live HPC:
npm run be-phase2-pilot-validate-v1 -- --live
```

## Reports

- `reports/brand-explorer/be-phase2-pilot-validation-v1.json`
- `reports/brand-explorer/be-phase2-pilot-validation-v1.md`
