# Operator Setup Batch 3B Contract Hardening Applied

Generated: 2026-06-02  
Scope: Smallest safe 3B slice only (diagnostics + provenance tests + canonical docs)

## 1) Files changed

- `public/js/operator-explorer-new-base-profile.js`
- `lib/operator-alignment-prefill-map.js`
- `api/lib/operator-setup-new-base-read.js`
- `public/js/operator-setup-explorer-behavior.js`
- `scripts/validate-operator-setup-batch-3b-contracts.mjs`
- `reports/operator-setup-batch-3b-contract-validation-output.json`

## 2) Diagnostics added

### Explorer read-key fallback diagnostics
- File: `public/js/operator-explorer-new-base-profile.js`
- Added debug-only diagnostics inside `pickField` / `pickList` for:
  - concept/target field
  - canonical key expected
  - key actually used
  - source layer used (`ex`, `prefill`, `fields`)
  - whether fallback was used
  - unresolved state
- Added concise canonical-first comments near critical fallback stacks:
  - company name family
  - service models
  - chain scales
  - geography families

### Alignment alias resolver diagnostics
- File: `lib/operator-alignment-prefill-map.js`
- Added debug-only diagnostics to `applyOperatorAlignmentPrefillAliases()` for:
  - canonical key resolution short-circuit
  - alias key chosen when canonical missing
  - unresolved keys
- No scoring math/threshold behavior changes.

### Leadership mapper provenance diagnostics
- File: `api/lib/operator-setup-new-base-read.js`
- Added debug-only provenance diagnostics in `mapNewBaseLeadershipForDetail()`:
  - role -> function
  - summary -> summary / shortBio / experienceSummary
  - bio -> bio
  - headshot -> headshotUrl
- Output shape unchanged.

### explorerProfileJson mirror masking diagnostics
- File: `public/js/operator-setup-explorer-behavior.js`
- Added debug-only diagnostics:
  - `mirror_prefill_applied`: counts where mirror filled empty structured controls
  - `mirror_write_contract`: structured key count vs mirror key count on submit
- Mirror behavior retained unchanged.

## 3) Tests added

- `scripts/validate-operator-setup-batch-3b-contracts.mjs`
  - Validates canonical-first key-family coverage and diagnostics presence.
  - Validates alignment alias resolution behavior (canonical preserved, alias fallback works).
  - Validates leadership child mapping contract shape.
  - Validates mirror masking diagnostics presence.
  - Writes output to:
    - `reports/operator-setup-batch-3b-contract-validation-output.json`

## 4) Canonical key families covered

- `companyName / company_name / Company Name`
- `serviceModelsSupported` and aliases
- `chainScalesSupported` and aliases
- `activeCountries / activeMarkets / regions / specificMarkets` and aliases
- leadership child mappings (`role`, `summary`, `bio`, `headshot` -> detail compatibility keys)
- `explorerProfileJson` mirror masking checks
- alignment prefill alias resolver provenance

## 5) Confirmation: no user-facing behavior changed

Confirmed. All additions are diagnostics/test/documentation only and gated.

## 6) Confirmation: scoring did not change

Confirmed. No scoring formulas, weights, thresholds, or recommendation logic were modified.

## 7) Confirmation: fallback order did not change

Confirmed. Existing fallback stacks and ordering were preserved.

## 8) How to enable/view diagnostics

### Browser-side diagnostics
- Set either:
  - `window.__OPERATOR_SETUP_CONTRACT_DIAGNOSTICS = true`
  - or `localStorage.setItem("operator_setup_contract_diagnostics", "1")`
- Open browser console and look for:
  - `[operator_setup_contract_diag]` JSON payloads

### Server-side diagnostics
- Set env var:
  - `OPERATOR_SETUP_CONTRACT_DIAGNOSTICS=1`
- Check server logs for:
  - `[operator_setup_contract_diag]` JSON payloads

## 9) Test commands run

- `node --check public/js/operator-explorer-new-base-profile.js`
- `node --check lib/operator-alignment-prefill-map.js`
- `node --check api/lib/operator-setup-new-base-read.js`
- `node --check public/js/operator-setup-explorer-behavior.js`
- `node --check scripts/validate-operator-setup-batch-3b-contracts.mjs`
- `node scripts/validate-operator-setup-batch-3b-contracts.mjs`

## 10) Test results

- Syntax checks: pass
- Contract/provenance validation script: pass (`ok: true`, 9 checks passed)

## 11) Tests not run and why

- No browser E2E harness was run.
  - Reason: scope is non-user-facing diagnostics and contract checks; script + syntax coverage was sufficient for this slice.

## 12) Remaining risks

1. Diagnostics could be noisy if enabled broadly in high-traffic environments.
2. Provenance visibility now exists, but production alerting/aggregation for these diagnostics is not yet wired.
3. Alias contracts remain in place by design; this slice does not enforce canonical-only behavior.

## 13) Recommended commit message

`batch 3b: add non-user-facing key provenance diagnostics and contract tests`

