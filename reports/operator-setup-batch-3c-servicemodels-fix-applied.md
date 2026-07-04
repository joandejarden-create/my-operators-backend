# Operator Setup Batch 3C — serviceModelsSupported Fix Applied

Generated: 2026-06-02  
Scope: **Batch 3C serviceModelsSupported only** (approved subset)

## 1. Files changed

- `api/lib/operator-setup-new-base-writer.js`
- `api/lib/operator-setup-new-base-read.js`
- `scripts/validate-operator-setup-batch-3c-servicemodels.mjs`
- `reports/operator-setup-batch-3c-servicemodels-validation-output.json`

## 2. Exact mapping/contract changed

- **Write contract hardening:** when `serviceModelsSupported` is missing but `primaryServiceModel` exists, canonical `serviceModelsSupported` is now populated as a single-item list during new-base write payload construction.
- **Read/prefill contract hardening:** `prefill.serviceModelsSupported` is now explicitly guaranteed from canonical fields (`serviceModelsSupported` / `Service Models Supported`) and falls back to `primaryServiceModel` only when canonical is missing.

## 3. Airtable table/field used

- Table: `Operator Setup - Profile & Positioning`
- Canonical field: `Service Models Supported` (multi-select)
- Compatibility field retained: `primaryServiceModel` (single-select)

## 4. UI key

- Canonical UI/prefill key: `serviceModelsSupported`
- Compatibility UI key retained: `primaryServiceModel`

## 5. Payload key

- Canonical payload key: `serviceModelsSupported`
- Compatibility payload key retained: `primaryServiceModel`

## 6. API/write mapping changed

- In `buildNewBaseTablePayloads`, mapping for form key `serviceModelsSupported` now coalesces from `primaryServiceModel` when canonical is absent.

## 7. Readback/detail key confirmed

- Detail endpoint prefill (`/api/intake/third-party-operators/:recordId`) confirms `prefill.serviceModelsSupported` is present after create and update.
- `meta.readPath` remained `new_base` in validation.

## 8. Explorer canonical resolution confirmed

- Explorer detail payload (`/api/operator-explorer/operator?operatorId=...`) now includes canonical `serviceModelsSupported` for validated records.
- Contract guard confirms explorer fallback stack still includes canonical first.

## 9. Confirmation that primaryServiceModel fallback remains

- Confirmed retained. `primaryServiceModel` is still present and used as compatibility fallback.

## 10. Confirmation that fallback order did not change

- Confirmed unchanged in `public/js/operator-explorer-new-base-profile.js`:
  - `serviceModelsSupported`
  - `Service Models Supported`
  - `primaryServiceModel`

## 11. Confirmation that displayed value did not change

- Output parity validation passed: canonical display value equals fallback display value (`Third-Party Management`) for validated record.

## 12. Confirmation that scoring did not change

- Runtime scoring check could not run due unavailable deal context in this runtime (`deal not found`).
- Guard check passed: no diffs in scoring modules touched by this change set.

## 13. Tests run

- `node scripts/validate-operator-setup-batch-3c-servicemodels.mjs` against isolated runtime on `http://127.0.0.1:8098`
- Included checks:
  - `serviceModels_save_canonical_mode`
  - `serviceModels_reload_prefill_present`
  - `serviceModels_detail_readback_present`
  - `serviceModels_update_canonical_mode`
  - `serviceModels_post_update_prefill_present`
  - `serviceModels_explorer_payload_has_canonical`
  - `serviceModels_output_parity`
  - `serviceModels_fallback_order_unchanged`
  - `scoring_stability_no_change` (fallback guard mode)

## 14. Test results

- Validation output: `ok: true`
- Created record in validation: `recjGbr8A5RkDcLN9`
- Full machine-readable evidence: `reports/operator-setup-batch-3c-servicemodels-validation-output.json`

## 15. Any tests not run and why

- Full runtime score-comparison across a known deal/operator pair was not run because no accessible deal context was found in this validation runtime.
- A static guard was used to confirm no scoring-module changes were introduced.

## 16. Remaining risks

- Existing sparse records may still rely on fallback until canonical field is populated over time.
- Scoring runtime parity should be re-checked in staging with a known valid deal/operator pair when available.

## 17. Recommended commit message

- `batch 3c: close canonical serviceModelsSupported contract gap with focused validation`

