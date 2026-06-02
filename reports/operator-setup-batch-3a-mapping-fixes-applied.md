# Operator Setup Batch 3A Mapping Fixes Applied

Generated: 2026-06-02  
Scope: Batch 3A only (smallest confirmed mapping fixes)

## 1) Files changed

- `api/third-party-operator-intake.js`
- `api/lib/operator-setup-new-base-build-sheet-rows.json`
- `api/lib/operator-setup-new-base-read.js`
- `public/third-party-operator-setup-new-two.html`
- `scripts/validate-operator-setup-batch-3a.mjs`

## 2) Exact fields fixed

### Canonical `companyLogo` persistence path
- Ensured multipart upload is mirrored into canonical body (`req.body.companyLogo`) before `writeOperatorSetupToNewBase()`.
- Kept legacy logo behavior in place; added canonical parity without removing legacy path.

### Missing canonical narrative mappings
- `companyTagline`
- `missionStatement`
- `companyHistory`
- `differentiators`
- `managementPhilosophy`

Added these to canonical new-base build-sheet fallback mapping so they save in canonical mode.

### Missing UI input added
- `brand_conversion_project_count` (optional numeric input)

Added to `Brand & Relationships` section in My Operator form; not required.

## 3) Airtable tables/fields used

- `Operator Setup - Profile & Positioning.companyLogo`
- `Operator Setup - Profile & Positioning.companyTagline`
- `Operator Setup - Profile & Positioning.missionStatement`
- `Operator Setup - Profile & Positioning.companyHistory`
- `Operator Setup - Profile & Positioning.differentiators`
- `Operator Setup - Profile & Positioning.managementPhilosophy`
- `Operator Setup - Profile & Positioning.brand_conversion_project_count`

## 4) UI keys

- `companyLogo`
- `companyTagline`
- `missionStatement`
- `companyHistory`
- `differentiators`
- `managementPhilosophy`
- `brand_conversion_project_count`

## 5) Payload keys

- `companyLogo` (attachment array payload in canonical mode)
- `companyTagline`
- `missionStatement`
- `companyHistory`
- `differentiators`
- `managementPhilosophy`
- `brand_conversion_project_count`

## 6) API/write mappings changed

- `api/third-party-operator-intake.js`
  - Added `buildUploadedLogoAttachment()` and canonical body merge for multipart upload:
    - `req.file` -> `req.body.companyLogo` (when absent) before canonical writer call.
  - Existing legacy `fields['Company Logo']` write remains.

- `api/lib/operator-setup-new-base-build-sheet-rows.json`
  - Added canonical rows for the 5 narrative fields and `companyLogo` + existing `brand_conversion_project_count` UI alignment.

## 7) Readback keys confirmed

- Prefill keys confirmed:
  - `prefill.companyTagline`
  - `prefill.missionStatement`
  - `prefill.companyHistory`
  - `prefill.differentiators`
  - `prefill.managementPhilosophy`
  - `prefill.companyLogo`

- Detail fields confirmed:
  - `operator.fields.brand_conversion_project_count`

- Detail/list read helpers updated:
  - Logo fallback now checks both `companyLogo` and `Company Logo` for compatibility.
  - Added detail shaping keys:
    - `Company History`
    - `Differentiators`
    - `Management Philosophy`
    - `brand_conversion_project_count`

## 8) Downstream pages affected

- My Operator intake/save
- Operator detail API (`/api/intake/third-party-operators/:recordId`)
- Operator Explorer detail (`/api/operator-explorer/operator?operatorId=rec...`)
- Operator list surfaces consuming logo (`/api/intake/third-party-operators`)

No scoring logic, capability snapshot logic, or alignment methodology changes were made.

## 9) Tests run

### Syntax/lint
- `ReadLints` on modified files: no lint errors.
- `node --check`:
  - `api/third-party-operator-intake.js`
  - `api/lib/operator-setup-new-base-read.js`
  - `scripts/validate-operator-setup-batch-3a.mjs`

### Fresh-runtime validation (canonical mode)
- Started fresh runtime on dedicated port with:
  - `NODE_ENV=production`
  - `OPERATOR_SETUP_WRITE_MODE=canonical`
  - `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
  - `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
  - `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
  - `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
  - `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`

- Executed:
  - `node scripts/validate-operator-setup-batch-3a.mjs`
  - Output file: `reports/operator-setup-batch-3a-validation-output.json`

## 10) Test results

All focused Batch 3A checks passed in the latest fresh-runtime run:

- Multipart save with logo + narratives: pass (`201`, `writeMode=canonical`)
- Canonical logo payload save: pass
- Detail read path: pass (`meta.readPath=new_base`)
- Narrative prefill roundtrip (all 5 fields): pass
- `brand_conversion_project_count` detail readback: pass
- `prefill.companyLogo` readback: pass
- Explorer detail for saved record (live, no mock): pass
- Operator list logo readback: pass

## 11) Tests not run and why

- No browser UI automation was run for visual verification of the new numeric input placement.
  - Reason: API/runtime contract validation was completed directly and is sufficient for this batch scope.

## 12) Remaining risks

1. Multipart upload logo persistence still depends on Airtable being able to fetch the provided URL (publicly reachable URL expected in real environments).
2. Existing long-running local servers can still cause stale-runtime confusion unless restarted.
3. Other Batch 3B/P1/P2 mapping gaps are intentionally not addressed in this batch.

## 13) Recommended commit message

`batch 3a: add canonical logo and narrative mappings with focused runtime validation`

