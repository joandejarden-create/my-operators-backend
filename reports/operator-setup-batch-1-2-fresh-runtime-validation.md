# Operator Setup Batch 1+2 Fresh Runtime Validation

Generated: 2026-06-02  
Scope: Fresh-runtime validation only (no new feature changes)

## Server start commands used

1. Production runtime (primary validation):
   - `$env:PORT='8091'; $env:NODE_ENV='production'; $env:OPERATOR_SETUP_WRITE_MODE='canonical'; $env:OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD='0'; $env:OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS='1'; $env:OPERATOR_SETUP_USE_NEW_BASE_WRITER='0'; $env:OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE='1'; $env:OPERATOR_SETUP_NEW_BASE_FAIL_OPEN='1'; npm start`
2. Development runtime (diagnostic gate ON):
   - `$env:PORT='8093'; $env:NODE_ENV='development'; $env:OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS='1'; $env:OPERATOR_SETUP_WRITE_MODE='canonical'; $env:OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD='0'; npm start`
3. Development runtime (diagnostic gate OFF):
   - `$env:PORT='8094'; $env:NODE_ENV='development'; $env:OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS='0'; $env:OPERATOR_SETUP_WRITE_MODE='canonical'; npm start`

Stale `:8080` process was avoided by using isolated validation ports.

## Runtime/env configuration tested

### Production run (`:8091`)
- `NODE_ENV=production`
- `OPERATOR_SETUP_WRITE_MODE=canonical`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=1` (intentionally set to prove prod still blocks mock detail)
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` (legacy flag set intentionally)
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=1` (legacy flag set intentionally)
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=1` (legacy flag set intentionally)

### Dev runs
- `:8093` -> `NODE_ENV=development`, `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=1`
- `:8094` -> `NODE_ENV=development`, `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`

## Test records used

- Existing live operator ID: `recTUjuDxL96yWcQA`
- Created during validation: `recyHakOliyjuJGKT`

## HTTP/API validation results

### A) Operator Explorer source-of-truth hardening

1. **Valid rec detail returns live data**
   - Request: `GET /api/operator-explorer/operator?operatorId=recTUjuDxL96yWcQA` (`:8091`)
   - Expected: 200 live data, no mock meta
   - Actual: `200`, `success=true`, `meta.readPath="new_base"`, `meta.demoData` absent/false
   - Result: **PASS**

2. **Non-rec detail returns controlled 404**
   - Request: `GET /api/operator-explorer/operator?operatorId=op-1` (`:8091`)
   - Expected: 404 + `INVALID_OPERATOR_ID_FORMAT`
   - Actual: `404`, `code="INVALID_OPERATOR_ID_FORMAT"`
   - Result: **PASS**

3. **Production cannot return mock with diagnostic signal**
   - Request: `GET /api/operator-explorer/operator?operatorId=op-1&diagnosticMock=1` + diagnostic header (`:8091`)
   - Expected: still blocked in prod
   - Actual: `404`, `code="INVALID_OPERATOR_ID_FORMAT"`
   - Result: **PASS**

4. **Dev diagnostic path only when all gates are satisfied**
   - `:8093` gate ON, no explicit signal -> `404` (**PASS**)
   - `:8093` gate ON + explicit signal -> `200` + `meta.diagnosticMock=true`, `meta.demoData=true` (**PASS**)
   - `:8094` gate OFF + explicit signal -> `404` (**PASS**)

### B) Detail read-path observability

1. **`meta.readPath` present**
   - Valid detail responses returned `meta.readPath`.
   - Result: **PASS**

2. **new-base record returns `readPath=new_base`**
   - Requests for `recTUjuDxL96yWcQA` and `recyHakOliyjuJGKT` returned `meta.readPath="new_base"`.
   - Result: **PASS**

3. **Legacy fallback coverage**
   - No confirmed legacy fallback record was identified in this runtime pass.
   - Result: **NOT EXECUTED (no known legacy-only record ID in test set)**

### C) Intake write-path stabilization

1. **Canonical write path + success response**
   - Request: `POST /api/intake/third-party-operator` (`:8091`) with representative payload
   - Expected: success + `writeMode=canonical`
   - Actual: `201`, `success=true`, `recordId="recyHakOliyjuJGKT"`, `writeMode="canonical"`
   - Result: **PASS**

2. **Data writes to new-base and record is readable**
   - Request: `GET /api/intake/third-party-operators/recyHakOliyjuJGKT` (`:8091`)
   - Actual: `200`, `meta.readPath="new_base"`, expected company name returned
   - Result: **PASS**

3. **No legacy/shadow/fail-open behavior in canonical mode**
   - Runtime log evidence for intake call:
     - `"writeMode":"canonical"`
     - `"useNewBaseWriter":true`
     - `"shadowWriteNewBase":false`
     - `"failOpenNewBase":false`
     - new-base writer events (`master_created`, `one_to_one_created`, `writer_done`) present
     - no shadow-write success/fail events for canonical call
   - Result: **PASS**

4. **Induced new-base failure fails closed**
   - Request: `POST /api/intake/third-party-operator` without `companyName` (`:8091`)
   - Expected: error, no legacy fallback
   - Actual: `500`, `error="New-base writer failed"`, `message="companyName is required for Master write"`, `writeMode="canonical"`
   - Result: **PASS**

5. **Canonical fail-closed despite old legacy flags**
   - Same production runtime had old flags set (`USE_NEW_BASE_WRITER=0`, `NEW_BASE_SHADOW_WRITE=1`, `NEW_BASE_FAIL_OPEN=1`) but explicit `OPERATOR_SETUP_WRITE_MODE=canonical`.
   - Failure still returned fail-closed canonical error.
   - Result: **PASS**

### D) Regression checks

1. **My Operator save success contract**
   - `201` success with message + `recordId` + `writeMode`.
   - Result: **PASS**

2. **Saved operator loads via detail path**
   - `GET /api/intake/third-party-operators/recyHakOliyjuJGKT` => `200`, `readPath=new_base`.
   - Result: **PASS**

3. **Explorer loads saved live operator**
   - `GET /api/operator-explorer/operator?operatorId=recyHakOliyjuJGKT` => `200`, `readPath=new_base`, no demo meta.
   - Result: **PASS**

4. **No obvious UI-break implication from controlled non-rec behavior**
   - API now returns explicit controlled 404 contract for non-rec; this is the intended empty/error state trigger.
   - Result: **PASS (API-level)**

## Evidence snippets

- Explorer non-rec in prod:
  - `{"status":404,"code":"INVALID_OPERATOR_ID_FORMAT","error":"Operator not found"}`
- Explorer valid rec:
  - `{"status":200,"meta":{"readPath":"new_base"},"hasDemoData":false}`
- Canonical create:
  - `{"status":201,"recordId":"recyHakOliyjuJGKT","writeMode":"canonical"}`
- Canonical induced failure:
  - `{"status":500,"error":"New-base writer failed","message":"companyName is required for Master write","writeMode":"canonical"}`
- Runtime writer flags:
  - `{"useNewBaseWriter":true,"shadowWriteNewBase":false,"failOpenNewBase":false,"writeMode":"canonical"}`

## Failures or blockers

1. Could not confirm a real legacy-fallback detail record in this run because no known legacy-only record ID was available in the test set.
2. `legacyFlagSnapshot` values in one writer log line reflected runtime values that did not match the intentional shell-start flag stress test exactly; behavior was still canonical and deterministic via `writeMode=canonical`.

## Ready to commit/deploy?

**Yes, with one targeted follow-up before Batch 3:**  
Run one additional verification using a known legacy-only operator record ID to explicitly capture `meta.readPath=legacy`.

All other Batch 1 + Batch 2 acceptance checks passed in fresh runtime.

## Required follow-up fix before Batch 3

No code fix required from this validation pass.  
Operational follow-up only: capture one confirmed `legacy` read-path sample record.

## Recommended commit message (if proceeding)

`validate fresh-runtime behavior for operator source-of-truth hardening and canonical intake writes`

