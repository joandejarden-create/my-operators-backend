# Operator Setup Batch 1 + 2 — Fixes Applied

Generated: 2026-06-02  
Status: Implemented (Batch 1 + Batch 2 only)

## 1) Files changed

- `api/operator-explorer.js`
- `api/third-party-operator-detail.js`
- `api/third-party-operator-intake.js`

## 2) Exact behavior changed

### Batch 1 — Owner-facing source-of-truth hardening

- `api/operator-explorer.js`
  - `getOperatorById()` now enforces rec-id format for normal owner-facing detail lookups.
  - Non-rec IDs now return controlled 404 with `code: "INVALID_OPERATOR_ID_FORMAT"`.
  - Mock data is no longer returned in normal owner-facing mode.
  - Diagnostic mock path is now explicitly gated:
    - only non-production runtime
    - `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=1`
    - explicit request signal (`diagnosticMock=1` query or `x-operator-explorer-diagnostic-mock: 1` header)
  - Diagnostic mock use now emits structured warning log (`diagnostic_mock_path_used`).

- `api/third-party-operator-detail.js`
  - Response now includes `meta.readPath`:
    - `new_base` when loading from `loadNewBaseOperatorBundle()`
    - `legacy` when falling back to Basics legacy read path
  - Existing read-path logging (`logOperatorReadPath`) remains intact.
  - Legacy fallback behavior remains in place (as requested).

### Batch 2 — Intake write-path stabilization

- `api/third-party-operator-intake.js`
  - Added deterministic write-mode resolver: `resolveOperatorSetupWriteMode()`.
  - New explicit mode env:
    - `OPERATOR_SETUP_WRITE_MODE=canonical|diagnostic-shadow|legacy-maintenance`
  - Production safety guard:
    - non-canonical modes are blocked in production unless `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=1`.
  - Canonical mode now maps to:
    - `useNewBaseWriter = true`
    - `shadowWriteNewBase = false`
    - `failOpenNewBase = false`
  - Canonical mode always uses `writeOperatorSetupToNewBase()` and fails closed on error (no silent fallback).
  - Added structured observability in `writer_flags` log:
    - `writeMode`
    - `writeModeSource`
    - `legacyFlagSnapshot`
  - API responses now include `writeMode` for traceability.

## 3) Environment/write mode behavior after change

- Resolution order:
  1. If `OPERATOR_SETUP_WRITE_MODE` is set to allowed value, use it.
  2. Else derive from legacy flags (backward compatibility).
  3. In production, force `canonical` unless `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=1`.

- Effective behavior:
  - `canonical`: new-base writer only; fail closed.
  - `diagnostic-shadow`: legacy primary + diagnostic shadow write path (non-prod recommended).
  - `legacy-maintenance`: legacy write path (non-prod recommended).

## 4) Flags added, removed, or reinterpreted

- **Added**
  - `OPERATOR_SETUP_WRITE_MODE`
  - `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD`
  - `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS`

- **Reinterpreted (compatibility layer)**
  - `OPERATOR_SETUP_USE_NEW_BASE_WRITER`
  - `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE`
  - `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN`
  - These still inform fallback mode when enum is not set, but write behavior is now governed by resolved `writeMode`.

- **No removals**
  - No existing env var was deleted.

## 5) How go-live mode behaves

Recommended go-live:
- `NODE_ENV=production`
- `OPERATOR_SETUP_WRITE_MODE=canonical` (or rely on production safety override)
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`

Result:
- Owner-facing explorer details require rec IDs and do not return mock/fallback payloads.
- Intake writes are canonical new-base writes only.
- New-base write failure returns explicit 500 API error (`New-base writer failed`).
- Legacy/shadow/fail-open behavior is not active in canonical go-live mode.

## 6) How dev/diagnostic mode behaves

- Diagnostic mock detail behavior can be enabled only with all required gates:
  - non-production runtime
  - `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=1`
  - explicit diagnostic request signal
- Intake can run non-canonical modes for diagnostics when intentionally configured.

## 7) Tests run

1. **Node syntax checks**
   - `node --check api/operator-explorer.js`
   - `node --check api/third-party-operator-detail.js`
   - `node --check api/third-party-operator-intake.js`
   - Result: pass.

2. **Direct handler behavior checks (Node script)**
   - non-rec explorer request in production with diagnostic request set -> blocked (404, invalid format code)
   - non-rec explorer request in development with diagnostic gates set -> diagnostic mock returns 200
   - intake canonical fail-closed test with legacy flags set and invalid Airtable creds -> returned `500 New-base writer failed`, `writeMode=canonical`
   - Result: pass for requested hardening expectations.

3. **Static observability verification**
   - confirmed `meta.readPath` response values exist in detail handler for both `new_base` and `legacy`.

4. **Live endpoint probe against currently running local server instance**
   - attempted against `:8080`, but process appears to be an already-running instance with stale code snapshot (did not include newly changed hint text).
   - Result: not used as proof for new behavior.

## 8) Test results

- **Passed**
  - non-rec no-mock behavior in production path (direct handler)
  - diagnostic-only mock behavior in dev path (direct handler)
  - canonical mode fail-closed behavior despite legacy flag values
  - syntax checks
  - read-path observability fields present in code

- **Partially validated**
  - live rec-id detail success on newly edited runtime not fully validated in this session due inability to restart bound local process on same port without disrupting existing runtime.

## 9) Tests not run and why

- Full live HTTP validation on the updated code path for:
  - valid rec detail returning live Airtable data with `meta.readPath`
  - canonical create/update write persisting to Airtable
- Reason:
  - active local port already bound by existing process, and the existing process appears to be running pre-change code; restarting/interrupting user runtime was avoided.
  - additionally, direct-process test environment in this session did not have trusted Airtable credentials for end-to-end write assertions.

## 10) Remaining risks

1. Production deployment must set/confirm env vars exactly; misconfiguration can still alter behavior.
2. If `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=1` is enabled, production can intentionally bypass canonical safety.
3. Legacy fallback in detail route remains by design; if new-base coverage is incomplete, some records may still resolve via legacy path (now observable).
4. Existing long-lived local server processes may continue serving stale code until restart.

## 11) Rollback steps

1. Revert the three API files:
   - `api/operator-explorer.js`
   - `api/third-party-operator-detail.js`
   - `api/third-party-operator-intake.js`
2. Remove/ignore new env vars:
   - `OPERATOR_SETUP_WRITE_MODE`
   - `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD`
   - `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS`
3. Restore previous legacy flag behavior as needed.
4. Restart server runtime to clear stale code processes.

## 12) Recommended commit message

`harden operator explorer source-of-truth and stabilize canonical intake writes`

