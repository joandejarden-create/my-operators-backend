# Operator Setup Batch 1 + 2 PR Plan (Proposal Only)

Generated: 2026-06-02  
Scope: **Batch 1 + Batch 2 only**  
Status: **Plan only — no implementation**

---

## Objective

Make Operator Setup deterministic for go-live by:

1. Hardening owner-facing Operator Explorer detail endpoints to source-of-truth behavior.
2. Stabilizing intake writes to canonical new-base flow.

This plan intentionally excludes:
- Airtable schema edits
- field renames/deletions
- scoring methodology changes
- select-option normalization implementation

---

## Evidence model used

- **Current code evidence:** `api/operator-explorer.js`, `api/third-party-operator-intake.js`, `api/third-party-operator-detail.js`, `api/lib/operator-setup-new-base-{read,writer}.js`
- **Current Airtable evidence:** live meta/schema available in Checkpoint A/B generation
- **Prior reports:** reference-only
- **Conflict/Needs Review flags:** included per change item

---

## Batch 1 — Owner-facing source-of-truth hardening

### Change B1-1
- **File path:** `api/operator-explorer.js`
- **Current behavior:** `getOperatorById()` allows non-`rec...` IDs and can return `MOCK_OPERATORS` (and enriched hardcoded detail) when `OPERATOR_EXPLORER_ALLOW_MOCKS` is enabled.
- **Proposed behavior:** owner-facing detail route must resolve only `rec...` IDs via `getThirdPartyOperatorDetail`; non-`rec` returns explicit 404/validation message, no mock payload.
- **Exact function/block:** `getOperatorById()`, blocks around:
  - `allowMocks` flag check
  - `MOCK_OPERATORS.find(...)`
  - hardcoded `detail` construction
- **Risk level:** High (if unchanged), Low-Medium (to change)
- **Impact on My Operator:** None direct.
- **Impact on Operator Explorer:** High positive; deterministic source-of-truth.
- **Impact on Operator Capability Snapshot:** None direct.
- **Impact on Alignment Snapshot:** None direct.
- **Impact on Alignment Score Breakdown:** None direct.
- **Test required:**
  - `GET /api/operator-explorer/operator?operatorId=rec...` returns live source data.
  - `GET ...?operatorId=op-1` returns controlled error (no mock payload).
  - Verify UI empty/error state rendering path.
- **Rollback plan:** restore prior branch/commit; temporary env-gated mock path can be re-enabled in non-prod only.

### Change B1-2
- **File path:** `api/operator-explorer.js` (+ optional route/middleware wrapper if present in server router)
- **Current behavior:** fallback behavior is primarily runtime flag-based and can be accidentally enabled.
- **Proposed behavior:** diagnostic fallback, if retained, must be explicitly dev-only and impossible in go-live mode (e.g., require `NODE_ENV !== "production"` and a separate diagnostic flag).
- **Exact function/block:** `getOperatorById()` flag evaluation block.
- **Risk level:** Medium
- **Impact on My Operator:** None.
- **Impact on Operator Explorer:** High positive (prevents stale/mock leakage).
- **Impact on snapshots/score:** None.
- **Test required:** verify in prod-mode config that mocks never return; in dev-mode config diagnostics still usable if intentionally enabled.
- **Rollback plan:** temporarily restore old flag behavior behind guarded branch.

### Change B1-3
- **File path:** `api/third-party-operator-detail.js` (observability only; no behavioral removal yet)
- **Current behavior:** new-base primary with legacy fallback path.
- **Proposed behavior:** keep behavior for now, but add/confirm explicit read-path logging/response metadata for observability (`read_path: new_base|legacy`) to detect non-canonical reads during go-live readiness.
- **Exact function/block:** branch at `if (bundle && bundle.master) ... else legacy`.
- **Risk level:** Low
- **Impact on My Operator:** None.
- **Impact on Explorer:** Medium positive (debuggability).
- **Impact on snapshots/score:** Indirect positive.
- **Test required:** call detail endpoint for known rec IDs and confirm telemetry/read path.
- **Rollback plan:** remove added logging only.

---

## Batch 2 — Intake write-path stabilization

### Change B2-1
- **File path:** `api/third-party-operator-intake.js`
- **Current behavior:** write path determined by three flags:
  - `OPERATOR_SETUP_USE_NEW_BASE_WRITER`
  - `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE`
  - `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN`
  This allows multiple operational combinations and drift risk.
- **Proposed behavior:** introduce one deterministic go-live mode:
  - canonical write = `writeOperatorSetupToNewBase()`
  - legacy writes disabled in go-live mode
  - shadow writes allowed only in diagnostic mode
- **Exact function/block:** `submitThirdPartyOperator()` around:
  - flag reads
  - `if (useNewBaseWriter) ...`
  - full legacy write branch below
  - shadow write block near end
- **Risk level:** High (if done incorrectly), but critical
- **Impact on My Operator:** High; save determinism improves.
- **Impact on Explorer:** High positive downstream consistency.
- **Impact on Capability Snapshot:** Medium positive (consistent underlying data).
- **Impact on Alignment Snapshot:** Medium positive.
- **Impact on Alignment Score Breakdown:** Medium positive (less operator-data drift).
- **Test required:**
  - submit create and update flows with representative payload
  - verify writes in Operator Setup new-base tables only (for go-live mode)
  - verify no writes to legacy tables in go-live mode
- **Rollback plan:** restore prior flag branching quickly by reverting commit.

### Change B2-2
- **File path:** `api/third-party-operator-intake.js`
- **Current behavior:** `failOpenNewBase` can allow fallback to legacy path after new-base write failure.
- **Proposed behavior:** in go-live mode, disable fail-open fallback (fail closed with clear error + logging); allow fail-open only in diagnostic environments.
- **Exact function/block:** catch block under `new_writer_failed_primary`.
- **Risk level:** Medium
- **Impact on My Operator:** Users receive explicit failures instead of silent split-write divergence.
- **Impact on Explorer/snapshots/score:** Positive consistency.
- **Test required:** force new-base write failure and confirm expected error contract.
- **Rollback plan:** re-enable fail-open in emergency via controlled env var in non-prod.

### Change B2-3
- **File path:** `api/third-party-operator-intake.js` + config docs
- **Current behavior:** flags are independent and can be combined ambiguously.
- **Proposed behavior:** replace with explicit mode enum (proposal):
  - `OPERATOR_SETUP_WRITE_MODE=canonical|diagnostic-shadow|legacy-maintenance`
  - go-live expected: `canonical`
- **Exact function/block:** top flag parsing and path selection.
- **Risk level:** Medium
- **Impact:** clarity, reduced operational mistakes.
- **Test required:** mode matrix test.
- **Rollback plan:** map enum back to old flags temporarily.

---

## Pre-change verification checklist

1. Confirm current behavior in staging:
   - `api/operator-explorer.js` mock path reproducible with non-rec id.
   - intake can currently execute both new-base and legacy paths depending on flags.
2. Capture baseline sample operator IDs and deal IDs for regression.
3. Confirm `writeOperatorSetupToNewBase()` success path for create+update.
4. Confirm `loadNewBaseOperatorBundle()` returns expected bundle for same records.
5. Snapshot current environment variables from runtime.
6. Confirm API consumers handle 404/empty states for invalid operator IDs.
7. Confirm logs currently capture `read_path`/writer events.

---

## Environment/feature flags to confirm before implementation

Current flags in play:
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN`
- `OPERATOR_EXPLORER_ALLOW_MOCKS`

Recommended go-live target policy:
- `OPERATOR_EXPLORER_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`

Recommended diagnostic/dev policy:
- mock only allowed when `NODE_ENV !== "production"` and explicit diagnostic flag set
- optional shadow writes only in diagnostic mode

---

## Expected behavior after Batch 1 + Batch 2

### Go-live behavior
- Operator Explorer detail accepts only `rec...` source-of-truth IDs.
- No MOCK/fallback operator payloads returned to owner-facing clients.
- My Operator writes are canonical to new-base only.
- New-base write failures return explicit API errors (no hidden legacy fallback).
- Read/detail remains new-base first; any legacy fallback usage is measurable and visible.

### Dev/diagnostic behavior
- Optional mock behavior can exist only behind strict dev-only gating.
- Optional shadow writes available only in explicit diagnostic mode.
- Clear mode telemetry in logs.

---

## Known places currently allowing non-source-of-truth behavior

1. `api/operator-explorer.js`
   - `MOCK_OPERATORS` array and non-rec fallback path.
2. `api/third-party-operator-intake.js`
   - dual-path/new-base+legacy+shadow behavior via flags.
3. `api/third-party-operator-detail.js`
   - legacy fallback remains active when new-base bundle not found (intentional for migration; should be observed closely).

---

## Known places currently allowing drift (dual writes/shadow writes)

1. `api/third-party-operator-intake.js`
   - primary new-base write + optional legacy fallback + optional shadow write blocks.
2. `api/third-party-operator-intake.js`
   - legacy table upserts still active when new-base not used or fail-open is allowed.

---

## Test plan (surgical)

### Batch 1 tests
1. Explorer detail with valid rec id -> 200 and live payload.
2. Explorer detail with non-rec id -> 404/no mock payload.
3. Production-mode config ensures mock path unreachable.
4. UI shows clear empty/error state for missing live operator.

### Batch 2 tests
1. Create operator via My Operator -> new-base tables updated.
2. Update operator via My Operator -> same master id updated in new-base.
3. Induced new-base failure -> explicit error (no legacy write in go-live mode).
4. Verify no legacy-table drift writes in canonical mode.
5. Verify detail read path aligns with new-base for newly saved records.

---

## Rollback path

1. Revert PR commit(s) (single revert should restore previous behavior).
2. Restore previous env flags if emergency rollback needed.
3. Validate:
   - intake save endpoint returns to previous behavior
   - explorer endpoint behavior matches prior baseline
4. Preserve logs around rollback window for postmortem.

---

## Recommended commit message

`harden operator source-of-truth paths for go-live`

Alternative (two commits):
1. `disable owner-facing operator explorer mock fallback in go-live mode`
2. `stabilize operator intake to canonical new-base write mode`

---

## Do not touch yet

1. Airtable schema (field create/rename/delete) for Operator Setup tables.
2. Scoring methodology/weights/threshold logic.
3. Select-option normalization implementation (only planning now).
4. Legacy alias/remap code removal (until migration validation confirms safe).
5. Capability/Alignment snapshot model changes unrelated to source determinism.

---

## Implementation sequencing recommendation

1. Implement Batch 1 first (Explorer hardening) — low blast radius, immediate trust gain.
2. Implement Batch 2 second (intake mode stabilization) — higher operational impact, requires stricter QA.
3. Run full Stage 8 E2E checklist after both.

