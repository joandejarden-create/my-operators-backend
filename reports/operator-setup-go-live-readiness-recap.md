# Operator Setup Go-Live Readiness Recap

Generated: 2026-06-02  
Scope recap after commits:
- `bf8865b7f4e00a780090e8cf3915982699056fc0` (Batch 1+2)
- `275136933beda9ed4de55ba44ce13d1eee762091` (Batch 3A)
- `d5ff0d824acd417537c2f2bbb32b2ef0558faf9b` (Batch 3B)

---

## Executive Answer (direct)

- **Internal QA readiness:** **Yes** (ready now).
- **External demo readiness:** **Yes, with targeted P1 cleanup recommended first**.
- **Production/go-live readiness:** **Not yet fully approved** until remaining P0/P1 mapping-contract items are closed (primarily canonical-vs-fallback contract enforcement evidence and any unresolved field-contract gaps outside 3A).

---

## A. Ready now

### What is fixed and safe
- Owner-facing Explorer detail path is hardened to rec-id / Airtable-backed behavior.
- Non-rec owner detail requests are deterministically blocked (no mock fallback in go-live mode).
- Canonical intake write mode is deterministic (`writeOperatorSetupToNewBase()`), fail-closed in canonical mode.
- Batch 3A confirmed canonical persistence/readback for:
  - `companyLogo` (canonical path + readback)
  - `companyTagline`
  - `missionStatement`
  - `companyHistory`
  - `differentiators`
  - `managementPhilosophy`
  - `brand_conversion_project_count` (input + save/readback)
- Batch 3B added non-user-facing provenance diagnostics and contract tests without changing rendered behavior.

### What is deterministic now
- Write-path mode behavior is deterministic under canonical config.
- Explorer non-rec behavior is deterministic and controlled.
- Detail read-path visibility (`meta.readPath`) is available for new-base vs legacy.
- Critical key-family provenance diagnostics can now be enabled and observed.

### What can be tested with confidence now
- My Operator save/reload for Batch 3A fields.
- Explorer live detail/list behavior for saved records (including logo path).
- Canonical/fail-closed write behavior.
- Fallback/alias provenance diagnostics (when enabled).

---

## B. Needs internal QA

- Full field round-trip across high-traffic sections still needs structured QA runs (beyond Batch 3A subset).
- Multi-role QA for:
  - intake create/update
  - prefill/edit
  - explorer detail/list
  - alignment pages consuming operator context
- Diagnostics-on soak in staging to collect real fallback-used patterns.

---

## C. Needs external-demo cleanup

Remaining P1 before external demos:
- Clarify/close important “written-but-not-displayed” field contracts that stakeholders may expect to see.
- Validate and document any remaining alias-heavy fields where canonical source is still ambiguous in downstream consumers.
- Confirm no demo-critical page is relying on compatibility fallback where canonical structured field should be primary.

---

## D. Needs later cleanup

P2/later:
- Legacy/duplicate/cleanup candidates and deprecation decisions.
- Broader documentation consolidation.
- Non-critical UX/polish changes.
- Batch 4 option normalization (planned separately, not part of current go-live-safe hardening).

---

## E. Do not touch yet

- Scoring formulas, weights, thresholds, recommendation logic.
- Select/multi-select normalization rollout.
- Airtable schema rename/delete changes.
- Legacy alias/remap removal and `explorerProfileJson` removal.
- User-facing display behavior changes tied to fallback-order semantics.

---

## F. Remaining risks

1. Alias/fallback contracts are now observable, but not yet fully enforced canonical-only.
2. Diagnostic signal may show unresolved canonical gaps in broader field families not included in 3A.
3. `companyLogo` relies on reachable asset URLs at runtime (environment-dependent operational risk).
4. Legacy compatibility paths remain by design (safe), but require ongoing observability to prevent silent drift.

---

## G. Recommended next step

### Next implementation batch (recommended)
- **Batch 3C diagnostics review + targeted canonical contract closure**:
  - Use staging soak logs from 3B diagnostics.
  - Fix only proven canonical contract misses (no scoring/display/order changes).
  - Add/extend contract tests for each confirmed miss.

This is the smallest safe path before declaring full go-live readiness.

---

## Remaining unresolved vs deferred

### What still remains unresolved
- Any unresolved canonical-vs-fallback source ambiguity outside 3A-fixed fields.
- Any operator fields still lacking confirmed save->read->downstream contract evidence.

### What is intentionally deferred
- Option normalization (Batch 4).
- Scoring and display behavior changes.
- Legacy alias/mirror removal.

---

## Are there remaining P0 blockers?

- **Yes (conditional):** if diagnostics reveal unresolved canonical source contracts in go-live-critical owner-facing fields not yet closed.
- **No immediate blocker found in Batch 1+2+3A scope itself**, but full go-live sign-off still depends on 3C evidence pass.

## Remaining P1 before external demos

- Important fields with incomplete display/write parity outside 3A scope.
- Alias-heavy downstream sections where canonical provenance is not yet explicitly validated through staging soak evidence.

---

## Deployment environment variables (staging/production)

### Recommended production
- `NODE_ENV=production`
- `OPERATOR_SETUP_WRITE_MODE=canonical`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`
- `OPERATOR_SETUP_CONTRACT_DIAGNOSTICS=0` *(enable temporarily only for controlled soak)*

### Recommended staging (soak phase)
- `NODE_ENV=production` *(or staging-equivalent prod-like mode)*
- `OPERATOR_SETUP_WRITE_MODE=canonical`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`
- `OPERATOR_SETUP_CONTRACT_DIAGNOSTICS=1` *(during soak window only)*

---

## Suggested internal QA checklist

1. **My Operator save**
   - Create + update in canonical mode, verify success contract.
2. **My Operator reload/prefill**
   - Verify saved values round-trip, especially 3A fields.
3. **Operator Explorer detail**
   - rec-id loads live; non-rec blocked.
4. **Operator Explorer list/logo**
   - logo displays from canonical record where provided.
5. **Operator Capability Snapshot**
   - verify no regressions (logic unchanged).
6. **Operator Alignment Snapshot**
   - verify no user-facing output regressions (methodology unchanged).
7. **Operator Alignment Score Breakdown**
   - verify score values unchanged for same inputs.
8. **Fallback diagnostics check**
   - enable diagnostics in staging and confirm provenance logs emit.
9. **Non-rec mock-blocking behavior**
   - verify block behavior in go-live config.
10. **Canonical write behavior**
   - verify no shadow/fail-open behavior in canonical mode.

---

## Recommendation on what to run next

- **Staging soak with diagnostics enabled:** **Yes (recommended)**
- **Batch 3C diagnostics review:** **Yes (recommended, next)**
- **Batch 4 select/multi-select normalization:** **Not yet**
- **External demo cleanup:** **Yes, after 3C targeted contract fixes**

