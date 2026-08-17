# Operator Setup Staging Diagnostics Soak Plan

Generated: 2026-06-02  
Purpose: Use Batch 3B diagnostics in staging to identify real fallback-used patterns and convert validated findings into targeted Batch 3C canonical contract closures (without changing user-facing behavior, scoring, schema, or fallback ordering during soak).

---

## 1) Exact staging environment variables

Use exactly:

- `NODE_ENV=production`
- `OPERATOR_SETUP_WRITE_MODE=canonical`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`
- `OPERATOR_SETUP_CONTRACT_DIAGNOSTICS=1`

Operational notes:
- Treat these as soak-lock settings; do not change mid-run.
- Keep diagnostics enabled only for the soak window.

---

## 2) Exact user flows to test

Run these flows end-to-end for each selected record cohort:

1. **My Operator create**
2. **My Operator update**
3. **My Operator reload/prefill**
4. **Operator Explorer list**
5. **Operator Explorer detail**
6. **Operator Alignment Snapshot**
7. **Operator Alignment Score Breakdown**
8. **Operator Capability Snapshot regression check**

Flow sequence (recommended):
- Create -> Reload/Prefill -> Update -> Reload/Prefill -> Explorer list -> Explorer detail -> Alignment Snapshot -> Alignment Score Breakdown -> Capability Snapshot check.

---

## 3) Exact records to use

Use at minimum the following six test profile types:

1. **One newly created operator** (created during soak)
2. **One existing new-base operator**
3. **One operator with fuller profile fields**
4. **One operator with sparse/missing fields**
5. **One operator with logo**
6. **One operator without logo**

Selection guidance:
- If one record satisfies multiple categories, still ensure total profile count reaches minimum volume target in Section 8.
- Track each record with a stable test label (e.g., `SOAK_NEW_01`, `SOAK_FULL_01`).

---

## 4) Diagnostic log patterns to capture

Capture all occurrences of:

- `[operator_setup_contract_diag]`
- `fallbackUsed=true`
- `unresolved=true`
- `mirror_prefill_applied`
- `mirror_write_contract`
- `readPath=legacy`
- `readPath=new_base`
- `writeMode=canonical`

Also capture absence checks:
- No owner-facing mock payload markers.
- No non-canonical write mode indicators.

---

## 5) Suggested log capture format

Use a normalized row format (CSV/JSONL accepted) with columns:

- `timestamp`
- `route_or_page`
- `operator_record_id`
- `field_or_concept`
- `canonical_key_expected`
- `key_actually_used`
- `fallback_used` (true/false)
- `unresolved` (true/false)
- `source_layer` (ui/client, api/read, api/write, mapper)
- `user_facing_output_affected` (yes/no)
- `recommended_action`

Recommended `recommended_action` enum:
- `batch_3c_fix`
- `legacy_acceptable`
- `data_backfill_needed`
- `contract_clarification`
- `defer_cleanup`
- `business_review`

---

## 6) Pass/fail criteria

### Pass criteria

- No mock data appears in owner-facing views.
- Canonical writes only (`writeMode=canonical` for write flows).
- No shadow/fail-open behavior observed.
- New records read back with `readPath=new_base`.
- No demo-critical field remains `unresolved=true` without classification.
- Fallback usage is either:
  - expected/acceptable legacy compatibility, or
  - explicitly logged as Batch 3C candidate.

### Fail criteria

- Any owner-facing mock/fallback data leak in go-live staging config.
- Any canonical write failure pattern that breaks create/update reliability.
- Any inability to reload newly written new-base records.
- Unexpected scoring output change for equivalent inputs during soak.

---

## 7) Converting soak findings into Batch 3C

Classify each finding into one and only one bucket:

1. **Confirmed canonical mapping miss**
   - Canonical key expected but alias/fallback used due to mapping gap.
   - Action: targeted mapping contract fix in Batch 3C + contract test.

2. **Fallback acceptable for legacy only**
   - Fallback used only for historical compatibility and behavior is acceptable.
   - Action: keep behavior; add/retain diagnostics and documentation.

3. **Missing data issue**
   - Canonical mapping exists, but source data absent/partial.
   - Action: data quality/backfill task, not mapping code change.

4. **Display/read contract issue**
   - Read-path returns expected data but downstream contract/use is inconsistent.
   - Action: targeted read contract adjustment in Batch 3C (no scoring/display redesign unless approved).

5. **Deferred cleanup**
   - Non-critical legacy alias/noise item.
   - Action: backlog as P2.

6. **Business review needed**
   - Ambiguous ownership, definition, or expected source-of-truth.
   - Action: decision required before code/schema changes.

Batch 3C intake rule:
- Promote only findings with reproducible evidence and clear canonical expectation.

---

## 8) Recommended duration and volume

- Run enough cycles to cover **at least 5-10 operator profiles**.
- Ensure each main owner-facing flow in Section 2 is executed across this profile set.
- Prefer two soak windows:
  - **Window A (daytime):** active create/update traffic simulation.
  - **Window B (off-peak):** stability and read-path consistency checks.

Minimum target:
- 5+ profiles x full flow sequence once
- 3+ profiles x repeat update/reload cycle

---

## 9) Stop conditions

Stop soak and escalate immediately if any occur:

1. Mock data appears in owner-facing views.
2. Canonical writes fail in normal create/update flows.
3. New-base records cannot reload/prefill.
4. Scoring output changes unexpectedly for unchanged inputs.
5. Fallback volume is too noisy to interpret (diagnostic storm masking signal).

If stop condition #5 occurs:
- Throttle capture scope by route/flow slices, do not alter fallback order or behavior.

---

## 10) Recommended post-soak report structure

Create a post-soak report with these sections:

1. **Run metadata**
   - dates, environment, commit refs, env var snapshot
2. **Coverage summary**
   - profiles tested, flows executed, pass/fail counts
3. **Determinism checks**
   - write mode, read path, mock-blocking verification
4. **Diagnostics findings**
   - fallback/unresolved/mirror findings with counts
5. **Finding classification table**
   - mapped to Batch 3C buckets from Section 7
6. **P0/P1/P2 recommendation output**
   - with explicit rationale and evidence links
7. **Batch 3C candidate list**
   - smallest safe set of targeted mapping closures
8. **Deferred/non-actionable items**
   - with reason
9. **Risk assessment**
   - go-live risk posture after soak
10. **Decision recommendation**
   - ready for internal QA gate, external demo gate, go-live gate, or hold

---

## Execution guardrails (no-change soak)

- No code edits during soak.
- No schema edits during soak.
- No scoring changes during soak.
- No display behavior changes during soak.
- No fallback ordering changes during soak.

