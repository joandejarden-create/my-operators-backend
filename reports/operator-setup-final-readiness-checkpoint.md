# Operator Setup Final Readiness Checkpoint

Generated: 2026-06-02

Completed commit sequence:
- `bf8865b7f4e00a780090e8cf3915982699056fc0` (Batch 1+2)
- `275136933beda9ed4de55ba44ce13d1eee762091` (Batch 3A)
- `d5ff0d824acd417537c2f2bbb32b2ef0558faf9b` (Batch 3B)
- `1cd90843c2ff19ca5a9dd1b0fb2e683d46fbb5c5` (Batch 3C serviceModelsSupported)

---

## A. Executive readiness call

- **Internal QA:** **Ready**
- **External demo:** **Ready with targeted cleanup**
- **Production/go-live:** **Ready with targeted cleanup** (requires final business/demo data QA and environment confirmation)

Readiness classification:
- Internal QA: `Ready`
- External demo: `Ready with targeted cleanup`
- Production/go-live: `Ready with targeted cleanup`

---

## B. What is now validated

1. **Batch 1 + 2 source-of-truth hardening**
   - Owner-facing explorer path hardened to source-of-truth behavior.
   - Canonical write mode stabilized and deterministic.
   - Mock-blocking behavior validated for non-rec operator IDs.

2. **Batch 3A mapping fixes**
   - Canonical logo and narrative mappings validated.
   - `brand_conversion_project_count` input/save/readback path validated.

3. **Batch 3B diagnostics/provenance**
   - Non-user-facing canonical-vs-fallback diagnostics added and validated.
   - No user-facing display/scoring/fallback-order changes introduced.

4. **Staging diagnostics soak**
   - Canonical writes, non-rec mock blocking, read-path behavior validated.
   - Fallback patterns identified and classified.

5. **Batch 3C serviceModelsSupported closure**
   - Canonical contract gap closed for `serviceModelsSupported`.
   - Canonical save/reload/detail/explorer validation passed.
   - Fallback to `primaryServiceModel` remains intact.
   - Fallback order preserved.
   - Output parity preserved.

6. **Deal-linked OAS + score validation**
   - Requested operator mismatch was confirmed as a **deal-cohort data issue**, not product regression.
   - Linked operator validation succeeded and closed runtime OAS/score parity gap:
     - OAS module validation passed.
     - Score stable across duplicate runs.
     - Canonical `serviceModelsSupported` present.
     - No mock data/legacy-fail-open-shadow evidence.

---

## C. Remaining targeted cleanup

- **Deal/operator cohort hygiene for demo scripts**
  - Requested operator `recBVEgtm8cS96mu7` was not in `recIeGRZP21udmTnt` companies cohort.
  - Keep demo test pairs explicitly aligned (deal-linked operator set).

- **Demo data quality pass**
  - Ensure selected demo operators have complete business-facing profile coverage.
  - Resolve sparse-field content gaps that are data issues (not mapping defects).

- **Auth-context QA**
  - OAS HTTP routes are auth-gated (`401` without session). Confirm expected behavior in authenticated demo context.

---

## D. Deferred/non-blocking items

- Browser-only `explorerProfileJson` mirror masking diagnostics pass (observability follow-up).
- Broader legacy/alias cleanup work (non-blocking).
- Batch 4 select/multi-select normalization (explicitly deferred).

---

## E. Do-not-touch-yet list

- Airtable schema changes (rename/delete/add for this track).
- Scoring formulas, weights, thresholds, recommendation logic.
- Fallback order changes.
- Removal of `primaryServiceModel` fallback.
- Removal of aliases/remaps.
- `explorerProfileJson` behavior removal.
- Batch 4 normalization implementation before demo readiness gate.

---

## F. Deployment environment variables

Use for staging and production (prod-like deterministic mode):

- `NODE_ENV=production`
- `OPERATOR_SETUP_WRITE_MODE=canonical`
- `OPERATOR_SETUP_ALLOW_NON_CANONICAL_PROD=0`
- `OPERATOR_EXPLORER_DIAGNOSTIC_ALLOW_MOCKS=0`
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0`
- `OPERATOR_SETUP_NEW_BASE_FAIL_OPEN=0`
- `OPERATOR_SETUP_CONTRACT_DIAGNOSTICS=0` (set `1` only in controlled diagnostics windows)

---

## G. Final pre-demo QA checklist

1. My Operator create/update save in canonical mode.
2. Reload/prefill for key fields including `serviceModelsSupported`.
3. Detail readback confirms `readPath=new_base` for selected demo operators.
4. Explorer detail confirms canonical `serviceModelsSupported` with unchanged display value.
5. Non-rec operator request remains blocked (no mock payload).
6. Deal-linked OAS companies validation for the exact demo deal/operator pair.
7. Operator score breakdown duplicate-run stability check.
8. Confirm no shadow/fail-open behavior in runtime logs.
9. Authenticated page pass for OAS snapshot pages (HTTP auth-gated routes).
10. Quick sparse-data scan on demo operators (content completeness, not code changes).

---

## H. Recommendation for external demo

- **Ready with targeted cleanup**
- Proceed after finalizing one or more known-good deal/operator demo pairs and running the pre-demo checklist above.

---

## I. Recommendation for production/go-live

- **Ready with targeted cleanup**
- Go-live should follow:
  - final business/demo data QA pass,
  - deployment env var confirmation,
  - authenticated smoke pass on OAS/UI routes.

No product-regression blockers remain from the validated batches in this sequence.

---

## J. Recommended next batch after demo readiness

- **Batch 3D: Demo/production data reliability and validation harness hardening**
  - Lock canonical demo deal/operator fixtures.
  - Add repeatable deal-linked validation checks.
  - Expand data completeness QA for sparse operator profiles.
  - Keep scope free of scoring/schema/normalization changes.

After Batch 3D (if needed), proceed to planned **Batch 4 option normalization** under separate approval.

