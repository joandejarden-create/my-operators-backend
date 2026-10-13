# Period Reconciliation Policy

## Immutability check on original
- allowed in-place mutation: **false**
- reason: CERTIFIED_PERIOD_IMMUTABLE
- guidance: Create a REPROCESSED / new official period with supersedesPeriodId + correctionReason. Do not overwrite certified history.

## Canonical path selected
**C. NEW_CORRECTED_PERIOD**

Allowed patterns evaluated:
- A. CERTIFIED_PERIOD_PATCH_VERSION — **not used** (no versioned patch of certified raw observations without successor)
- B. REPROCESSED_PERIOD_VERSION — equivalent naming under immutability guidance
- C. NEW_CORRECTED_PERIOD — **selected** when retry succeeds (`buildCorrectionPeriodLinkage` + `ADP_CERTIFIED_PERIOD_CORRECTION_V1`)
- D. AUDIT_ATTACHMENT_ONLY — used if retry fails or response non-comparable (retry evidence in report only)

Same-period recovery writers exist for **pre-certification** gaps only. Certified periods must not be overwritten.
