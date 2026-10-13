# Completeness Policy Contract

```json
{
  "policyName": "ADP_PROVIDER_COMPLETENESS_POLICY_V1",
  "policyVersion": "adp_provider_completeness_policy_v1",
  "howManyFailuresMayOccur": "max(2, floor(expected × 0.1)) overall AND per provider",
  "percentageBased": true,
  "percentTolerance": 0.1,
  "absoluteTolerance": 2,
  "oneTimeoutAllowed": "YES when failed+timedOut ≤ cap (for expected=252, cap=25; for per-provider scenarioCount=63, cap=6)",
  "retriesRequired": false,
  "providerMateriallyIncompleteMayCertify": "NO — zero successful responses for any provider OR per-provider failures above cap → QA_REVIEW_REQUIRED (or hard fail if silent reduction)",
  "whenQaReviewRequired": "materialFailure without silentDenominatorReduction (overall or provider-level imbalance)",
  "whenQaFailed": "silentDenominatorReduction (attempted < 85% expected unexplained) or other hard gates",
  "considerationDenominator": "comparable successful observations only — timeouts/errors excluded (not silent grain change; documented exclusion)"
}
```

## Source of truth
`lib/ai-demand-positioning/certification/adp-provider-completeness-policy-v1.js`

## Status mapping
| Condition | Outcome |
|-----------|---------|
| silentDenominatorReduction | QA_FAILED (hard) |
| materialFailure (overall or provider imbalance) without silent reduction | QA_REVIEW_REQUIRED |
| otherwise + all other gates pass | CERTIFIED (certification-era) / LEGACY_QA_PASS (legacy audit) |
