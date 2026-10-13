# YOTEL Completeness Forensic

## Policy that permitted certification
- **Policy name:** ADP_PROVIDER_COMPLETENESS_POLICY_V1
- **Expected:** 252
- **Successful:** 251
- **Failed:** 0
- **Timed out:** 1
- **Overall failure cap:** max(2, floor(expected × 0.10)) = **25**
- **Provider failure cap (per provider, scenarioCount=63):** **6**
- **Absolute tolerance:** 2
- **Percentage tolerance:** 10%
- **Provider-specific:** zero-success OR per-provider failures above cap → material (review)
- **Retry required by policy:** NO
- **Failed response in consideration denominator:** NO (excluded by comparable filter)
- **Consideration denominator used:** **251** (251), not 252
- **Provider imbalance:** []
- **materialFailure:** false
- **silentDenominatorReduction:** false

## Why 1 Claude timeout still certified
failed+timedOut = 1 ≤ overall cap 25; Claude successful=62 (>0); Claude failures=1 ≤ provider cap 6.

## Certification recheck
- status: **CERTIFIED**
- engineStatus: **CERTIFIED**
- hardFailures: none
- reviewFlags: none
