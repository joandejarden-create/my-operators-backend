# ADP Provider Denominator Audit V1

Contract: `ADP_MEASUREMENT_CONTRACT_V1`

## Canonical rule

Provider Presence Rate = subject-present comparable observations for provider / comparable observations for provider. Failed/missing provider calls are omitted (Missing != measured zero).

Comparable rule: Comparable observations exclude dryRun, provider errors, and unparsed empty responses. Missing provider observation != measured zero.

| Property | Provider | Scheduled | Successful | Failed/Missing | Math Denominator | Display Denominator | Status |
|---|---|---:|---:|---:|---:|---:|---|
| Waterstone | openai | 78 | 78 | 0 | 78 | 78 | **PASS** |
| Waterstone | gemini | 78 | 73 | 5 | 73 | 73 | **PASS** |
| Waterstone | perplexity | 78 | 78 | 0 | 78 | 78 | **PASS** |
| Waterstone | claude | 78 | 78 | 0 | 78 | 78 | **PASS** |
| Renaissance Times Square | openai | 65 | 65 | 0 | 65 | 65 | **PASS** |
| Renaissance Times Square | gemini | 65 | 61 | 4 | 61 | 61 | **PASS** |
| Renaissance Times Square | perplexity | 65 | 65 | 0 | 65 | 65 | **PASS** |
| Renaissance Times Square | claude | 65 | 65 | 0 | 65 | 65 | **PASS** |
| Cambridge Beaches | openai | 60 | 60 | 0 | 60 | 60 | **PASS** |
| Cambridge Beaches | gemini | 60 | 53 | 7 | 53 | 53 | **PASS** |
| Cambridge Beaches | perplexity | 60 | 60 | 0 | 60 | 60 | **PASS** |
| Cambridge Beaches | claude | 60 | 60 | 0 | 60 | 60 | **PASS** |
| NOW NOW NOHO | openai | 63 | 63 | 0 | 63 | 63 | **PASS** |
| NOW NOW NOHO | gemini | 63 | 54 | 9 | 54 | 54 | **PASS** |
| NOW NOW NOHO | perplexity | 63 | 63 | 0 | 63 | 63 | **PASS** |
| NOW NOW NOHO | claude | 63 | 63 | 0 | 63 | 63 | **PASS** |
| Hotel Phillips | openai | 63 | 63 | 0 | 63 | 63 | **PASS** |
| Hotel Phillips | gemini | 63 | 55 | 8 | 55 | 55 | **PASS** |
| Hotel Phillips | perplexity | 63 | 63 | 0 | 63 | 63 | **PASS** |
| Hotel Phillips | claude | 63 | 63 | 0 | 63 | 63 | **PASS** |

## Summary

```json
{
  "PASS": 20,
  "DISPLAY_MISMATCH": 0,
  "CALCULATION_MISMATCH": 0,
  "BOTH": 0
}
```

## Phillips Gemini

```json
{
  "propertyId": "adp_hotel_phillips_kansas_city",
  "property": "Hotel Phillips",
  "provider": "gemini",
  "periodId": "adp_period_adp_hotel_phillips_kansas_city_20260820194028_21bf47",
  "scheduled": 63,
  "attempted": 63,
  "successful": 55,
  "failed": 8,
  "missing": 0,
  "failedOrMissing": 8,
  "mathDenominator": 55,
  "displayDenominator": 55,
  "includedInMath": 55,
  "mentionedComparable": 27,
  "mathRate": 49.1,
  "displayRate": 49.1,
  "oldScheduledDenomRate": 42.9,
  "rateChangedVsScheduledDenom": true,
  "publishedFields": {
    "scheduled": 63,
    "comparable": 55,
    "total": 55,
    "mentioned": 27,
    "excludedFromMetric": 8,
    "presence": 49.1,
    "denominatorGrain": "comparable_observations"
  },
  "status": "PASS",
  "note": "Consideration uses comparable observations (failed Gemini omitted). Provider Gemini must use the same comparable grain, not scheduled 63."
}
```

## OLD vs NEW customer provider values

Rates that changed are calculation corrections (scheduled denom to comparable denom). Complete providers are rate-unchanged.

| Property | Provider | Old Rate/Total | Corrected Rate/Total | Why |
|---|---|---|---|---|
| Waterstone | gemini | 65.4% (51/78) | 69.9% (51/73) | 5 failed omitted from denom |
| Waterstone | openai/claude/perplexity | unchanged | unchanged | complete coverage |
| Renaissance | gemini | 9.2% (6/65) | 9.8% (6/61) | 4 failed omitted |
| Renaissance | openai/claude/perplexity | unchanged | unchanged | complete coverage |
| Cambridge | gemini | 73.3% (44/60) | 83.0% (44/53) | 7 failed omitted |
| Cambridge | openai/claude/perplexity | unchanged | unchanged | complete coverage |
| NOHO | gemini | 20.6% (13/63) | 24.1% (13/54) | 9 failed omitted |
| NOHO | openai/claude/perplexity | unchanged | unchanged | complete coverage |
| Phillips | gemini | 42.9% (27/63) | 49.1% (27/55) | 8 failed omitted |
| Phillips | openai/claude/perplexity | unchanged | unchanged | complete coverage |

Incomplete coverage UI shows `N of M observations captured` when scheduled differs from comparable.
