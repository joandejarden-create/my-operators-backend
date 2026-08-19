# Owner Intent — Chg vs Prior Run V1

> **Status:** Wired for automatic customer display when a comparable prior exists.  
> **Live current:** federated Brand AI baseline `DEMO_VALIDATION` / `2026-08-14`.  
> **Two periods ≠ trend.**

## What customers see

Compact Coverage Diagnostics column **Chg vs Prior Run**:

| State | Display |
|---|---|
| Both exact-scope periods certified, comparable | `+9 pts` / `-3 pts` / `No change` |
| No comparable prior | `Insufficient History` |
| Current certified, prior not | `—` (no numeric index change) |
| Uncertified index (e.g. Gemini Soft Brand) | `—` even if Presence history exists |

Change is **index points**, never percent. Never show `0` for missing history.

Presence period-over-period lives in **expanded Historical Comparison** only.

## Comparison unit

`subjectBrandId × scenarioId × providerScope × geography × language`

No cross-provider, cross-scenario, cross-geography, or cross-language fill.

## Period selection

- **CURRENT** = the period backing the live Owner Intent row (today: `DEMO_VALIDATION`)
- **PRIOR** = most recent earlier valid comparable period

A later unpublished wave is **not** prior. Periods are never pooled. Cross-period deduplication is prohibited.

## Certification

Numeric index change requires:

1. Current exact scope certified under the current period
2. Prior exact scope certified under a comparable certification contract

Registry records are period-tagged (`measurementPeriod: DEMO_VALIDATION`). The 2026-08-18 longitudinal period is **not** auto-promoted (`AUTOMATIC_CUSTOMER_PROMOTION = false`).

## Automatic after a wave

After a governed live join of a new Brand longitudinal period:

1. Identify latest completed period as current
2. Identify prior comparable period
3. Recertify exact scopes offline
4. Compute Chg vs Prior for eligible rows
5. Customer payload updates with no frontend per-brand code

## Copy

Forbidden in this column/section: Trend, Trending up/down, Improving, Declining.

Gate: `npm run test:brand-ai-owner-intent-chg-vs-prior-v1`
