# HIF V6 — Hotel Explorer Commercial Timing & Geography Filter Spec

**Status:** Integration-ready specification · **CUSTOMER_BLOCKED** · no unauthorized publication  
**As of:** 2026-11-21

## User questions to answer

1. “Show hotels in my target country with credible brand conversion or management opportunities within the next 24 months.”
2. “Show all known future contractual events, regardless of timing.”

## Required filters

| Filter | Source field |
|--------|----------------|
| Country | `physicalCountry` (VERIFIED / REVIEW_REQUIRED only for scored views) |
| Region/state | `stateProvince` |
| City/market | `city` / `market` |
| Opportunity type | BCT / MCT / contractual intelligence / HOLD |
| Commercial priority | PRIORITY_A … PRIORITY_D / HOLD |
| Next decision window | `nextCommercialDecisionDate` |
| Months to event | `monthsToDecision` |
| Timing horizon | `0_6_MONTHS` … `5_PLUS_YEARS` / UNKNOWN / HISTORICAL |
| Decision window kind | CONFIRMED / EVIDENCE_INFERRED / EXPIRATION_ONLY_PROXY / TIMING_UNKNOWN |
| Agreement type | FRANCHISE / MANAGEMENT / OPERATING / MULTIPLE / UNRESOLVED |
| Agreement confidence | resolved vs unresolved |
| Current opportunity status | active / historical / superseded |

## Display rules

- Show **WHY** relevant and **WHEN** a decision may arise.
- Do **not** present EXPIRATION_ONLY_PROXY as a confirmed decision deadline.
- Do **not** expose invented notice periods.
- Geographically UNRESOLVED hotels stay out of country scorecards.
- Worldwide eligible; priority markets are concentration only.

## Data contract

Produced by `scripts/hif-v6-commercial-timing-geo-audit.mjs` → `reports/sec-edgar-hif-v6-commercial-timing-geo-audit/`.
