# GDI Maturity Funnel V1 — Backfill Dry-Run

**Generated:** 2026-10-05T07:56:02.795Z
**Mode:** DRY RUN (no writes)
**Gate bypass:** NO
**Speculative accounts created:** NO

## Focus hotels (Bethesda / W Rome / YOTEL)

| Hotel | Total | Old Ready | Old Visible | SIGNAL | CANDIDATE | QUALIFIED | ACTIONABLE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| recLuxvwwxID7U2B8 | 54 | 28 | 28 | 0 | 26 | 0 | 28 |
| rece0or38cxo3Fymb | 13 | 0 | 5 | 0 | 13 | 0 | 0 |
| recrPQcZg7SFARRb2 | 95 | 2 | 2 | 1 | 92 | 0 | 2 |

## Global maturity counts

```json
{
  "CANDIDATE": 131,
  "ACTIONABLE": 30,
  "SIGNAL": 1
}
```

## Account quality distribution (classified)

```json
{
  "CONTACT_PATH_SHELL": 7,
  "GENERIC_ORG_SHELL": 53,
  "TRUE_BUYER_ACCOUNT": 38,
  "UNKNOWN": 1,
  "TRUE_PARTICIPATING_ACCOUNT": 2,
  "GENERATOR_WRAPPER": 53,
  "VENUE_OPERATOR_PLACEHOLDER": 6,
  "TRUE_ORGANIZER_HOUSING_ACCOUNT": 2
}
```

## Visibility delta if `GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1=1`

```json
{
  "newlyVisible": 0,
  "newlyHidden": 5,
  "unchanged": 157
}
```

**Recommendation:** Keep `GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1` **OFF** until Phase 2 review if `newlyVisible` includes questionable rows.

## Rows that became QUALIFIED

_No rows classified as QUALIFIED under Phase 1 evaluator (expected — strict cohort/lodging/account requirements)._


## Legacy field mapping (compat)

| Legacy signal | Suggested maturity (non-authoritative) |
| --- | --- |
| Ready / CONTACT_NOW / HIGH_PRIORITY | ACTIONABLE |
| QUALIFY_NOW / QUALIFIED | QUALIFIED (suggested only) |
| WATCH / RESEARCH_FURTHER / FUTURE_WATCH | CANDIDATE |
| DEMAND_GENERATOR / DISQUALIFIED | SIGNAL |

**Authoritative:** `gdiMaturityState` from `assignGdiMaturityState()`.
**ACTIONABLE** ≡ `isGdiCustomerOpportunityReady()` (unchanged).

## Artifacts

- `reports\gdi\maturity-v1\backfill-dry-run.csv`
- `reports\gdi\maturity-v1\backfill-dry-run.md`
