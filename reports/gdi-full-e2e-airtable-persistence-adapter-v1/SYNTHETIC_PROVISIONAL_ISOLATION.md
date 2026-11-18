# Synthetic / provisional isolation (D4 Option B)

**No deletions. No new schemas required for V1.**

## Classes (operator-visible)

| Class | Existing signals | Customer / hotel-confirmed? |
|-------|------------------|-------------------------------|
| VERIFIED_HOTEL_PROVIDED | Source `HOTEL_REPORTED`, Status Active | Yes |
| PROVISIONAL_INTERNAL_PLANNING | Source `PROVISIONAL_*`, Status Test Scenario | No |
| SYNTHETIC_TEST | Provenance Synthetic Test / founder_test_scenario | No |
| GENERIC_ONBOARDING_SEED | Fit/Target rationale `generic_seed` | No |
| UNVERIFIED_RESEARCH | Default research | No (until eligibility) |

Module: `lib/group-demand-intelligence/workflow/gdi-synthetic-provisional-isolation-v1.js`

## Recommended filtered views

See `recommendedOperatorIsolationViews()` — Verified Hotel-Reported Only; Provisional/Test; Exclude Synthetic commercial profiles; Non-seed Fits/Targets.

## Persistence rule

Full E2E must not write need-date / GDI commercial tables. Seed Fit/Targets must not be upgraded to verified without evidence. Presentation already blocks TEST_SCENARIO/SYNTHETIC for customer need-date icons.
