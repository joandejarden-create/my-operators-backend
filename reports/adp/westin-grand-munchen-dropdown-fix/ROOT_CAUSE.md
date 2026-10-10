# Root Cause — Westin Missing from ADP Dropdown

## Classification

**VISIBILITY_FLAG_MISSING** (also reads as skipped post-cert catalog registration)

## Exact defect

| Field | Hilton TS | Renaissance TS | YOTEL Geneva | **Westin München (before)** |
|-------|-----------|----------------|--------------|-----------------------------|
| customerDropdownVisible | true | true | **false** | **false** |
| officialBaselinePublished | (n/a/true) | (n/a) | false | **false** |
| CERTIFIED published period | YES | YES | YES | YES |
| In listPropertyProfiles | YES | YES | NO | **NO** |

Westin was fully CERTIFIED and published (`publishStatus: Live`, period `adp_period_adp_westin_grand_munchen_20261007145836_73c25a`) but the fixture still had `customerDropdownVisible: false` from preflight scaffolding.  
`listPropertyProfiles` hard-skips that flag → API returns no Westin option → UI dropdown empty for this hotel.

YOTEL remains `false` intentionally (prospect / not customer-dropdown-released). Regression = YOTEL still excluded; peers still present.

## Not the cause

- Missing published period / certification filter at API (manifest was Live + CERTIFIED)
- Frontend cache
- Country/market filter
- Hotel-specific UI omit list
