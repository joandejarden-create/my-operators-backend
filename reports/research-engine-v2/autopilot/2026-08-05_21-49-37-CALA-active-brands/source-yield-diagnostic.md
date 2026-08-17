# Source Yield Diagnostic

- **Run:** 2026-08-05_21-49-37-CALA-active-brands
- **High proposals:** 108
- **Airtable writes:** false
- **Apply recommendation:** YES — High proposals ≥ 10 and safety checks pass — recommend approval-bundle-bound apply after founder review.

## Why yield was low

- Prior smoke used tiny description fetch budgets and burned attempts on Hilton 403s
- IHG descriptions/amenities already populated for active brands
- Hilton/Marriott/Choice corporate pages bot-blocked from Node fetch
- Address High VIC claims were dropped when geocode was deferred (fixed: address-only path)
- Rooms High exhausted for prior avid writes; many IHG pages have empty numberOfRooms

## Queues

| Queue | Status | Eligible | High | Extractor |
| --- | --- | ---: | ---: | --- |
| description_extraction | executed_exhausted | 341 | 0 | production-census-description-extractor |
| amenities_extraction | executed_exhausted | 1007 | 0 | description-extractor+lane-2 |
| radar_public_readiness | executed_exhausted | 425 | 0 | production-census-population-lane-2 |
| address_confirmation | executed | 107 | 107 | production-census-address-geocode-resolver |
| property_name_cleanup | executed_exhausted | 2 | 0 | production-census-property-name-cleanup-extractor |
| property_type_asset_context | executed | 425 | 1 | production-census-population-lane-2 |
| rooms_keys | executed_exhausted | 420 | 0 | production-census-rooms-keys-extractor |
| coordinate_resolution | soft_deferred | 0 | 0 | geocode-provider (soft-deferred) |

## No-proposal reasons (taxonomy)

| Reason | Count |
| --- | ---: |
| — | 0 |

## Highest-yield next improvement

- **ihg_rooms_prose_patterns:** IHG hoteldetail often omits numberOfRooms — expand High-only prose patterns when explicit.

## Blocked source families

- Hilton (hilton.com edge 403 — Page Reference Code)
- Marriott (marriott.com 403)
- Choice (choicehotels.com 403)
