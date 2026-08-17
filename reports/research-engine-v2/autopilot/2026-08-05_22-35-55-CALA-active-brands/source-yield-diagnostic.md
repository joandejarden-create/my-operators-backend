# Source Yield Diagnostic

- **Run:** 2026-08-05_22-35-55-CALA-active-brands
- **High proposals:** 0
- **Airtable writes:** false
- **Apply recommendation:** NO — High proposals < 10 — improve extractors / source access before apply unless founder wants a smoke-test write.

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
| address_confirmation | executed_exhausted | 0 | 0 | production-census-address-geocode-resolver |
| property_name_cleanup | executed_exhausted | 2 | 0 | production-census-property-name-cleanup-extractor |
| property_type_asset_context | executed_exhausted | 425 | 0 | production-census-population-lane-2 |
| rooms_keys | executed_exhausted | 420 | 0 | production-census-rooms-keys-extractor |
| coordinate_resolution | soft_deferred | 0 | 0 | geocode-provider (soft-deferred) |

## No-proposal reasons (taxonomy)

| Reason | Count |
| --- | ---: |
| K:brand_census_match_issue | 723 |
| E:extractor_too_narrow | 627 |
| C:official_page_fetch_blocked | 398 |
| D:page_fetched_data_not_present | 79 |

## Highest-yield next improvement

- **corporate_bot_block_bypass_learning:** Hilton/Marriott/Choice official pages return 403 to Node fetch — need approved public-source path or Webhound learning for edge patterns (not production writes).

## Blocked source families

- Hilton (hilton.com edge 403 — Page Reference Code)
- Marriott (marriott.com 403)
- Choice (choicehotels.com 403)
