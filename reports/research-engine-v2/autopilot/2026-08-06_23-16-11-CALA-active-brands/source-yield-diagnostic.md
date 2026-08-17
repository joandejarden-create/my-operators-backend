# Source Yield Diagnostic

- **Run:** 2026-08-06_23-16-11-CALA-active-brands
- **High proposals:** 116
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
| brand_normalization | executed | 1091 | 116 | unknown |

## No-proposal reasons (taxonomy)

| Reason | Count |
| --- | ---: |
| — | 0 |

## Highest-yield next improvement

- **continue_address_then_ihg_rooms:** Continue address-only High writes, then IHG rooms where official counts appear.

## Blocked source families

- Hilton (hilton.com edge 403 — Page Reference Code)
- Marriott (marriott.com 403)
- Choice (choicehotels.com 403)
