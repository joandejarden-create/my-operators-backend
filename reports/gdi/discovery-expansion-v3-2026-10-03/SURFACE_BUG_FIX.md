# Surface Bug Fix — hasExplicitHotelMotionCopy

## Reproduced
YES — AidEx Geneva was `DOWNGRADE_TO_DEMAND_GENERATOR` / `public_demand_generator_without_hotel_thesis` when `summaryWhyMatters` was boilerplate despite overflow thesis + accommodation URL.

## Root cause
Early path scanned only venue/destination/title when why/matters were boilerplate, **discarding** `hotelOpportunityThesis`.

## Fix
1. Prefer **structured** lodging / housing-URL / placement evidence
2. Always honor non-boilerplate `hotelOpportunityThesis` / overflow thesis
3. Only strip boilerplate why/matters from the free-text blob — never drop thesis

## AidEx
| | Disposition | Customer Ready |
|---|---|---|
| BEFORE | DOWNGRADE_TO_DEMAND_GENERATOR | false |
| AFTER | KEEP_ACTIVE | true |

Remaining blockers after fix: none

## Threshold change
NO

## Regression
`npm run test:gdi-customer-surface-revalidation` includes AidEx KEEP_ACTIVE case.
