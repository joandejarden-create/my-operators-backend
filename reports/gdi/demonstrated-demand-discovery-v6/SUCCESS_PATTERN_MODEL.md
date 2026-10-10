# Successful Opportunity Pattern Model (Bethesda / NYC)

Controls analyzed: **58**

## Pre-readiness success traits
- SUCCESS TRAIT 1: Named organizer/buyer entity (not SERP title fragment)
- SUCCESS TRAIT 2: Future date or recurring cycle with decision window
- SUCCESS TRAIT 3: Explicit hotel-motion thesis (overflow / housing / primary / stay-to-play)
- SUCCESS TRAIT 4: Public contact OR housing/organizer path
- SUCCESS TRAIT 5: Market-credible destination/venue — not generic activity news
- SUCCESS TRAIT 6: Often discovered via housing pages, association site-selection, procurement, or competitor host patterns — not bare event calendars

## Trait IDs (admission scoring)
1. NAMED_ORGANIZER_BUYER_ENTITY
2. FUTURE_DATE_OR_CYCLE_DECISION_WINDOW
3. EXPLICIT_HOTEL_MOTION_THESIS
4. PUBLIC_CONTACT_OR_HOUSING_PATH
5. MARKET_CREDIBLE_VENUE_OR_DESTINATION
6. LODGING_OR_HOUSING_EVIDENCE
7. SOURCE_AUTHORITY_OR_OFFICIAL_PAGE
8. REPEAT_OR_COMPETITOR_HOST_PATTERN

## Aggregate ready-time rates
| Metric | Value |
|--------|------:|
| Entity strong % | 97% |
| Timing present % | 98% |
| Lodging hint % | 72% |
| Hotel motion % | 93% |
| WHO path % | 69% |

## Admission rule
DEMONSTRATED_DEMAND_LEAD = true  
AND SUCCESS_PATTERN_MATCH ∈ {STRONG, PLAUSIBLE}  
→ admit to Jev completion (not customer-ready by itself)
