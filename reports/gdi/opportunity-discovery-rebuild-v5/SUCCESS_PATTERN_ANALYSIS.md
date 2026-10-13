# Successful Opportunity Pattern (Bethesda / NYC)

## Aggregate (ready-time structural)
| Metric | Value |
|--------|------:|
| Controls | 58 |
| Entity strong % | 97% |
| Timing present % | 98% |
| Lodging hint % | 72% |
| Hotel motion % | 93% |
| WHO path % | 69% |

## Pattern rules (V5 admission)
```json
{
  "requireNamedOrganizer": true,
  "requireFutureTiming": true,
  "requireLodgingOrHousingHint": true,
  "requireHotelMotionThesis": true,
  "preferWhoPath": true,
  "minSupportingSignals": 2
}
```

## What successful opportunities had that zero-yield leads lack
- Named organizer/buyer entity (not SERP title fragment)
- Future date or recurring cycle with decision window
- Explicit hotel-motion thesis (overflow / housing / primary / stay-to-play)
- Public contact OR housing/organizer path
- Market-credible destination/venue — not generic activity news
- Often discovered via housing pages, association site-selection, procurement, or competitor host patterns — not bare event calendars

## Function
`buildGdiSuccessfulOpportunityPattern()` — `lib/group-demand-intelligence/opportunity-discovery-v5/success-pattern.js`
