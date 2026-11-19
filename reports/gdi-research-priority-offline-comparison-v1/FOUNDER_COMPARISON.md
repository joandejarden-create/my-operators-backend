# GDI research-priority offline comparison V1

Generated: 2026-11-19T12:11:54.403Z

## Safety
- Paid calls: 0
- Airtable writes: 0
- Production selection policy changed: **false**

## Structural finding
With fairMinPerSegment=1 and maxCandidates < planned segments, commercial score fill never runs; late taxonomy segments are systematically deferred.

## Bethesda Marriott (frozen inventory)

| Metric | Value |
| --- | ---: |
| Inventory rows | 86 |
| Cap | 8 |
| Current selected | 8 |
| Proposed pure-score selected | 8 |
| Overlap current∩proposed | 1 |
| Overlooked by current (in proposed top) | 7 |

### Segments selected under current policy
```
{
  "associations_conferences_events": 1,
  "corporate_accounts_scaling": 1,
  "corporate_meetings_incentives": 1,
  "government_public_sector": 1,
  "sports_entertainment": 1,
  "education_scientific": 1,
  "projects_infrastructure": 0,
  "social_special_events": 1,
  "local_catering_day_meetings": 1,
  "corporate_celebrations_private_events": 0,
  "travel_trade_group_intermediaries": 0,
  "relocation_extended_stay": 0,
  "preferred_lodging_partnerships": 0,
  "professional_event_planners": 0
}
```

### Examples overlooked by current cap (proposed score)
- American Medical Women's Association (AMWA) · associations_conferences_events · proposed=75.5 existing=53.08
- American Medical Women's Association (AMWA) · associations_conferences_events · proposed=73 existing=51.13
- Association for Clinical and Translational Science (ACTS) · education_scientific · proposed=73 existing=41.58
- National Down Syndrome Society (NDSS) · associations_conferences_events · proposed=72.6 existing=80.25
- AHIMA · associations_conferences_events · proposed=72.2 existing=80.25
- American College of Cardiology (ACC) · associations_conferences_events · proposed=72.2 existing=82.2
- Arlington Soccer Association · associations_conferences_events · proposed=71.4 existing=81.22

## Recommendation
Do not enable proposed weights in production until founder review. Consider raising default deep cap to plannedSegmentCount OR reserving commercial-fill slots after a bounded fair floor.

**Founder gate:** do not enable proposed weights in production until review.
