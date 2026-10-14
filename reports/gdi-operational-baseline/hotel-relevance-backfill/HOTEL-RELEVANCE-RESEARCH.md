# Hotel-relevance research — coordinator stage

**Version:** `gdi-hotel-relevance-research-v1`  
**Module:** `lib/group-demand-intelligence/workflow/hotel-relevance-research-v1.js`  
**Coordinator stage:** `synthesize_hotel_relevance` (after `enrich_contacts`, before `evaluate_eligibility`)

## What it produces (mandatory)

For each candidate, before final qualification / card generation:

1. Account + occasion + traveling cohort  
2. Hotel business mechanism (how lodging demand arises)  
3. Property-specific fit (hotel config + occasion evidence)  
4. Limitations / competing arrangements / uncertainties  
5. Timing rationale  
6. Contact route + responsibility  
7. Specific next sales action  

Facts get sources; commercial inference is labeled (`INFERENCE:` / `MIXED:`).  
Unnamed hotel / TBA block does **not** become “open buying.”

## Disposition

| Disposition | Meaning |
|-------------|---------|
| `READY_FOR_CARD` | Core answers present — eligible for card/drawer generation |
| `INTERNAL_GAP` | Precise gaps recorded — keep internal / enrich |
| `INSUFFICIENT` | Too incomplete or open-buying guard tripped — not facing |

## Future hotel run

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <hotelId> --dry-run
# Stage synthesize_hotel_relevance writes:
#   runs/<id>/hotel_relevance_research/hotel-relevance-research.json
#   runs/<id>/hotel_relevance_research/sales-review-overlay-from-research.json
```

Publication uses the **research-generated** overlay, not a hand package.

## Mayflower backfill

```bash
node scripts/gdi-backfill-hotel-relevance-research-v1.mjs --hotel rec0eTEnCqlsGJJt9 --adapter airtable --apply
```

Recovers existing substantive briefs into the canonical research object, stamps `hotelRelevanceResearch` on Airtable records via importer, and remains reproducible for the next hotel.
