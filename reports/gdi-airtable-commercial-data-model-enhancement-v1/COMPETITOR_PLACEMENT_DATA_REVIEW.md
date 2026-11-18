# Competitor placement data review

## Existing

- Payload / publication: `competitorHotel`, `cmiCompetitorHotel`, `competitorVenueStatus`, `competitorVenueStatusLabel`  
- Contract: `cmi-canonical-eligibility-contract-v1.js` evidence statuses  
- GDO typed: `venueStatus` / `sourcingStatus` (placement openness, not always named competitor)  
- Weekly material fields include `hostHotel`, `venue` (payload)

## Distinctions supported by policy

CONFIRMED_PLACEMENT · RELIABLE_INFERRED_PLACEMENT · HISTORICAL_PLACEMENT · UNKNOWN — via CMI evidence status, not a new table.

## Gaps

- Population inconsistent; Airtable-only rebuild may lose CMI JSON if compacted  
- Named incumbent hotel rarely typed  

## Recommendation

Reuse JSON + publication freeze; optional typed competitor fields only if ops filtering required (CD-5). Do not treat competitor booking as open RFP.
