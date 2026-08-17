# Production Census Schema v1.1 — Apply

**Status:** `production_census_schema_v11_ready_for_future_enrichment`
**Fields added:** 62
**Census count after:** 666
**Brand Explorer untouched:** true
**Duration ms:** 130487

## Fields added

- Hotel Description - Source Text (multilineText)
- Hotel Description - AI Summary (multilineText)
- Short Property Summary (multilineText)
- Property Positioning (multilineText)
- Hotel Class / Segment (singleSelect)
- Property Type (singleSelect)
- Asset Context (singleSelect)
- Market / Submarket (singleLineText)
- Amenities - Source Text (multilineText)
- Amenities - Structured Tags (multilineText)
- F&B Flag (checkbox)
- Meeting Space Flag (checkbox)
- Fitness Flag (checkbox)
- Pool Flag (checkbox)
- Resort Amenities Flag (checkbox)
- Extended Stay Amenity Flag (checkbox)
- Parking Flag (checkbox)
- Airport Shuttle Flag (checkbox)
- Spa Flag (checkbox)
- Beach / Waterfront Flag (checkbox)
- Branded Residences Flag (checkbox)
- Mixed-Use Flag (checkbox)
- Rooms / Keys (number)
- Rooms Source URL (url)
- Rooms Confidence (singleSelect)
- Building / Asset Notes (multilineText)
- Opening Date (date)
- Opening Date Source URL (url)
- Renovation / Conversion Status (singleLineText)
- Renovation / Conversion Date (date)
- Renovation / Conversion Source URL (url)
- Owner Name (singleLineText)
- Owner Type (singleSelect)
- Owner Source URL (url)
- Owner Confidence (singleSelect)
- Developer Name (singleLineText)
- Developer Source URL (url)
- Developer Confidence (singleSelect)
- Ownership Review Status (singleSelect)
- Operator / Management Company (singleLineText)
- Operator Type (singleSelect)
- Management Model (singleSelect)
- Operator Source URL (url)
- Operator Confidence (singleSelect)
- Operator Review Status (singleSelect)
- Possible Operator Target (checkbox)
- Independent Hotel Flag (checkbox)
- Independent Classification (singleSelect)
- Brand-Unassigned Reason (singleLineText)
- Possible Soft-Brand Candidate (checkbox)
- Possible Brand Conversion Candidate (checkbox)
- Possible Owner Outreach Target (checkbox)
- Possible Financing Target (checkbox)
- Possible Dealality Opportunity (checkbox)
- Data Confidence Tier (singleSelect)
- Relationship Confidence (singleSelect)
- Last Verified Date (date)
- Next Review Needed (date)
- Enrichment Status (singleSelect)
- Enrichment Priority (singleSelect)
- Human Review Required (checkbox)
- Notes for Steward (multilineText)

## Safe backfill applied

```json
{
  "records_patched": 666,
  "enrichment_status_not_started": 666,
  "human_review_required_true": 4,
  "errors": []
}
```

## Safety

- No fake owner/operator/rooms/dates: true
- No 0,0 coords: true
- Production Use Status OK: true
