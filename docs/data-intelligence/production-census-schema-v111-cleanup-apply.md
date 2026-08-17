# Production Census Schema v1.1.1 Cleanup Apply

**Status:** `production_census_schema_v111_cleanup_partial_manual_view_steps_needed`
**Generated:** 2026-08-05T10:44:25.815Z
**Apply executed:** true
**Base:** Deal Capture Platform (masked `appCCU…foLk`)
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Executive summary

- Three approved renames applied via Airtable Meta API.
- Census validation passed (666 records, no deletes, no record writes, enrichment still blank).
- Amenity-field hide and founder views require manual Airtable UI steps (API unsupported / 422).
- Brand Explorer safety gates all PASS (Active 62, semantic 0/0/0).

## Fields renamed

| From | To | Field ID |
| --- | --- | --- |
| Last Verified Date | Last Reviewed Date | fldxOmnKSWHBmjigl |
| Resort Amenities Flag | Resort / Leisure Flag | fldYIHZ2pMot20LIO |
| Extended Stay Amenity Flag | Extended Stay Flag | fldixhIZZLcHDXxDn |

## Fields kept unchanged

- Rooms / Keys
- Operator / Management Company
- Owner Name
- Source URL
- State / Region

## Manual hide (Decision G)

Do **not** delete. In **Deal Capture Platform → Hotel Property Census → Grid view**, hide:

- Fitness Flag
- Pool Flag
- Parking Flag
- Airport Shuttle Flag
- Spa Flag
- Beach / Waterfront Flag

## Manual founder views

Airtable `POST .../views` returned **422** for this base. Create these Grid views manually:

### Census - Core Identity
Property Name, Current Brand, City, State / Region, Country, Affiliation Status, Production Use Status, Data Confidence Tier, Enrichment Status, Human Review Required

### Census - Enrichment
Property Name, Hotel Description - Source Text, Hotel Description - AI Summary, Amenities - Source Text, Amenities - Structured Tags, Property Type, Asset Context, Market / Submarket, Enrichment Status, Enrichment Priority

### Census - Owner Operator
Property Name, Owner Name, Owner Confidence, Operator / Management Company, Operator Confidence, Ownership Review Status, Operator Review Status

### Census - Steward Review
Property Name, Human Review Required, Notes for Steward, Brand-Unassigned Reason, Enrichment Priority

## Census validation

- Records: 666; duplicates: 0; fields: 95 (no deletes)
- Enrichment Status = Not Started: 666
- Human Review Required = true: 4 held only
- Descriptions / amenities / owner / operator / rooms / dates: still blank
- Production Use Status unchanged; no 0,0 coordinates

## Brand Explorer safety

- Active universe: **62**
- Semantic C/H/M: **0/0/0**
- PVQL: PASS
- Momentum evidence: PASS
- Mandatory release gates: PASS

## Next

Complete manual hide + view setup if needed, then freeze Census field contract and start descriptions + amenities + property type enrichment.
