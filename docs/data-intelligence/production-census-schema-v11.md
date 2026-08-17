# Production Census Schema v1.1

**Acceptance:** `production_census_schema_v11_ready_for_future_enrichment`  
**Base (Platform):** `appCCU…foLk`  
**Census table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Census records:** **666** (unchanged; no duplicates)  
**Fields added:** **62** (table now 95 fields including links)  
**Safe backfill:** 666 patched · Enrichment Status = Not Started · Human Review Required = true on **4** held records

## Principle

Hotel Property Census remains the **master property record**.  
Supporting tables unchanged: Brand Affiliations, Source Evidence, Steward Review.

One property → one master Census row → future enrichment layers.

## Field groups added

- Description / profile (class, type, asset context, market/submarket, summaries)
- Amenities (source text, structured tags, amenity flags)
- Physical / scale (rooms, opening/renovation — empty until source-backed)
- Ownership / development (owner/developer — empty until source-backed)
- Operator / management (operator fields — empty until source-backed)
- Independent / unassigned hotel logic flags
- Data governance (confidence, enrichment status/priority, steward notes)

## Safe backfill only

| Field | Applied |
| --- | --- |
| Enrichment Status | Not Started (666) |
| Human Review Required | true for 4 steward-held Brand-Unconfirmed; false otherwise |
| Data Confidence Tier | from Identity Confidence |
| Enrichment Priority | High (held) / Medium (data-eligible) / Low |
| Independent Classification / soft-brand / conversion flags | derived from Affiliation Status only |

**Not backfilled:** descriptions, amenities, owner, developer, operator, rooms, opening/renovation dates, management model.

## Safety

- Production Use Status remains `Census Only / Not Owner-Facing`
- Brand Explorer Presentation untouched
- No fake owner/operator/rooms/dates
- No 0,0 coordinates
- Frozen VIC + frozen 62 untouched
- No Brand Status / CV / Verified / Momentum writes

## Next

Source-backed enrichment lanes may populate new columns later.  
Brand Explorer production patch remains **blocked**.

Reports:

- `reports/research-engine-v2/production-census-schema-v11-dry-run.{md,json}`
- `reports/research-engine-v2/production-census-schema-v11-apply.{md,json}`
