# HBX Census Schema + Identity Linkage v1

**Status:** `production_census_hbx_census_schema_and_identity_linkage_v1_complete`  
**Objective:** `hbx-census-schema-and-identity-linkage-v1`  
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`) — Deal Capture Platform only

## What shipped
1. Created **16** missing HBX identity + phone provenance fields on Hotel Property Census via Meta API (`ENABLE_HBX_SCHEMA_REPAIR=1`).
2. Re-ran **identity/provenance linkage only** for the same Wave1 `existing_match_high` set (**470** unique Census IDs).
3. Did **not** re-run Phase 1 Address/Phone/URL enrichment; did **not** insert; did **not** write rooms/coords/media/facilities.

## Field mapping (linkage writes)
| Airtable field | Source |
| --- | --- |
| HBX Hotel Code | candidate `hbx_hotel_code` |
| HBX Chain Code | candidate `chain_code` |
| HBX Category Code | candidate `category` |
| HBX Linkage Confidence | `High` for existing_match_high |
| HBX Source Status | `Matched` |
| HBX Content Review Status | `Internal Only` |
| Phone Confidence / Source Type / Review / Date / Notes / Source URL | when Census Phone already present; Source Type=`hbx_content_api` |

Blank-only for identity codes. Phone **value** not rewritten. Notes for Steward left in place.

## Results
- Schema missing: **0**
- HBX Hotel Codes written: **470**
- HBX Chain Codes: **392**
- Category codes: **470**
- Phone provenance: **469**
- Conflicts (phone mismatch → Needs Review): **83**
- Inserts: **0**

## Forbidden (confirmed not written)
Rooms / Keys, Latitude/Longitude, images, descriptions, facilities, owner/operator/developer, opening/renovation/affiliation dates, Recent Momentum, Company Validated, Brand Verified, Brand Status, Brand Explorer, Brand Setup.

## Artifacts
- `reports/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.md`
- `reports/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.json`
- Module: `lib/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.js`
