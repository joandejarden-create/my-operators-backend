# Full CALA 15K Shell Format + Source/Candidate Brand Backfill v1

**Status:** `production_census_full_cala_15k_shell_format_source_brand_backfill_v1_complete`  
**Objective:** `full-cala-15k-shell-format-source-brand-backfill-v1`  
**Generated:** 2026-08-09T16:55:02.493Z  
**Dry run:** false  
**Airtable writes:** **1194**

## Scope
- Countries: Dominican Republic, Costa Rica, Panama
- Shell records reviewed: **1194**
- Records updated: **1194**

## Canonical Property Name
- Fixed: **562**

## Source / provenance
- Provenance writes: **1194**

## Candidate brand
- Candidate Brand Text: **83**
- Candidate Brand Family: **0**
- Candidate Brand Source: **83**
- Brand Validation Status: **83**
- Current Brand writes: **0** (must be 0)
- Brand Family writes: **0** (must be 0)

## Family / Source
{
  "field_name_requested": "Family / Source",
  "field_name_actual": "Family / Source Family",
  "type": "singleSelect",
  "semantics": "Brand / source-family affiliation options (IHG, Hilton, Marriott, Independent, etc.) — NOT discovery provenance.",
  "current_usage": "Used as Brand Family / parent-company family signal alongside Brand Family in Census Autopilot.",
  "recommended_handling": "Do not backfill from unvalidated Cvent/HBX chain text. Use Candidate Brand Family for unvalidated signals. Leave Family / Source Family blank until validated.",
  "backfill_this_mission": false
}

## Schema
- Created: **14**
- Missing: **0**


## Confirmations
- Hotel Property Census only: **true**
- No Current Brand from unvalidated Cvent: **true**
- Shells remain Hold / HR Required: **true**
- Restricted fields untouched: **true**
