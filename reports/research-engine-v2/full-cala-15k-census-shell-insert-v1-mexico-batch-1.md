# Full CALA 15K Census Shell Insert v1

**Status:** `production_census_full_cala_15k_shell_insert_v1_mexico_batch_1_apply_complete`  
**Objective:** `full-cala-15k-census-shell-insert-v1`  
**Generated:** 2026-08-09T17:31:30.505Z  
**Dry run:** false  
**Airtable inserts:** **500**

## Universe
- Existing Census before: **4257**
- Total staging candidates: **16364**
- Eligible shell inserts: **11932**
- Expected Census after full insert (approx): **16189**

## Classification
- `existing_match_high`: **3565**
- `probable_duplicate_hold`: **761**
- `existing_match_medium`: **99**
- `new_candidate_medium`: **10802**
- `possible_duplicate_review`: **6**
- `new_candidate_high`: **1130**
- `reject_non_hotel`: **1**

## Source contribution
- cvent_candidate: **13369**
- independent_discovery: **666**
- hbx_content_api: **2329**

## By country (eligible inserts)
- Brazil: **4861**
- Mexico: **2632**
- Argentina: **803**
- Colombia: **607**
- Costa Rica: **349**
- Chile: **317**
- Peru: **303**
- Jamaica: **172**
- Belize: **118**
- Saint Barthélemy: **108**
- Paraguay: **102**
- Saint Martin: **100**
- Sint Maarten: **100**
- Ecuador: **89**
- Bahamas: **87**
- Puerto Rico: **85**
- Guatemala: **77**
- Martinique: **64**
- Bolivia: **58**
- Turks and Caicos: **57**
- Barbados: **55**
- U.S. Virgin Islands: **51**
- Bonaire: **51**
- Honduras: **51**
- Montserrat: **50**

## First batch plan
- Country: **Mexico**
- Planned inserts: **500**
- Applied: **500**
- Country eligible (pre-cap): **2587**
- Country remaining eligible: **2087**
- Source mix (batch): {"hbx_content_api":367,"cvent_candidate":133}
- HBX codes in batch: **500**
- Census after (estimate): **4757**
- HBX field index hits: **1908**
- Plan skipped (HBX in-batch dedupe): **0**
- Plan skipped (name+country in-batch): **282**
- Stopped on error threshold: **no**

## Schema missing (shell extras)
- none

## Policy holds
- Rooms / Keys, coordinates, images, descriptions, facilities
- Owner/operator/developer/dates/Recent Momentum/Company Validated/Brand Verified
- Cvent = candidate identity only (not field-level SoT)
- Current Brand not written from chain/brand text

## Confirmations
- Hotel Property Census only: **true**
- No Brand Explorer / Brand Setup / VIC: **true**
- No duplicate inserts intended (dedupe gate): **true**
- All inserts Census Only / Hold / HR Required: **true**
- Checkpoint: `data/research-engine-v2/full-cala-15k-census-shell/full-cala-15k-checkpoint.json`
