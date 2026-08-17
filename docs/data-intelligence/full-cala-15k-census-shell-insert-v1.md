# Full CALA 15K Census Shell Insert v1

**Status:** `production_census_full_cala_15k_shell_insert_v1_mexico_batch_3_apply_complete`  
**Objective:** `full-cala-15k-census-shell-insert-v1`  
**Generated:** 2026-08-09T18:32:36.764Z  
**Dry run:** false  
**Airtable inserts:** **265**

## Universe
- Existing Census before: **5257**
- Total staging candidates: **16364**
- Eligible shell inserts: **10749**
- Expected Census after full insert (approx): **16006**

## Classification
- `existing_match_high`: **4540**
- `probable_duplicate_hold`: **753**
- `existing_match_medium`: **132**
- `reject_non_hotel`: **185**
- `new_candidate_medium`: **10315**
- `possible_duplicate_review`: **5**
- `new_candidate_high`: **434**

## Source contribution
- cvent_candidate: **13369**
- independent_discovery: **666**
- hbx_content_api: **2329**

## By country (eligible inserts)
- Brazil: **4842**
- Mexico: **1544**
- Argentina: **794**
- Colombia: **596**
- Costa Rica: **343**
- Chile: **311**
- Peru: **303**
- Jamaica: **170**
- Belize: **118**
- Saint Barthélemy: **106**
- Paraguay: **102**
- Saint Martin: **98**
- Sint Maarten: **98**
- Ecuador: **89**
- Puerto Rico: **82**
- Bahamas: **74**
- Guatemala: **74**
- Martinique: **63**
- Bolivia: **58**
- Turks and Caicos: **57**
- Barbados: **55**
- Bonaire: **51**
- U.S. Virgin Islands: **50**
- Montserrat: **49**
- Honduras: **48**

## First batch plan
- Country: **Mexico**
- Planned inserts: **265**
- Applied: **265**
- Country eligible (pre-cap / quality-filtered): **265**
- Country eligible before quality: **1525**
- Country remaining eligible: **0**
- Source mix (batch): {"cvent_candidate":265}
- Preflight class mix (batch): {"shell_insert_with_review":265}
- HBX codes in batch: **0**
- Census after (estimate): **5522**
- HBX field index hits: **2582**
- Plan skipped (HBX in-batch dedupe): **0**
- Plan skipped (name+country in-batch): **255**
- Stopped on error threshold: **no**
- Preflight blocked: **no**

## Mexico preflight quality
- Remaining eligible reviewed: **1544**
- Insertable (safe+review): **267**
- Held/reject: **1277**
- Classifications: {"safe_shell_insert":0,"shell_insert_with_review":267,"probable_duplicate_hold":0,"weak_identity_hold":1277,"non_hotel_reject":0,"insufficient_data_hold":0}
- Source mix: {"hbx_only":0,"cvent_plus_hbx":0,"cvent_only":1544,"independent_or_multi":0}
- Field presence: {"with_city":399,"with_inferred_city":399,"with_address":0,"with_website":0,"with_phone":0,"with_candidate_brand":0,"missing_city":1145,"missing_country":0}
- Top-500 hold ratio: **0.77**
- Block reason: **none**

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
