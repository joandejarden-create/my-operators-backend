# Full CALA 15K — Next Shell Batch Preflight (NO WRITES)

**Status:** `production_census_full_cala_15k_shell_insert_v1_colombia_batch_2_preflight_ready`  
**Generated:** 2026-08-09T18:53:44.281Z  
**Production Census observed:** **5522**  
**Dry run / no writes:** **true**  
**NEXT_RECOMMENDED_ACTION:** `APPLY_NEXT_BATCH`

## Recommended next batch
- Country: **Colombia**
- Batch: **Colombia Batch 2**
- Expected inserts: **293**
- Proposed Current Brand writes: **0**
- Proposed Brand Family writes: **0**

## Mexico hold (do not weaken)
- Decision: **hold_enrichment_only**
- Weak holds: **1276**
- Insertable remaining under gate: **1**
- Artifact: `reports/research-engine-v2/full-cala-15k-mexico-batch-4-plan.json`

## Colombia Batch 2 evaluation
```json
{
  "potential_new": 596,
  "estimated_safely_insertable": 293,
  "hbx_backed": 998,
  "cvent_only": 363,
  "with_structured_city": 998,
  "hold_weak_identity": 303,
  "recommended": true
}
```

## Costa Rica Batch 2 evaluation
```json
{
  "potential_new": 343,
  "estimated_safely_insertable": 141,
  "hbx_backed": 721,
  "recommended": false
}
```

## Country inventory (top 20 by insertable)
| Country | Total | Exist High | Potential New | HBX-only | Cvent+HBX | Cvent-only | City | Safe insertable | Weak hold | Completed shells |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Colombia | 1361 | 680 | 596 | 737 | 261 | 363 | 998 | 293 | 303 | 500 |
| Costa Rica | 963 | 583 | 343 | 607 | 114 | 242 | 721 | 141 | 202 | 500 |
| Mexico | 4111 | 2043 | 1277 | 515 | 480 | 2468 | 1579 | 1 | 1276 | 1265 |
| Panama | 338 | 324 | 0 | 157 | 64 | 117 | 221 | 0 | 0 | 280 |
| Grenada | 26 | 2 | 23 | 0 | 0 | 26 | 0 | 0 | 23 | 0 |
| Guadeloupe | 32 | 0 | 32 | 0 | 0 | 32 | 0 | 0 | 32 | 0 |
| Chile | 362 | 41 | 311 | 0 | 0 | 362 | 0 | 0 | 311 | 0 |
| Guyana | 16 | 0 | 16 | 0 | 0 | 16 | 0 | 0 | 16 | 0 |
| Dominica | 23 | 2 | 20 | 0 | 0 | 23 | 0 | 0 | 20 | 0 |
| Haiti | 19 | 0 | 19 | 0 | 0 | 19 | 0 | 0 | 19 | 0 |
| Brazil | 5165 | 290 | 4842 | 0 | 0 | 5165 | 0 | 0 | 4840 | 0 |
| Belize | 123 | 4 | 118 | 0 | 0 | 123 | 0 | 0 | 118 | 0 |
| Bahamas | 94 | 6 | 74 | 0 | 0 | 94 | 0 | 0 | 74 | 0 |
| Dominican Republic | 610 | 586 | 0 | 331 | 90 | 189 | 421 | 0 | 0 | 416 |
| Ecuador | 105 | 15 | 89 | 0 | 0 | 105 | 0 | 0 | 89 | 0 |
| Saint Martin | 100 | 0 | 98 | 0 | 0 | 100 | 0 | 0 | 98 | 0 |
| Jamaica | 193 | 20 | 170 | 0 | 0 | 193 | 0 | 0 | 170 | 0 |
| French Guiana | 8 | 0 | 8 | 0 | 0 | 8 | 0 | 0 | 8 | 0 |
| Guatemala | 86 | 8 | 74 | 0 | 0 | 86 | 0 | 0 | 74 | 0 |
| Aruba | 42 | 8 | 32 | 0 | 0 | 42 | 0 | 0 | 32 | 0 |

## Batch preflight detail
- Candidate pool reviewed: **596**
- existing_match_high (country): **680**
- Within-plan HBX dedupe skips: **0**
- Within-plan name dedupe skips: **0**
- Classifications: {"safe_insert":293,"shell_insert_with_review":0,"hold_weak_identity":303,"existing_match_high":0,"duplicate_candidate":0,"non_hotel_or_invalid":0,"needs_manual_review":0,"insufficient_data_hold":0}
- Source mix: {"hbx_content_api":236,"cvent_candidate":57}
- HBX-only / Cvent+HBX / Cvent-only: **236** / **57** / **0**
- HBX Hotel Codes: **293**
- Candidate Brand: **53**
- Quality gate: ENABLE_CVENT_ONLY_QUALITY_GATE=1 — SAFE/REVIEW only; HBX-first priority; Cvent = identity only; no Current Brand / Brand Family; no rooms/coords/media/owner/dates

## Policy
- Cvent = candidate identity only
- No Rooms/Keys, coords, images, descriptions, facilities, owner/operator/dates, Recent Momentum
- No Current Brand / Brand Family from unvalidated / Cvent-only data
- Shells remain Census Only / Hold / HR Required if later applied
