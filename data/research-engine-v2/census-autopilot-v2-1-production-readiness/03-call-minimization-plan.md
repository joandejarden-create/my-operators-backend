# SerpApi Call Minimization Plan

## Goal
Minimize paid calls while maximizing independent Census completion. **Do not** maximize quota use.

## Old forecast problem
~14301 ≈ nearly one search per candidate / inflated new×1.15 — overstates need.

## WHY calls were proposed
- **A**: New hotel independent confirmation
- **B**: Existing hotel identity confirmation
- **C**: Address gap
- **D**: Coordinate gap
- **E**: Phone gap
- **F**: Website gap
- **G**: Amenities gap
- **H**: Property type/class input
- **I**: Contradiction/freshness validation
- **J**: Other

## Avoidance
- skip_insufficient_identity: 30
- skip_identity_conflict_until_review: 278
- skip_probable_duplicate_second_call: 353
- skip_existing_verified_rooms_only_gaps: 657
- official_first_native_strong: 357
- pid_dedupe_vs_per_candidate: 528
- dealality_cache_rerun_savings_pct: 40
- field_specific_not_blanket: 1218

## Revised forecast
- Confirmation: **12047**
- Existing gaps: **329**
- **Total: 12376**
- Saved vs old: **1925 (13.5%)**

## Rules
1. Dedupe to property_identity_id before paid call
2. Official-first for native-strong brands
3. Skip Rooms-only gaps (not SerpApi)
4. Skip insufficient / conflict until identity repair
5. Dealality research cache on every lookup
6. Field-specific routing — not blanket enrichment
