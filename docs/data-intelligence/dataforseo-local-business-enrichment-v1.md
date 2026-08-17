# DataForSEO Local Business Enrichment v1

**Status:** `production_census_dataforseo_local_business_enrichment_v1_partial_policy_decision_needed`
**Objective:** `dataforseo-local-business-enrichment-v1`
**Generated:** 2026-08-07T23:57:32.947Z
**Mode:** candidates-only (no Hotel Property Census writes / no inserts)

## Scope

- Existing records tested: **100**
- Markets tested: **9**
- Queries / tasks run: **118**
- Estimated cost: **$0.0480**

## Enrichment (existing records)

- Local/business candidates found: **79**
- High-confidence matches: **0**
- Medium-confidence matches: **0**
- Duplicate-risk matches: **10**
- Address candidates: **19**
- Phone candidates: **19**
- Website candidates: **19**
- Coordinate candidates: **19**
- Rooms evidence from Maps: **0** (not allowed)

## Discovery (pilot markets)

- New hotel candidates: **36**
- Already in census: **2**
- Possible duplicates: **10**
- Non-hotel / unsupported rejected: **9**

## Cost efficiency

- Cost per matched record (high+medium): **n/a**
- Cost per new hotel candidate: **$0.0013**
- Cost per address candidate: **$0.0025**
- Cost per phone candidate: **$0.0025**
- Cost per website candidate: **$0.0025**
- Cost per geo candidate: **$0.0025**

## Safety

- Census writes: **0**
- Inserts: **0** (candidate insert queue only)
- Brand Setup / Brand Explorer: **0**
- Rooms from Maps/local: **0**
- DataForSEO as SoT: **false**
- Field-level write flags all off

## Recommended write policy

### A. Safe to approve now (after steward spot-check)
- Website writes: wait for more match_high official-domain samples (current high=0, website=19).
- Address: keep candidate-only until match_high address sample is larger.

### B. Needs founder / legal approval
- Phone from DataForSEO / Google local (19 candidates) — secondary contact policy.
- Storing Google-derived coordinates (19 candidates) — geocode storage / terms.
- Storing Google-derived address as production SoT.

### C. Keep candidate-only
- match_medium steward queue
- duplicate_risk records
- new_hotel_candidates (no auto-insert in field-completion-only)
- category-ambiguous / hostel / apartment / VR
- Rooms / Keys from Maps (never)

## Scale estimate (full CALA)

If enrichment yield holds across ~1224 Census rows: ~1444 Maps queries, ~$0.59 estimated, ~0 high matches, ~4896 new hotel candidates if discovery expanded beyond 9 markets (order-of-magnitude only).
