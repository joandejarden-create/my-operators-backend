# V3 Field Gap Diagnostic — Summary

**Authorized run:** `cav3_2026-08-08T15-04-05-566Z`  
**Airtable writes during diagnostic:** **NONE**  
**Artifacts:** `31-field-gap-diagnostic/`

## Plain answer

| Field | Why blank in Airtable? | Verdict |
|-------|------------------------|---------|
| **State / Region** | Never researched into V3 staging (freeze has no state; geography resolver omits it). Classifier also never `add()`s it despite AUTO_WRITE_SAFE. | **EXPECTED** (this cohort) + latent writer bug |
| **Address** | Not persisted on V2.3 discovery physical; 0/150 staging values. Classifier never `add()`s Address despite CORROBORATED_WRITE. | **EXPECTED** (this cohort) + latent writer bug |
| **Submarket** | Not uniformly blank: **46/150 written**. Remaining **104** failed Dealality corridor match (`no_corridor_match`) — often weak city labels. | **EXPECTED** for blanks; writes OK when present |
| **Latitude** | **60/150** have official Hilton/Choice coords in freeze, but Phase 1 blanket `BLOCKED_RIGHTS` suppressed them (`unless official` not implemented). 90 had no coords. | **BUG** for the 60 |
| **Longitude** | Same as Latitude. | **BUG** for the 60 |
| **Phone** | 0/150 staging values; also blanket blocked_rights. | **EXPECTED** |

## Summary table

| Field | Staging Nonblank | Written | Primary Source | Write Class | Blank Reason | Expected or Bug | Recommended Fix |
|-------|-----------------:|--------:|----------------|-------------|----------------|-----------------|-----------------|
| State / Region | 0 | 0 | none_in_staging (policy: official_or_dealality) | AUTO_WRITE_SAFE | A. RESEARCH VALUE NOT FOUND | EXPECTED | Add State/Region to classifyFieldWrites + geography derivation; research before backfill |
| Address | 0 | 0 | none_in_staging (policy: official_page_only_this_pilot) | CORROBORATED_WRITE | A. RESEARCH VALUE NOT FOUND | EXPECTED | Persist official address in discovery/enrichment; add Address to classifyFieldWrites when official evidence exists |
| Submarket | 46 | 46 | dealality_geography (proposeCensusSubmarketCorridor) | AUTO_WRITE_SAFE | A. RESEARCH VALUE NOT FOUND | EXPECTED | Improve Dealality corridor matching / city normalization for No Match cases; 46 already written OK |
| Latitude | 60 | 0 | official_brand_directory (Hilton structured / Choice directory) | BLOCKED_RIGHTS | B. VALUE EXISTS — BLOCKED RIGHTS | BUG | Fix dry-run to write official freeze coords as CORROBORATED_WRITE; run corrective blank-fill for 60 records |
| Longitude | 60 | 0 | official_brand_directory (Hilton structured / Choice directory) | BLOCKED_RIGHTS | B. VALUE EXISTS — BLOCKED RIGHTS | BUG | Fix dry-run to write official freeze coords as CORROBORATED_WRITE; run corrective blank-fill for 60 records |
| Phone | 0 | 0 | none_in_staging | BLOCKED_RIGHTS | A. RESEARCH VALUE NOT FOUND | EXPECTED | Keep blank until official phone researched or SerpApi persistence approved; add to insert allowlist only then |

## SerpApi 1,050 blocked rows

- **Formula:** 150 × 7 fields = **1,050**
- Fields: Latitude, Longitude, Phone, Amenities - Source Text, Amenities - Structured Tags, Hotel Description - Source Text, Hotel Description - AI Summary
- **Address is NOT in the 1,050**
- Lat+Lng+Phone = **450 / 1,050 (42.9%)**
- **0** of the 60 coordinate values were SerpApi-sourced — all official directory — yet still blocked

## Safe to backfill NOW (dry-run only; not applied)

- **Latitude / Longitude** for **60** records from already-approved official freeze evidence

## Must wait

- Address, Phone, State / Region (no approved staging values)
- Submarket remaining 104 (geography research, not rights)
