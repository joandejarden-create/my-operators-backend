# Dealality Batch Learning Audit

**Status:** `dealality_batch_learning_system_ready`  
**Generated:** 2026-08-09T18:45:35.650Z  
**Process actually learned:** yes  
**Airtable writes:** false  
**Brand Explorer patches:** false

## Last batches

| Process | Batch | Date | Source |
| --- | --- | --- | --- |
| Census | production_census_schema_v114_rooms_keys_provenance | 2026-08-05 | `reports/research-engine-v2/production-census-schema-v114-rooms-keys-provenance-apply.json` |
| Brand Explorer | 62_webhound_public_tabs_batch_c_owner_facing_claims | 2026-08-05 | `reports/brand-explorer/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.json` |

## Counts

| Metric | Value |
| --- | ---: |
| Code rules | 16 |
| Validation rules | 27 |
| Source patterns | 7 |
| Fixtures flagged | 12 |
| Tests flagged | 29 |
| Unresolved Webhound | 2 |
| Unresolved steward | 3 |
| Proposed open | 11 |

## Module hooks

- OK `lib/research-engine-v2/production-census-coordinate-resolver.js`
- OK `lib/research-engine-v2/production-census-address-geocode-resolver.js`
- OK `lib/research-engine-v2/production-census-geocoding-providers.js`
- OK `lib/research-engine-v2/marriott-hqv-coordinate-client.js`
- OK `lib/partner-intelligence/brand-explorer-62-background-validation.js`
- OK `lib/data-intelligence/dealality-batch-learning-system.js`

## Source reports present

```json
{
  "census_first_pass_apply": true,
  "census_coord_resolver": true,
  "census_address_geocode": true,
  "webhound_closed": true,
  "be_background_validation": true,
  "be_safe_text_cleanup": true,
  "be_safe_text_cleanup_1A_apply": true,
  "be_safe_text_cleanup_1B_apply": true,
  "be_mgallery_quality_minor_apply": true,
  "be_62_quality_clean_freeze": true,
  "be_brand_setup_child_table_validation_62": true,
  "be_62_webhound_claim_validation": true,
  "be_62_webhound_airtable_reconciliation_v1": true,
  "be_62_webhound_claim_patch_batch_a": true,
  "be_62_webhound_public_tabs_batch_c": true
}
```

## Ledger validation

```json
{
  "ok": true,
  "errors": [],
  "entry_count": 62
}
```
