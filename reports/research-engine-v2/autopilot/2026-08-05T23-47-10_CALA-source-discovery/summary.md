# Hotel Property Census — CALA Discovery Summary

Matched Active / Live Brand Setup brands to production Hotel Property Census records.

- **Status:** `production_census_cala_discovery_mode_ready_needs_source_adapter`
- **Active brands searched:** 62
- **Parent companies searched:** Choice, Hilton, Marriott
- **Countries covered (ready adapters):** Antigua and Barbuda, Argentina, Aruba, Bahamas, Barbados, Belize, Brazil, Cayman Islands, Chile, Colombia, Costa Rica, Curaçao, Dominican Republic, Ecuador, El Salvador, Grenada, Guatemala, Haiti, Honduras, Jamaica, Mexico, Panama, Peru, Puerto Rico, Saint Kitts and Nevis, Saint Lucia, Trinidad and Tobago, Uruguay
- **Discovered properties:** 290
- **Existing Hotel Property Census matches:** 262
- **New property candidates:** 28
- **Duplicate risks:** 0
- **Steward review cases:** 0
- **Source families used:** Hilton, Choice, Marriott, VIC_evidence
- **Blocked source families:** (none)
- **Marriott countries searched:** Mexico, Dominican Republic, Costa Rica, Colombia, Panama
- **Marriott HQV required for discovery:** false
- **Marriott MARSHA coverage:** 168/168
- **Estimated insert count if applied:** 28
- **Airtable writes:** false

## Recommended apply command

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
  CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
  CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
  CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
  npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 100 \
  --approval-bundle reports/research-engine-v2/autopilot/2026-08-05T23-47-10_CALA-source-discovery/approval-bundle.json \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --enable-production-writes
```

## Recommended next enrichment run after insert

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --run-until-complete --batch-size 250
```
