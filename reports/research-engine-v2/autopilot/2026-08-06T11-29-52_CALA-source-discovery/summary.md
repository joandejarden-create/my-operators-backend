# Hotel Property Census — CALA Discovery Summary

Matched Active / Live Brand Setup brands to production Hotel Property Census records.

- **Status:** `production_census_cala_discovery_mode_ready_needs_source_adapter`
- **Active brands searched:** 1
- **Parent companies searched:** Preferred Hotels & Resorts
- **Countries covered (ready adapters):** Antigua and Barbuda, Argentina, Aruba, Bahamas, Barbados, Belize, Bonaire, Brazil, British Virgin Islands, Cayman Islands, Chile, Colombia, Costa Rica, Cuba, Curaçao, Dominica, Dominican Republic, Ecuador, El Salvador, Grenada, Guadeloupe, Guatemala, Haiti, Honduras, Jamaica, Martinique, Mexico, Nicaragua, Panama, Peru, Puerto Rico, Saint Kitts and Nevis, Saint Lucia, Saint Vincent and the Grenadines, Trinidad and Tobago, Turks & Caicos, U.S. Virgin Islands, Uruguay
- **Discovered properties:** 61
- **Existing Hotel Property Census matches:** 0
- **New property candidates:** 61
- **Duplicate risks:** 0
- **Steward review cases:** 0
- **Source families used:** Preferred, VIC_evidence
- **Blocked source families:** (none)
- **Marriott countries searched:** (none)
- **Marriott HQV required for discovery:** false
- **Marriott MARSHA coverage:** 0/0
- **Estimated insert count if applied:** 61
- **Airtable writes:** false

## Recommended apply command

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
  CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
  CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
  CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
  npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 100 \
  --approval-bundle reports/research-engine-v2/autopilot/2026-08-06T11-29-52_CALA-source-discovery/approval-bundle.json \
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
