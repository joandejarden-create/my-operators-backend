# Hotel Property Census — CALA Discovery Summary

Matched Active / Live Brand Setup brands to production Hotel Property Census records.

- **Status:** `production_census_cala_discovery_mode_ready_needs_source_adapter`
- **Active brands searched:** 62
- **Parent companies searched:** Choice, Hilton, Marriott
- **Countries covered (ready adapters):** Mexico
- **Discovered properties:** 122
- **Existing Hotel Property Census matches:** 122
- **New property candidates:** 0
- **Duplicate risks:** 0
- **Steward review cases:** 0
- **Source families used:** Hilton, Choice, VIC_evidence
- **Blocked source families:** Marriott
- **Estimated insert count if applied:** 0
- **Airtable writes:** false

## Recommended apply command

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
  CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
  CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
  CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
  npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --queue source_discovery --run-until-complete --batch-size 100 \
  --approval-bundle reports/research-engine-v2/autopilot/2026-08-05T22-55-01_CALA-source-discovery/approval-bundle.json \
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
