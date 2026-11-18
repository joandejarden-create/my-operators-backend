# GDI Full E2E Airtable persistence adapter V1

Fail-closed adapter + founder-approved D1–D8 mapping. **No production writes.**

## Key artifacts

- `FOUNDER_EXECUTIVE_SUMMARY.md`
- `APPROVED_TARGET_STATE_MAPPING.md`
- `BETHESDA_EXACT_WRITE_MANIFEST.json`
- `YOTEL_ID_RECONCILIATION.json`
- `RADISSON_ID_RECONCILIATION.json`
- `GUADALAJARA_ID_RECONCILIATION.json`
- `MULTI_HOTEL_RECONCILIATION_SUMMARY.json`
- `SYNTHETIC_PROVISIONAL_ISOLATION.md`
- `PRODUCTION_PILOT_AUTHORIZATION_REQUEST.md`

## Code

- `lib/.../gdi-full-e2e-airtable-persistence-adapter-v1.js`
- `lib/.../gdi-full-e2e-airtable-persistence-eligibility-v1.js`
- `lib/.../gdi-synthetic-provisional-isolation-v1.js`
- Match Key identity on Need Period Opportunity Matches (contract)

## Commands

```bash
node scripts/gdi-bethesda-exact-write-manifest-v1.mjs
node scripts/gdi-multi-hotel-airtable-fs-reconciliation-v1.mjs
npm run test:gdi-full-e2e-airtable-persistence-adapter-v1
```
