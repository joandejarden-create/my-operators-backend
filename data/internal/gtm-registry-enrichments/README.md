# GTM registry enrichments (internal only)

Place completed **public registry lookups** here as JSON files. Gitignored except this README.

## Workflow

1. Generate work queue:
   ```bash
   node scripts/report-gtm-owner-registry-enrichment-queue.mjs --limit=30
   ```
2. For each owner, lookup entity + legal representative in the country registry (see `lib/gtm-owner-target/registry-contact-config.js`).
3. Save one JSON file per owner (or batch array) in this folder.
4. Import:
   ```bash
   node scripts/import-gtm-registry-contact-enrichments.mjs --dry-run
   node scripts/import-gtm-registry-contact-enrichments.mjs --apply
   node scripts/sync-gtm-owner-target-contacts.mjs --apply
   node scripts/classify-gtm-owner-target-icp.mjs --apply
   ```

## JSON shape

See `fixtures/gtm-registry-enrichment-example.json`.

Required:
- `ownerName` (must match CoStar Owner Target) or `ownerTargetId`
- `registry.system`, `registry.entityName`, `registry.verificationUrl`
- `contact.name` or `registry.legalRepresentative`

For **V1R** (strike-list eligible):
- Corporate email on entity domain
- Public proof URL saved in `registry.verificationUrl`

## Mexico — Wave 1 corporate web first (no SIGER signup)

**Do NOT register for SIGER** unless you have a Mexican CURP and need legal-rep proof for V1R.

**Default path:**
1. Corporate / IR website (e.g. `fibrainn.mx/en/corporate/management`)
2. LinkedIn — CEO, IR, Director de Desarrollo
3. Import via `node scripts/import-gtm-wave1-mx-enrichments.mjs --dry-run`

**Optional fallbacks:** SIEM CSV (datos.gob.mx), SAT RFC validator, SIGER, RNT (`MX_RNT_LOOKUP_ENABLED=1`)

See `reports/gtm-wave1-mx-outreach-plan.md` after running `node scripts/report-gtm-wave1-mx-outreach-plan.mjs`.

## Verification tiers

| Tier | Meaning |
|------|---------|
| V1 | CoStar contact export (existing) |
| V1R | Registry legal rep + corporate email + proof URL |
| V2 | Registry legal rep + LinkedIn (no verified email yet) |
| V3 | Entity resolved; contact not yet reachable |
