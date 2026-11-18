# GDI multi-hotel Airtable reconciliation V2

Read-only reconciliation and dry-run sync planning after PR #367 Bethesda pilot.

**STOP FOR FOUNDER REVIEW — no production writes in this package.**

## Start here

1. [FOUNDER_EXECUTIVE_SUMMARY.md](./FOUNDER_EXECUTIVE_SUMMARY.md) — verdict + 17 acceptance answers  
2. [FOUNDER_DECISIONS_REQUIRED.md](./FOUNDER_DECISIONS_REQUIRED.md) — approval gate  
3. [MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json](./MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json) — machine totals  

## Deliverables

| File | Purpose |
|------|---------|
| FOUNDER_EXECUTIVE_SUMMARY.md | Executive verdict |
| STORAGE_AUTHORITY_AND_RECONCILIATION_CONTRACT.md | SoT rules |
| ROCK_CREEK_VENUE_IDENTITY_ANALYSIS.md | Venue ID audit |
| ROCK_CREEK_NON_DESTRUCTIVE_REMEDIATION_PLAN.md | Planned PE venue fix |
| HOTEL_INVENTORY_RECONCILIATION_MATRIX.md | Aggregate matrix |
| YOTEL / RADISSON / GUADALAJARA `*_ID_LEVEL_RECONCILIATION.md` | Primary hotel rows |
| REFERENCE_HOTEL_VALIDATION.md | Wyndham, Castillo, Bethesda, Nobu, Waterstone |
| GLOBAL_IDENTITY_AND_LINK_INTEGRITY.md | Shared demand + conflicts |
| COMMERCIAL_ELIGIBILITY_RESULTS.md | CREATE/UPDATE/DEFER totals |
| MULTI_HOTEL_DRY_RUN_SYNC_MANIFEST.json | Planner manifest |
| ROCK_CREEK_IDENTITY_AUDIT.json | Live venue audit |
| `*_ID_LEVEL_RECONCILIATION.json` | Per-hotel machine rows |
| WEEKLY_CHANGE_PRESERVATION.md | Durability contract |
| PERSISTENCE_RETRY_AND_READBACK_CERTIFICATION.md | Retry/read-back |
| TEST_RESULTS.md | Test gates |
| PRODUCTION_SYNC_ACTIVATION_PROPOSAL.md | Next pilot proposal |
| FOUNDER_DECISIONS_REQUIRED.md | Decisions RC1–RC8 |

## Regenerate (read-only)

```bash
node scripts/gdi-rock-creek-venue-identity-audit-v2.mjs
node scripts/gdi-multi-hotel-airtable-reconciliation-v2.mjs
node scripts/test-gdi-multi-hotel-airtable-reconciliation-v2.mjs
```

## Tests

```bash
node scripts/test-gdi-multi-hotel-airtable-reconciliation-v2.mjs
```