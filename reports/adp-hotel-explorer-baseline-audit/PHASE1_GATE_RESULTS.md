# Phase 1 gate results (Europe/Paris context: run 2026-11-18)

## PASS (CI composite `npm run test:adp-phase1-radar-he-gates`)

| Gate | Result |
|---|---|
| `test:radar-host-hotel-explorer-wiring-v1` | PASS |
| `test:radar-hotel-explorer-progressive-enrichment-v1` | PASS |
| `assert:hotel-explorer-share-assets` | PASS |
| `test:adp-published-store-durability-v1` | PASS |
| `test:ai-demand-positioning-published-foundation` | PASS (aligned to Airtable overlay compaction) |
| `test:adp-certification-inventory-reconciliation-v1` | PASS |
| `test:hotel-explorer-share-research-parity` | PASS |

## Wired but blocked / FAIL (documented — not weakened)

| Gate | Result | Reason |
|---|---|---|
| `test:adp-permanent-full-e2e-certification-v1` | EXIT 2 | `data/adp-durable-published/**` gitignored; fixtures missing locally |
| `test:adp-e2e-persistence-contract-v1` | FAIL | Missing `PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` + remaining synthetic persist assertions after policy dry-run restore |
| `test:adp-customer-surface-contract` | FAIL | Deeper evidence/BPP/share surface failures remain after null-intent + null-profile guards (36 failures at last run) |

## Safe repo fixes applied

- BPP `classifyBppEligibility` null profile guard
- `territoryLabelForIntent` null/empty guard
- Restored `applyHistoricalRecordsToAirtable` dry-run primary-field mapping export
- Persistence policy accepts `true`/`1` and exposes fail-closed Airtable flags
- Certification baseline reconstructed from published manifests (27 properties)

## Paid cost

$0
