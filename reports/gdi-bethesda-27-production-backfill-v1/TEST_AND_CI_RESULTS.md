# Test and CI results — Bethesda 27 production backfill

## Local tests (pre-PR)

| Test | Result |
|------|--------|
| `test:gdi-bethesda-27-production-backfill-v1` | PASS |
| `test:gdi-bethesda-research-consolidation-v1` | PASS |
| `test:gdi-research-source-summary-serialization-v1` | PASS (baselineBuilt=686, bad=0) |
| `test:gdi-all-hotel-airtable-backfill-final-v1` | PASS |

## Production apply actuals

| Metric | Actual |
|--------|-------:|
| CREATED | 27 |
| Failed | 0 |
| Read-back failures | 0 |
| Post Bethesda Airtable | 86 |
| Portfolio research total | 686 |
| Idempotency WOULD_CREATE | 0 |
| Idempotency ALREADY_PRESENT | 27 |
| Canonical FS | 86 |
| Shared title/org drift retained | 24 |
| GDO writes | 0 |
| Publication writes | 0 |
| Rock Creek applied | false |
| Auto persistence enabled | false |

Authorization: `gdi-bethesda-research-inventory-27-creates-v1`
