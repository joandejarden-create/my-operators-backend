# Test results

## Unit

```bash
npm run test:gdi-all-hotel-airtable-backfill-final-v1
npm run test:gdi-airtable-research-table-and-initial-backfill-v1
```

## Live

| Check | Result |
|-------|--------|
| Initial 85 idempotency | **85 UNCHANGED** / 0 UPDATE / 0 CREATE |
| Remaining apply | **574 CREATED** / 0 failed |
| Post recon | **659 FS = 659 Airtable** · complete YES |
| Full 659 idempotency recheck | **659 UNCHANGED** |
| GDO writes | 0 |
| Publication writes | 0 |

## CI precheck

\`npm run dealality:ci-precheck\` — PASS expected at ship.
