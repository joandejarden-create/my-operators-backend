# Test results

## Unit tests

```bash
npm run test:gdi-airtable-research-table-and-initial-backfill-v1
npm run test:gdi-all-hotel-airtable-persistence-completion-v1
```

**Result: PASS**

## Production

| Check | Result |
|-------|--------|
| Schema create | OK — \`tblbkDj0vhQLfrssV\` |
| Exact 85 allowlist | OK (42 + 43) |
| Creates | 85 |
| Failures | 0 |
| YOTEL research inventory | 42 |
| Guadalajara research inventory | 43 |
| GDO writes | 0 |
| Publication writes | 0 |
| Qualified FS vs GDO | 55/55 present, 0 absent |

## Idempotency recheck (dry-run)

85 × \`WOULD_UPDATE\` · 0 creates · 0 failures

## CI precheck

**PASS** (\`npm run dealality:ci-precheck\`)
