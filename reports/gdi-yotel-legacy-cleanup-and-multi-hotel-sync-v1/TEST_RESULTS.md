# Test results — YOTEL legacy cleanup

## Deterministic

| Suite | Result |
|---|---|
| `npm run test:gdi-yotel-legacy-gdo-deletion-v1` | PASS |
| `npm run test:gdi-yotel-replacement-migration-plan-v1` | PASS (prior) |
| `npm run test:gdi-multi-hotel-airtable-reconciliation-v2` | PASS (prior) |

## Live execution

| Check | Result |
|---|---|
| Preflight exact 108 allowlist | PASS |
| Deleted | 108 |
| Remaining YOTEL GDOs | 0 |
| Failed batches | 0 |
| Local research opportunities | 42 preserved |
| Off-git unredacted backup | written under `C:\Dev\dealality\_secure-backups\...` |
| Cross-hotel GDO inventories | unchanged (verified post-delete) |
| STAGED/CURRENT packages | present / untouched |
| Rock Creek write | not executed |
| General production writes | still fail-closed |
