# Test results — YOTEL replacement preparation

## Deterministic suites

| Suite | Result |
|---|---|
| `npm run test:gdi-yotel-replacement-migration-plan-v1` | PASS (13 assertions) |
| `npm run test:gdi-multi-hotel-airtable-reconciliation-v2` | PASS (20 assertions) |
| `npm run dealality:ci-precheck` | PASS (at preparation commit) |

Covered: hotel-ID scoping, YOTEL-only delete identification, cross-hotel reject, backup PII redaction, local baseline identity, qualified vs research-only, dependency preserve policy, staged non-atomic sequence, fail-closed mutation guard, no publication mutation.

## Live verification (corrective turn)

| Check | Result |
|---|---|
| YOTEL Airtable GDO count | 108 |
| Manifest vs live opportunity IDs | exact match |
| Production deletions/creates | none |
| Local opportunities.json count | 42 |
