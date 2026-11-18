# Test and certification results

**Phase:** Non-mutating certification  
**Production Airtable writes:** not executed

## Suite: multi-hotel reconciliation V2

```bash
node scripts/test-gdi-multi-hotel-airtable-reconciliation-v2.mjs
```

| Result | Value |
|--------|-------|
| Status | **PASS** |
| Assertions | 20 |
| Suite | `test-gdi-multi-hotel-airtable-reconciliation-v2` |

### Assertion map (founder checklist §14)

| # | Topic | Covered |
|---|-------|---------|
| 1 | Matching FS and AT opportunity IDs | yes |
| 2 | Airtable-only historical | yes |
| 3–4 | FS-only eligible vs speculative | yes |
| 5 | Hotel ID conflicts | yes |
| 6–7 | Shared vs distinct hotel opportunity identity | yes |
| 8 | Duplicate org aliases → CONFLICT | yes |
| 9–10 | Rock Creek embedded vs structured venue IDs | yes |
| 11–12 | Idempotent rerun | yes |
| 13 | Partial failure → UNCHANGED after apply | yes |
| 14 | Weekly Change continuity | yes |
| 15 | GDO qualification (CREATE vs DEFERRED) | yes |
| 16–20 | No publication change / no unauthorized writes / privacy guards | yes |

## Live reconciliation generator (read-only)

```bash
node scripts/gdi-multi-hotel-airtable-reconciliation-v2.mjs
```

Produces/updates manifest, matrix, primary hotel MD, and per-hotel JSON under this directory. `mutation: false` enforced.

## Rock Creek venue audit

```bash
node scripts/gdi-rock-creek-venue-identity-audit-v2.mjs
```

Output: `ROCK_CREEK_IDENTITY_AUDIT.json`

## CI precheck

Run before PR: `npm run dealality:ci-precheck` (gitleaks + workflow sanity).

## Not claimed

Live multi-hotel Airtable apply certification — requires founder allowlist + post-apply read-back on the next pilot only.