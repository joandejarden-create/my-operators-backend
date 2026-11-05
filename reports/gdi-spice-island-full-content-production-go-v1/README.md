# Spice Island Full-Content Production GO V1

**Status:** `FOUNDER_GO_REQUIRED_FOR_DEPLOYMENT`  
**Hotel:** Spice Island Beach Resort (`recKRJjcPnb4tVDDS`)  
**No production writes / merges / deploys performed.**

## Gate summary

| Check | Result |
|-------|--------|
| Gitleaks (PR #136 CI) | Still FAIL until allowlist commit is pushed |
| Gitleaks finding class | **False positive** — `staged_*` ADP publication artifact ID, not a live credential |
| Rotation required | **No** |
| PR #136 merge ready | **No** — gitleaks red + V2 contract not on PR branch |
| Deployment-source parity | **FAIL** — main auto-deploy can wipe Railway lean recovery; V2 not on remote PR |
| Seven V2 snapshots (local) | **PASS** (1 Sales Ready / 6 Account Development) |
| Hangers drawer (local registry-only) | **PASS** — full commercial body, Khaleen + Aviann, 7 facts |
| Bethesda / Waterstone | **PASS** — live 30 / 28 |
| Production Spice share | Still lean until GO deploy + volume migration |

## Blockers before FOUNDER GO

1. Commit + push to PR #136: scoped `.gitleaks.toml` allowlist for `staged_*` + V2 full-content contract + share sanitize / publication gate / presentation dedupe.
2. Confirm Secret scan and required PR checks are green.
3. Merge PR #136 **or** pin Railway deploy source so GitHub `main` cannot revert the registry-capable build.
4. Then authorize production volume snapshot migration (seven IDs only).

## Artifacts

- `00-executive-verdict.json`
- `01-gitleaks-status.json`
- `02-deployment-source-parity.json`
- `03-seven-v2-snapshot-validation.json`
- `04-full-drawer-parity.json`
- `05-bethesda-waterstone-regression.json`
- `06-production-migration-plan.json` (dry plan only)
- `07-exact-production-repair-sequence.json`
- `08-rollback-plan.json`
- `09-pr136-merge-readiness.json`

## Commands

```bash
node scripts/test-gdi-publication-full-content-contract-v1.mjs
node scripts/gdi-production-publication-full-content-parity-v1.mjs
node scripts/gdi-spice-island-full-content-production-go-preflight-v1.mjs
```
