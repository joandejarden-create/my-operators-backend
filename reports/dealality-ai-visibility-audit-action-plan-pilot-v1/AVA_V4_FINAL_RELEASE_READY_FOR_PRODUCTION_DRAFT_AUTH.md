# AVA V4 — Final Release Ready for Production Draft Auth

**Verdict:** `AVA_V4_FINAL_RELEASE_READY_FOR_PRODUCTION_DRAFT_AUTH`

## PR / HEAD

| Item | Value |
|---|---|
| PR | **#145** — https://github.com/joandejarden-create/my-operators-backend/pull/145 |
| Branch | `release/ava-v4-final-customer-quality-v1` |
| HEAD SHA | *(see git / PR tip; packaging commits on this branch)* |
| Certified quality revision | `514ec0ce04a71434d1b217eb1507ce2346317e3e` |
| Base | current `origin/main` |

## CI / security

| Gate | Result |
|---|---|
| AVA production release preflight | **PASS** (`AVA_V4_RELEASE_READY_FOR_FOUNDER_DEPLOY_AUTH`) |
| Customer quality standard tests | **PASS** |
| PDF channel parity | **PASS** (published ≠ draft; draft looks V4) |
| Durable storage gates | **PASS** |
| 38-hotel manifest hash verify | **38/38** |
| `assert:production-assets` | **PASS** (incl. `AI_VISIBILITY_AUDIT`) |
| Clean checkout server start | **PASS** (Admin HTML **200** with local `.env`) |
| Secret scan (gitleaks) | Runs on PR via `.github/workflows/secret-scan-pr.yml` |

## 38/38 artifact hashes

`AVA_38_HOTEL_FINAL_MANIFEST.json` synced to certified regeneration SHAs.

`node scripts/ava-v4-verify-38-manifest-v1.mjs` → `matched=38`, `mismatches=0`.

## Production draft-replacement dry-run

```text
AVA_STORAGE_MODE=local NODE_ENV=production \
  AVA_STORAGE_ROOT=<admin-or-sandbox> \
  node scripts/ava-v4-stage-certified-drafts-v1.mjs --dry-run
```

| Metric | Result |
|---|---|
| Hotels | **38/38** |
| Dry-run writes | **0** |
| Published PDFs would be modified | **No** (`publishedUntouched=true`) |
| Share tokens would be rotated | **No** (`shareTokensUntouched=true`) |
| Prior draft metadata captured for rollback | **Yes** (`previousDraftSha` / `previousDraftVersionId`) |

Results: `AVA_V4_STAGE_CERTIFIED_DRAFTS_RESULTS.json`

## Backup and rollback procedure

1. Before apply: script copies existing `draft.pdf` to `AVA_DRAFT_BACKUP_ROOT/<propertyId>/draft-before-<sha12>.pdf` and writes `meta-before.json`.
2. Meta stores `previousDraftPdfSha256` + `previousDraftVersionId`.
3. Rollback: restore backup over `pdfs/<propertyId>/draft.pdf`; restore prior draft SHA/version into audit meta.
4. Confirm `published.pdf` SHA still equals `publishedShaBefore`.
5. Do **not** rotate share tokens or rewrite customer links.

Authorized apply (future founder auth only):

```text
AVA_STORAGE_MODE=required AVA_STORAGE_ROOT=<prod-ava-data> NODE_ENV=production \
  node scripts/ava-v4-stage-certified-drafts-v1.mjs --authorize-production-draft-transfer
```

## Published / tokens protected

- Draft transfer writes **draft channel only** via `stageAvaPdf`.
- Dry-run confirms **no published path mutation**.
- No share-token scripts invoked; customer links unchanged.

## Explicit non-actions completed this pass

- No merge to main
- No Railway deploy
- No production draft apply
- No customer publish
- No paid research / ADP / GDI methodology changes
