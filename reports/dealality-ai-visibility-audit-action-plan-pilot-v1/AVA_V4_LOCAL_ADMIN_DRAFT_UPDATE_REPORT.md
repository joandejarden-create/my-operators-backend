# AVA V4 — Local Admin Draft Update (38 hotels)

**Verdict:** `AVA_V4_LOCAL_ADMIN_UPDATED_38_OF_38`  
**When:** 2026-11-06  
**Authorization:** local Admin Draft only (no deploy / publish / production)

## 1. Root cause of stale local reports

`http://localhost:8080` runs `node server.js` with:

- `AVA_STORAGE_MODE=local`
- `AVA_STORAGE_ROOT` → `C:\Dev\dealality\my-operators-backend\data\ava-data`

The customer-quality remediation regenerated and staged corrected PDFs into the **quality worktree** (`my-operators-backend-ava-v4-quality\data\ava-data` + `portfolio-v4-local-staging`), but the Admin process continued reading the **main checkout** `data\ava-data` draft channel, which still held the prior certified V4 drafts.

Preview and Download both use `GET /api/admin/ai-visibility-audit/:propertyId/pdf?channel=draft` (download adds `download=1`) against that same draft resolver — so both served the stale main-repo drafts.

## 2. Files and draft metadata updated

- Promoted from: `AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json` + `portfolio-v4-local-staging/`
- Into: `C:\Dev\dealality\my-operators-backend\data\ava-data\pdfs\<propertyId>\` (`draft.pdf`, versioned `*.pdf`, `current.pdf` alias)
- Metadata: `data\ava-data\audits\<propertyId>\AUDIT_META.json` — `draftPdfSha256`, `draftVersionId`, `previousDraftPdfSha256`, `draftTemplate`, `draftChannelSource=CERTIFIED_LOCAL_V4_MANIFEST`
- Workflow script: `scripts/ava-promote-certified-local-admin-drafts-v1.mjs`
- Convergence hook: when `AVA_ADMIN_STORAGE_ROOT` is set, final convergence auto-promotes into that Admin storage root

## 3. Hotels updated

**38 / 38** (`AVA_V4_LOCAL_ADMIN_DRAFT_PROMOTE_COMPLETE`)

## 4. Preview / Download SHA verification

HTTP against live `localhost:8080` for all 38 hotels:

- Preview body SHA = Download body SHA = corrected manifest SHA
- Channel header = `draft`
- Failures: **0**

Six required hashes confirmed over HTTP:

| Hotel | SHA-256 |
|---|---|
| Waterstone | `18e9d18ebeaf6a9203b3e45b550a9d76a972c85c865236aa24fe069db56977eb` |
| AC A Coruña | `8847fc3d15dbd4b97e07fd930f2278bb908377903106e63e164edcf70e019e65` |
| Castillo | `1812240d0f3949736cb1fc5dc8fb1dc8596693edcddb742b8724e6fc19c08030` |
| Cambridge | `63016caeff9369a7bbb8cd936dfdb6e7b7156bdf66a16d70bea05fbcb3d8ebb2` |
| Hilton | `9a9459f68f369f31ab3db494c4b16814629f1507df2492b50eaf0e41ba65659c` |
| Spice Island | `3a2a6e4908686293491dedd85e90420bdf72bc77c92a4f76caf12ee049779bc1` |

## 5. Local server status

- PID **34556** (`node server.js`) left running — no restart required (reads draft files from disk per request)
- Storage root still: `C:\Dev\dealality\my-operators-backend\data\ava-data`
- Unrelated processes not terminated

## 6. Published PDFs unchanged

Promote results: `publishedUntouched=true` for all 38. Spot-check:

- Waterstone published: `8173ca19ace9…` unchanged  
- AC A Coruña published: `7ed9c2e0925a…` unchanged  
- Castillo published: `76944045b5e5…` unchanged  

## 7. Production untouched

No Railway deploy, no production volume write, no publish, no share-token changes, no ADP/GDI changes, no paid research.

## Re-run command (future)

```powershell
$env:AVA_STORAGE_MODE='local'
$env:AVA_STORAGE_ROOT='C:\Dev\dealality\my-operators-backend\data\ava-data'
node scripts/ava-promote-certified-local-admin-drafts-v1.mjs --all-38
```

Or after portfolio regeneration:

```powershell
$env:AVA_ADMIN_STORAGE_ROOT='C:\Dev\dealality\my-operators-backend\data\ava-data'
# then run ava-portfolio-final-convergence-v1.mjs
```
