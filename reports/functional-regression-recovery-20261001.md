# Functional Regression Recovery — 2026-10-01

**Branch:** `cursor/local-system-startup-recovery`  
**HEAD before this fix:** `1f15440`  
**Protect snapshot:** `C:\Dev\Backup-Scripts\dealality-snapshots\startup-recovery-protect-20261001-155048`  
(meta + full tree excl. `node_modules` / `.git` / large runtime caches; ~42k files)

## Root cause

Startup recovery commits (`d272564`, `1f15440`) did **not** overwrite Brand AI or AI Demand Admin product sources.

The real regression for **AI Demand Admin**:

HTML (`public/app/admin/ai-demand-admin.html`) referenced assets that were **never git-tracked** and were **missing from the working tree**:

| Asset | Status before fix | Source restored |
|-------|-------------------|-----------------|
| `public/js/admin-ai-demand-reviews.js` | MISSING (404) | `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy` (36608 bytes; newest) |
| `public/css/admin-ai-demand-reviews.css` | MISSING (404) | Same backup (5443 bytes) |
| `public/css/admin-ai-demand-admin.css` | MISSING (404) | Same backup (625 bytes) |

Without `admin-ai-demand-reviews.js`, the Reviews tab (filters, table, generate/preview flows) was dead — “significant functionality lost” while the shell HTML still loaded.

**Brand AI Intelligence** (`/ai-visibility`) and **AI Demand Positioning** (`/owner-ai-demand.html`) core files matched the Oct 1 backup byte-for-byte before this pass; pages load with full section chrome. Data panels require auth (expected).

## d272564 restore classification (product-relevant)

| Class | Finding |
|-------|---------|
| SAFE RESTORE | Added never-committed runtime libs for boot (market-alerts-contact, CI, Surfe, etc.). Did **not** modify ADP/Brand AI tracked UI. |
| OUTDATED RESTORE | Not applied to Brand AI / ADP admin product files (hashes equal `fd919a1`). |
| PARTIAL RESTORE | Market Alerts `orchestrator.js` (fixed in `1f15440`) — unrelated to ADP/Brand AI UI. |
| UNKNOWN | N/A for these products |

## Source map (key files)

| FILE | CURRENT | LAST KNOWN GOOD | SOURCE | ACTION |
|------|---------|-----------------|--------|--------|
| `public/js/admin-ai-demand-reviews.js` | restored 36608 | Backup 2026-10-01 36608 | Backup-Staging | RESTORED + track |
| `public/css/admin-ai-demand-reviews.css` | restored 5443 | Backup 2026-10-01 | Backup-Staging | RESTORED + track |
| `public/css/admin-ai-demand-admin.css` | restored 625 | Backup 2026-10-01 | Backup-Staging | RESTORED + track |
| `public/app/admin/ai-demand-admin.html` | = fd919a1 / bak | bak | already present | NONE |
| `public/js/admin-ai-demand-admin.js` | = bak | bak | already present | NONE |
| `public/js/admin-gdi-reports.js` | = bak | bak | already present | NONE |
| `public/js/admin-adp-action-plan.js` | = bak | bak | already present | NONE |
| `public/ai-visibility-brand.html` + `js/ai-visibility/*` | = bak | bak | already present | NONE |
| `public/owner-ai-demand.html` + `js/ai-demand-positioning/*` | = bak | bak | already present | NONE |

## Startup fixes retained

All prior startup recovery modules remain; import graph `missingCount: 0`; `npm start` healthy.

## Verification

| Check | Result |
|-------|--------|
| npm start | PASS |
| /health | PASS |
| import graph | 0 missing |
| Brand AI Intelligence UI | PASS (shell + filters + sections; loading data needs auth) |
| AI Demand Positioning UI | PASS (property select + full section structure) |
| AI Demand Admin assets | PASS (reviews.js/css + admin.css HTTP 200) |
| AI Demand Admin page | PASS structure present; gate shows auth-required without Memberstack (expected) |
| Brand Explorer | PASS |
| Market Alerts | PASS |
| Hotel Intelligence templates | PASS |

## Still missing / expected limitations

- Admin gate requires signed-in Dealality admin (no Surfe/Airtable writes performed to bypass).
- Brand AI / ADP report bodies need auth + published data — not a restore defect.

## Safety

Surfe: 0 · Production Airtable writes: 0 · No merge/deploy/push
