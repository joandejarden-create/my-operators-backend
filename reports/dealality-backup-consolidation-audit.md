# Dealality backup consolidation audit (READ-ONLY)

Generated: 2026-10-01T20:23:37.2427327+02:00
**ZERO files moved/deleted/changed.**

## Totals

| Metric | GB |
|--------|----|
| Total backup roots | 46.67 |
| Unique estimate | 15.30 |
| Redundant/overlap estimate | 31.37 |

### Root sizes

| Root | GB | Files |
|------|----|-------|
| Backup-Staging | 18.32 | 273719 |
| Backup-Scripts | 13.78 | 127749 |
| dealality-backups | 5.93 | 64741 |
| Cursor-Recovery-Archive-2026-09-08 | 5.14 | 805 |
| Nightly-Backups | 3.49 | 39285 |

## Snapshot inventory

| Root | Snapshot | GB | Files | Archive? | PDFs | Score | Cloud class |
|------|----------|----|-------|----------|------|-------|-------------|
| Backup-Staging | 2026-10-01_0200 | 11.25 | 158422 | True | 131 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Backup-Staging | 2026-08-22_0200 | 7.07 | 115297 | False | 0 | HIGH VALUE ARCHIVE | MOVE TO CLOUD ARCHIVE |
| dealality-backups | LATEST | 5.93 | 64733 | False | 0 | CRITICAL KEEP | KEEP LOCAL |
| Backup-Scripts | pre-crash-recovery-freeze-20261001-175451 | 4.60 | 42571 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Backup-Scripts | pre-crash-recovery-freeze-20261001-175055 | 4.60 | 42571 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Backup-Scripts | startup-recovery-protect-20261001-155048 | 4.58 | 42591 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Nightly-Backups | 2026-07-30_1242 | 3.49 | 39284 | False | 0 | HIGH VALUE ARCHIVE | MOVE TO CLOUD ARCHIVE |
| Cursor-Recovery-Archive-2026-09-08 | chat-raw-gzip | 2.86 | 380 | False | 0 | CRITICAL KEEP | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Cursor-Recovery-Archive-2026-09-08 | git-safety | 1.63 | 22 | False | 0 | CRITICAL KEEP | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Cursor-Recovery-Archive-2026-09-08 | chats | 0.53 | 380 | False | 0 | CRITICAL KEEP | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Cursor-Recovery-Archive-2026-09-08 | recovered-projects | 0.11 | 15 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Cursor-Recovery-Archive-2026-09-08 | working-state-20260908-170517 | 0.00 | 2 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Backup-Scripts | Backup-Dealality.ps1 | 0.00 | 1 | False | 0 | DO NOT TOUCH | KEEP LOCAL |
| Cursor-Recovery-Archive-2026-09-08 | working-state-20260908-115559 | 0.00 | 2 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Backup-Scripts | pre-restore-monthly-review-archive-20261001-194049 | 0.00 | 15 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |
| Cursor-Recovery-Archive-2026-09-08 | working-state-20260908-015812 | 0.00 | 2 | False | 0 | HIGH VALUE ARCHIVE | KEEP LOCAL UNTIL CLOUD VERIFIED |

## Staging vs LIVE monthly-review archive

- indexIdentical: **True**
- READY PDF compare: 15/15 identical
- onlyStaging: 0; onlyLive: 0; differ: 0
- LATEST has archive: **False**

## Answers A-E

### A. Backup-Staging\2026-10-01_0200 unique after restore?
YES - still holds full pre-crash monthly-review archive tree + other Staging contents; after restore, LIVE archive index/PDFs match Staging for compared READY reviews, but LATEST still lacks archive. Staging remains provenance + fallback until cloud verified.

### B. Cursor-Recovery unique?
YES - chat-raw-gzip, chats, ALL-RECOVERED-URLS.csv, git-safety bundle (~1.6GB) are not in product Git working tree or LATEST operational backup.

### C. Nightly unique?
LIKELY LITTLE product uniqueness vs newer Staging/LATEST; may contain older Cursor-workspaceStorage state from July 30. Treat as MOVE TO CLOUD ARCHIVE after verify.

### D. Is LATEST sufficient alone?
**False** - LATEST missing monthly-review archive (restored only to live working tree). After local backup refresh that includes archive + secrets policy for cloud, LATEST can be the single local operational snapshot.

### E. Cloud-only candidates
- Nightly-Backups (after cloud verify)
- Backup-Staging\2026-08-22_0200 (after cloud verify)
- Cursor-Recovery chat archives (cloud archive OK, keep local until verified)
- Backup-Staging\2026-10-01_0200 (cloud archive after verify; keep local until Drive ZIP proven)

## Overlap

| Pair | Identical est. | Unique est. | Note |
|------|----------------|-------------|------|
| Backup-Staging vs LIVE (monthly-review archive) | ~archive restored | see stagingVsLive | indexIdentical=True; pdfsIdentical=15/15 |
| Backup-Staging vs LATEST | HIGH (project trees) | 2.75 | LATEST missing archive; Staging still has full archive copy |
| Cursor-Recovery vs Git+LATEST | LOW | 4.37 | chats + git bundle unique |
| Nightly-Backups vs Staging/LATEST | 2.45 | 0.87 | July snapshot; likely superseded |
| Backup-Scripts snapshots vs LIVE | 11.02 | 1.38 | safety snaps around crash recovery |

## Unique file samples (value-path hashes)

| Backup | Snapshot | Path | Class |
|--------|----------|------|-------|
| Backup-Staging | 2026-08-22_0200 | `package.json` | B. UNIQUE OLDER VERSIONS |
| Backup-Staging | 2026-08-22_0200 | `server.js` | B. UNIQUE OLDER VERSIONS |
| Backup-Staging | 2026-10-01_0200 | `package.json` | B. UNIQUE OLDER VERSIONS |
| dealality-backups | LATEST | `package.json` | B. UNIQUE OLDER VERSIONS |
| Backup-Scripts | pre-crash-recovery-freeze-20261001-175055 | `package.json` | B. UNIQUE OLDER VERSIONS |
| Backup-Scripts | pre-crash-recovery-freeze-20261001-175451 | `package.json` | B. UNIQUE OLDER VERSIONS |
| Backup-Scripts | startup-recovery-protect-20261001-155048 | `package.json` | B. UNIQUE OLDER VERSIONS |
| Cursor-Recovery-Archive-2026-09-08 | chat-raw-gzip | `(directory)` | A. UNIQUE FILES ONLY IN THIS BACKUP |
| Cursor-Recovery-Archive-2026-09-08 | chats | `(directory)` | A. UNIQUE FILES ONLY IN THIS BACKUP |
| Cursor-Recovery-Archive-2026-09-08 | git-safety | `(directory)` | A. UNIQUE FILES ONLY IN THIS BACKUP |
| Nightly-Backups | 2026-07-30_1242 | `package.json` | B. UNIQUE OLDER VERSIONS |
| Nightly-Backups | 2026-07-30_1242 | `server.js` | B. UNIQUE OLDER VERSIONS |

## Optional cold archive packaging (NOT CREATED)

### Dealality-Nightly-Backups-2026-07-30.7z
- Source: `C:\Dev\Nightly-Backups`
- Dest: `G:\My Drive\Dealality Backups\Archive\Nightly-Backups\`
- Expected compressed: ~1.5-2.5 (est.)
- Verify: 7z t; compare file count; spot-check Cursor-workspaceStorage sample hashes

### Dealality-Backup-Staging-2026-08-22_0200.7z
- Source: `C:\Dev\Backup-Staging\2026-08-22_0200`
- Dest: `G:\My Drive\Dealality Backups\Archive\Backup-Staging\`
- Expected compressed: ~half of raw (est.)
- Verify: 7z t; confirm package.json + sample public assets

### Dealality-Cursor-Chats-2026-09-08.7z
- Source: `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip`
- Dest: `G:\My Drive\Dealality Backups\Archive\Cursor-Recovery\`
- Expected compressed: already gzip'd; modest further shrink
- Verify: list archive; open ALL-CHATS-INDEX.csv; confirm bundle hash separately

### Dealality-Backup-Staging-2026-10-01_0200-precrash.7z
- Source: `C:\Dev\Backup-Staging\2026-10-01_0200`
- Dest: `G:\My Drive\Dealality Backups\Archive\Backup-Staging\`
- Expected compressed: ~8-12 (est. from 18GB raw)
- Verify: must include monthly-review/archive/index.json + sample report.pdf hashes vs live

## Recommended architecture

- **LOCAL:** `C:\Dev\dealality-backups\LATEST` + `Backup-Scripts\Backup-Dealality.ps1`
- **KEEP LOCAL UNTIL CLOUD VERIFIED:** Staging `2026-10-01_0200`, Cursor chat/git-safety
- **CLOUD:** `G:\My Drive\Dealality Backups\` rolling ZIPs + `Archive\` for Nightly / old Staging / chats
- **GitHub:** committed source history

---
READY FOR JOAN REVIEW - BACKUP CONSOLIDATION AUDIT COMPLETE
