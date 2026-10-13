# C:\Dev regeneratable space audit (READ-ONLY)

Generated: 2026-10-01T19:55:26.0280578+02:00
**ZERO files changed/deleted/moved** (reports written only).

## Totals

| Metric | GB |
|--------|----|
| C:\Dev total | 101.08 |
| All node_modules | 4.66 |
| Cache/generated candidates | 1.51 |
| Backup roots total | 46.67 |
| Backup overlap estimate | 3.29 |
| Shared .git | 4.00 |
| **A. NODE_MODULES potential** | **4.48** |
| **B. CACHE/GENERATED (C:\Dev only)** | **0.32** |
| B+. AppData Playwright browsers (outside C:\Dev) | 1.18 |
| **C. BACKUP consolidation potential** | **3.29** |
| **D. TOTAL LOW-RISK C:\Dev (A+B)** | **4.80** |
| D+. including AppData Playwright | 5.99 |

## Top 10 disk consumers (C:\Dev top-level)

| Folder | GB |
|--------|----|
| Backup-Staging | 18.32 |
| Backup-Scripts | 13.78 |
| deal-capture-proxy | 11.09 |
| dealality-backups | 5.93 |
| Cursor-Recovery-Archive-2026-09-08 | 5.14 |
| deal-capture-proxy-census | 3.76 |
| Nightly-Backups | 3.49 |
| deal-capture-proxy-deploy-fd919a1 | 2.86 |
| deal-capture-proxy-main-deploy | 2.83 |
| deal-capture-proxy-v92-deploy | 2.83 |

## 1. node_modules

| Parent | GB | Files | Lockfile | Worktree | Dirty | Class |
|--------|----|-------|----------|----------|-------|-------|
| rail-explore | 1.17 | 56542 | True | False | False | VERY LOW RISK TO CLEAN LATER |
| matcha | 0.71 | 29618 | True | False | True | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy-census | 0.36 | 16343 | True | True | True | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy-v92-deploy | 0.32 | 12406 | True | True | True | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy-deploy-fd919a1 | 0.32 | 12406 | True | True | True | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy-hotfix-hi-share | 0.32 | 12406 | True | True | True | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy-main-deploy | 0.32 | 12406 | True | True | True | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy | 0.32 | 12406 | True | False | True | VERY LOW RISK TO CLEAN LATER |
| dealality-fairfield | 0.32 | 11953 | True | True | False | VERY LOW RISK TO CLEAN LATER |
| deal-capture-proxy-v92-bpp-evidence | 0.31 | 11233 | True | True | True | VERY LOW RISK TO CLEAN LATER |
| .next | 0.07 | 106 | False | False | True | LOW RISK BUT VERIFY FIRST |
| standalone | 0.06 | 1928 | False | False | False | LOW RISK BUT VERIFY FIRST |
| default | 0.05 | 1309 | False | False | False | LOW RISK BUT VERIFY FIRST |
| railway | 0.00 | 618 | True | False | True | VERY LOW RISK TO CLEAN LATER |
| parallel-sdk-probe | 0.00 | 482 | False | True | True | LOW RISK BUT VERIFY FIRST |

Likely duplicate trees: worktree node_modules under deal-capture-proxy-* / dealality-* share the same package-lock lineage as primary.

## 2. Cache / generated

| Path | GB | Confidence | Class | Why |
|------|----|------------|-------|-----|
| `C:\Users\joand\AppData\Local\ms-playwright` | 1.18 | HIGH | VERY LOW RISK TO CLEAN LATER | Playwright browser binaries - reinstall via npx playwright install |
| `C:\Dev\deal-capture-proxy\matcha\.next` | 0.09 | MEDIUM | LOW RISK BUT VERIFY FIRST | Build output - regenerable if build pipeline exists |
| `C:\Dev\deal-capture-proxy\tmp` | 0.08 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy\rail-explore\.next` | 0.08 | MEDIUM | LOW RISK BUT VERIFY FIRST | Build output - regenerable if build pipeline exists |
| `C:\Dev\dealality-backups\logs` | 0.01 | HIGH | VERY LOW RISK TO CLEAN LATER | Log files typically disposable |
| `C:\Dev\deal-capture-proxy-census\tmp` | 0.01 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-main-deploy\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-gdi-restore-20260915\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-deploy-monterrey\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-deploy-main\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-hotfix-hi-share\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-v92-bpp-evidence\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-v91-release\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-v92-deploy\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-adp-parity-deploy\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-adp-atomic-deploy\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\dealality-local-task-runner\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-deploy-fd919a1\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\deal-capture-proxy-deploy-cala-six\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\dealality-fairfield\tmp` | 0.00 | MEDIUM | LOW RISK BUT VERIFY FIRST | Temp folder - usually disposable but may hold recovery scratch |
| `C:\Dev\Nightly-Backups\Logs` | 0.00 | HIGH | VERY LOW RISK TO CLEAN LATER | Log files typically disposable |
| `C:\Dev\deal-capture-proxy\rail-explore\playwright-report` | 0.00 | HIGH | VERY LOW RISK TO CLEAN LATER | Test/report output regenerable via npm test / Playwright |
| ``C:\Dev\deal-capture-proxy\.git\logs`` | 0.00 | LOW | DO NOT TOUCH | Git logs - not disposable |
| `C:\Dev\deal-capture-proxy\rail-explore\test-results` | 0.00 | HIGH | VERY LOW RISK TO CLEAN LATER | Test/report output regenerable via npm test / Playwright |

## 3. Worktrees

| Worktree | Total GB | excl nm GB | nm GB | nm% | data GB | Dirty | M/U | Unique commits |
|----------|----------|------------|-------|-----|---------|-------|-----|----------------|
| deal-capture-proxy | 11.09 | 10.77 | 0.32 | 2.9% | 1.97 | dirty | 35/1120 | 0 |
| deal-capture-proxy-census | 3.76 | 3.40 | 0.36 | 9.6% | 1.48 | dirty | 89/2311 | 1 |
| deal-capture-proxy-deploy-fd919a1 | 2.86 | 2.54 | 0.32 | 11.2% | 1.07 | dirty | 1/5 | 0 |
| deal-capture-proxy-main-deploy | 2.83 | 2.51 | 0.32 | 11.4% | 1.06 | dirty | 4/9 | 36 |
| deal-capture-proxy-v92-deploy | 2.83 | 2.51 | 0.32 | 11.4% | 1.06 | dirty | 0/3 | 23 |
| dealality-fairfield | 2.83 | 2.51 | 0.32 | 11.3% | 1.06 | clean | 0/0 | 47 |
| deal-capture-proxy-hotfix-hi-share | 2.81 | 2.49 | 0.32 | 11.4% | 1.06 | dirty | 0/6 | 18 |
| deal-capture-proxy-v92-bpp-evidence | 2.81 | 2.51 | 0.31 | 11% | 1.06 | dirty | 0/2 | 20 |
| dealality-local-task-runner | 2.51 | 2.51 | 0.00 | 0% | 1.06 | dirty | 4/14 | 38 |
| deal-capture-proxy-v91-release | 2.49 | 2.49 | 0.00 | 0% | 1.06 | dirty | 2/0 | 3 |
| deal-capture-proxy-gdi-restore-20260915 | 2.49 | 2.49 | 0.00 | 0% | 1.06 | dirty | 6/115 | 0 |
| deal-capture-proxy-adp-atomic-deploy | 2.45 | 2.45 | 0.00 | 0% | 1.03 | dirty | 2/1 | 1 |
| deal-capture-proxy-deploy-monterrey | 2.45 | 2.45 | 0.00 | 0% | 1.03 | dirty | 1/0 | 0 |
| deal-capture-proxy-adp-parity-deploy | 2.45 | 2.45 | 0.00 | 0% | 1.03 | clean | 0/0 | 2 |
| deal-capture-proxy-deploy-main | 2.45 | 2.45 | 0.00 | 0% | 1.03 | clean | 0/0 | 0 |
| deal-capture-proxy-deploy-cala-six | 2.45 | 2.45 | 0.00 | 0% | 1.03 | clean | 0/0 | 0 |

## 4. Backup overlap

| Root | GB | Newest | Archive? | Class | Note |
|------|----|--------|----------|-------|------|
| `C:\Dev\Backup-Staging` | 18.32 | 2026-10-01_0200 (2026-10-01) | True | KEEP UNTIL GOOGLE DRIVE BACKUP VERIFIED | Contains unique pre-crash monthly-review archive used 2026-10-01 ADP recovery; treat as KEEP until cloud verified |
| `C:\Dev\Backup-Scripts` | 13.78 | dealality-snapshots (2026-10-01) | False | DO NOT TOUCH | Contains Backup-Dealality.ps1 + dealality-snapshots + safety snaps; referenced by scheduled task - DO NOT TOUCH |
| `C:\Dev\dealality-backups` | 5.93 | LATEST (2026-10-01) | False | KEEP UNTIL GOOGLE DRIVE BACKUP VERIFIED | Canonical local rolling LATEST (dual backup design); may lack archive if snapshotted post-loss |
| `C:\Dev\Cursor-Recovery-Archive-2026-09-08` | 5.14 | working-state-20260908-170517 (2026-09-08) | False | BACKUP CONSOLIDATION CANDIDATE | Cursor recovery archive 2026-09-08 - verify unrecovered unique files before any consolidation |
| `C:\Dev\Nightly-Backups` | 3.49 | 2026-07-30_1242 (2026-07-30) | False | BACKUP CONSOLIDATION CANDIDATE | Older nightly root - compare to Staging/LATEST before considering consolidation |

### Backup questions

A. **Backup-Staging unique pre-crash material?** YES - used to restore ADP monthly-review archive (KEEP).
B. **Cursor-Recovery unique unrecovered?** UNKNOWN without file-level diff - CONSOLIDATION CANDIDATE after verify.
C. **Nightly-Backups unique?** Likely older overlap - CONSOLIDATION CANDIDATE after compare to Staging/LATEST.
D. **dealality-backups\LATEST canonical rolling?** YES per Backup-Dealality.ps1 design.

## 5. Top large files (excl .git objects / node_modules)

| Size MB | Class | Path |
|---------|-------|------|
| 1,662.23 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\git-safety\dealality-all-refs.bundle` |
| 997.42 | BACKUP | `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\_railway-lean-deploy.zip` |
| 997.42 | BACKUP | `C:\Dev\dealality-backups\LATEST\deal-capture-proxy\_railway-lean-deploy.zip` |
| 997.42 | ARCHIVE | `C:\Dev\deal-capture-proxy\_railway-lean-deploy.zip` |
| 997.42 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\pre-crash-recovery-freeze-20261001-175451\tree\_railway-lean-deploy.zip` |
| 997.42 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\startup-recovery-protect-20261001-155048\tree\_railway-lean-deploy.zip` |
| 997.42 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\pre-crash-recovery-freeze-20261001-175055\tree\_railway-lean-deploy.zip` |
| 967.44 | LOCAL DATA | `C:\Dev\data\libpostal\libpostal\address_parser\address_parser_crf.dat` |
| 757.63 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip\Brand AI Visisbility__79a17a03.jsonl.gz` |
| 623.19 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip\Hotel_Owner Explorer Research__ed895dbe.jsonl.gz` |
| 592.79 | LOCAL DATA | `C:\Dev\data\libpostal\libpostal\address_parser\address_parser_postal_codes.dat` |
| 590.03 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip\Dealality Census__b62b1eb7.jsonl.gz` |
| 308.67 | BACKUP | `C:\Dev\Backup-Staging\2026-08-22_0200\deal-capture-proxy\.git\cursor\crepe\6859fa45b429f579f010a7e10f8bb9c65ba573d1\postings.bin` |
| 308.67 | BACKUP | `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\.git\cursor\crepe\6859fa45b429f579f010a7e10f8bb9c65ba573d1\postings.bin` |
| 308.67 | UNKNOWN | `C:\Dev\deal-capture-proxy\.git\cursor\crepe\6859fa45b429f579f010a7e10f8bb9c65ba573d1\postings.bin` |
| 281.06 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip\Old Home problem images__2db4745d.jsonl.gz` |
| 257.32 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip\AI Product Builder Workflow__763ccb64.jsonl.gz` |
| 247.98 | BACKUP | `C:\Dev\dealality-backups\LATEST\deal-capture-proxy\data\inegi-denue\denue_09_extract\conjunto_de_datos\denue_inegi_09_.csv` |
| 247.98 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\startup-recovery-protect-20261001-155048\tree\data\inegi-denue\denue_09_extract\conjunto_de_datos\denue_inegi_09_.csv` |
| 247.98 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\pre-crash-recovery-freeze-20261001-175055\tree\data\inegi-denue\denue_09_extract\conjunto_de_datos\denue_inegi_09_.csv` |
| 247.98 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\pre-crash-recovery-freeze-20261001-175451\tree\data\inegi-denue\denue_09_extract\conjunto_de_datos\denue_inegi_09_.csv` |
| 247.98 | LOCAL DATA | `C:\Dev\deal-capture-proxy\data\inegi-denue\denue_09_extract\conjunto_de_datos\denue_inegi_09_.csv` |
| 247.98 | BACKUP | `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\data\inegi-denue\denue_09_extract\conjunto_de_datos\denue_inegi_09_.csv` |
| 204.66 | LOCAL DATA | `C:\Dev\deal-capture-proxy\data\inegi-denue\denue_14_extract\conjunto_de_datos\denue_inegi_14_.csv` |
| 204.66 | BACKUP | `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\data\inegi-denue\denue_14_extract\conjunto_de_datos\denue_inegi_14_.csv` |
| 204.66 | BACKUP | `C:\Dev\dealality-backups\LATEST\deal-capture-proxy\data\inegi-denue\denue_14_extract\conjunto_de_datos\denue_inegi_14_.csv` |
| 204.66 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\pre-crash-recovery-freeze-20261001-175451\tree\data\inegi-denue\denue_14_extract\conjunto_de_datos\denue_inegi_14_.csv` |
| 204.66 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\pre-crash-recovery-freeze-20261001-175055\tree\data\inegi-denue\denue_14_extract\conjunto_de_datos\denue_inegi_14_.csv` |
| 204.66 | BACKUP | `C:\Dev\Backup-Scripts\dealality-snapshots\startup-recovery-protect-20261001-155048\tree\data\inegi-denue\denue_14_extract\conjunto_de_datos\denue_inegi_14_.csv` |
| 181.43 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chat-raw-gzip\Brand Explorer Buildout__2aaaeadf.jsonl.gz` |
| 172.42 | UNKNOWN | `C:\Dev\deal-capture-proxy\.git\cursor\crepe\6859fa45b429f579f010a7e10f8bb9c65ba573d1\index.bin` |
| 172.42 | BACKUP | `C:\Dev\Backup-Staging\2026-08-22_0200\deal-capture-proxy\.git\cursor\crepe\6859fa45b429f579f010a7e10f8bb9c65ba573d1\index.bin` |
| 172.42 | BACKUP | `C:\Dev\Backup-Staging\2026-10-01_0200\deal-capture-proxy\.git\cursor\crepe\6859fa45b429f579f010a7e10f8bb9c65ba573d1\index.bin` |
| 131.12 | LOCAL DATA | `C:\Dev\data\libpostal\libpostal\address_parser\address_parser_phrases.dat` |
| 96.70 | BACKUP | `C:\Dev\Cursor-Recovery-Archive-2026-09-08\chats\Brand AI Visisbility__79a17a03.txt` |
| 94.62 | LOCAL DATA | `C:\Dev\data\libpostal\libpostal\address_parser\address_parser_vocab.trie` |
| 91.63 | BACKUP | `C:\Dev\Nightly-Backups\2026-07-30_1242\Cursor-workspaceStorage\f9da20dda2b4f4147468be6368bd6411\state.vscdb.backup` |
| 91.63 | BACKUP | `C:\Dev\Nightly-Backups\2026-07-30_1242\Cursor-workspaceStorage\f9da20dda2b4f4147468be6368bd6411\state.vscdb` |
| 91.63 | BACKUP | `C:\Dev\Backup-Staging\2026-08-22_0200\Cursor-workspaceStorage\f9da20dda2b4f4147468be6368bd6411\state.vscdb` |
| 91.63 | BACKUP | `C:\Dev\Backup-Staging\2026-10-01_0200\Cursor-workspaceStorage\f9da20dda2b4f4147468be6368bd6411\state.vscdb` |

Full top 100: `reports/c-dev-large-files.csv`

## 6. Git storage

- Common dir: `C:\Dev\deal-capture-proxy\.git`
- Total: 4.00 GB
- Pack: 0.44 GB
- Objects tree: 3.44 GB
- LFS: 0.00 GB (present=False)
- git gc --optional may reclaim loose/unreachable objects; NOT run (read-only). Worktrees share this object store.

## 7. Lowest-risk cleanup opportunities

1. **Reproducible node_modules** across primary + worktrees (~4.48 GB) - restore with `npm ci` per worktree needed. Largest: `rail-explore` 1.17 GB, `matcha` 0.71 GB, then ~0.32 GB duplicated across 7+ worktrees.
2. **C:\Dev caches/tmp/.next** (~0.32 GB) - regenerable. Separate: AppData `ms-playwright` browsers ~1.18 GB (outside C:\Dev).
3. **Backup consolidation** (Nightly + part of Cursor-Recovery) only after Google Drive verification (~3.29 GB estimate). Also ~1 GB `_railway-lean-deploy.zip` appears 6+ times across live tree + backups.
4. **C:\Dev low-risk total (A+B)** ~4.80 GB. Including AppData Playwright ~5.99 GB.

### DO NOT TOUCH
- All linked worktree working trees (unique dirty/untracked state)
- `Backup-Scripts` (scheduled task)
- `Backup-Staging` until cloud backup verified (unique pre-crash archive source)
- `reports/.../monthly-review/archive` (customer artifacts - backup, don't delete as 'cache')
- Fixtures, local DBs, research data

---
READY FOR JOAN REVIEW - C:\\DEV REGENERATABLE SPACE AUDIT COMPLETE

