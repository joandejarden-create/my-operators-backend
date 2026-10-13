# READY FOR JOAN REVIEW — BACKUP SIMPLIFICATION AUDIT COMPLETE

Generated: 2026-10-01 (local)  
**Mode: READ ONLY — ZERO files moved/deleted/renamed. ZERO scheduled-task changes. ZERO script rewrites in this audit.**

---

## Executive summary

Dealality already has **one active backup script** and **one active nightly scheduled task**. The preferred architecture is largely implemented in `Backup-Dealality.ps1`.

The problem is **leftover historical destinations** still consuming ~40+ GB on `C:\Dev`:

| Layer | Status |
|-------|--------|
| Active script + task | **Already canonical (1 + 1)** |
| Local LATEST | **Refreshed + ADP archive present (post-cutover)** |
| Google Drive history | **First verified ZIP exists** (`2026-10-01_2036`) |
| Historical local roots | **Still present** — Staging, Nightly, crash snaps, Cursor-Recovery |

**Active Dealality nightly backup jobs:** **1**  
**Active Dealality backup scripts:** **1**  
**Changes made this audit:** **0**

---

## 1. All backup mechanisms found

### A. ACTIVE (canonical)

| Script | Source | Destination | Schedule | Retention | Overlap |
|--------|--------|-------------|----------|-----------|---------|
| `C:\Dev\Backup-Scripts\Backup-Dealality.ps1` | `C:\Dev\deal-capture-proxy` + `%APPDATA%\Cursor\User\workspaceStorage` | Local: `C:\Dev\dealality-backups\_NEW_BACKUP_TEMP` → `LATEST`; Cloud: `G:\My Drive\Dealality Backups\{ts}\Dealality-backup.zip` | Task `Dealality Nightly Backup` daily 02:00 | Cloud: 14d daily / 8w weekly **configured**; deletes only if `DEALALITY_BACKUP_RETENTION_DELETE=1` (currently off) | Sole active writer |

### B. HISTORICAL / ORPHAN destinations (no active writer found)

| Destination | How it was created (evidence) | Still written by active script? |
|-------------|-------------------------------|----------------------------------|
| `C:\Dev\Backup-Staging\{yyyy-MM-dd_0200}` | Prior nightly design; dated `_0200` matches old 2 AM clock; includes `.git` (current script excludes `.git`) | **NO** — zero refs in `Backup-Dealality.ps1` |
| `C:\Dev\Nightly-Backups\2026-07-30_1242` | Robocopy log proves Jul 30 copy of `deal-capture-proxy` → Nightly | **NO** — no active task/script found |
| `C:\Dev\Backup-Scripts\dealality-snapshots\*` | Manual crash/pre-restore freezes (2026-10-01 recovery) | **NO** — not written by nightly script |
| `C:\Dev\Cursor-Recovery-Archive-2026-09-08` | One-time Sep 8 Cursor chat/git recovery | **NO** |
| `%USERPROFILE%\OneDrive\Dealality Backups\` | Legacy cloud target from earlier script versions; leftover dated folders exist | **NO** — current script OneDrive archive writes = **ZERO** (report label only) |

### C. Audit/helper scripts only (not backup engines)

Under `deal-capture-proxy\scripts\`: `_backup-consolidation-audit.ps1`, `_audit-backup-*.ps1`, `_gdrive-cutover-*.ps1` — analysis/cutover helpers, not scheduled.

### D. Windows system tasks (noise — not Dealality)

`MareBackup`, `AppListBackup\Backup`, `CloudRestore\Backup`, `RegIdleBackup` — OS telemetry/registry; **not** Dealality.

---

## 2. Windows Task Scheduler

### Dealality-related

| Task | Command | Schedule | Enabled | Destination | Duplicate? |
|------|---------|----------|---------|-------------|------------|
| **Dealality Nightly Backup** | `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Dev\Backup-Scripts\Backup-Dealality.ps1"` | Daily 02:00 | **Yes / Ready** | `dealality-backups\LATEST` + Google Drive ZIP | **No — only Dealality job** |

- Last run: 2026-10-01 02:00:01  
- Last result: `-1073741510` / `3221225786` (abnormal terminate — not clean exit; consistent with interrupted overnight run)  
- Next run: 2026-10-02 02:00  
- Tasks whose Arguments mention `C:\Dev`: **1** (this one only)

### Answer

**How many nightly Dealality backup jobs are actually active?** → **1**

---

## 3. Exact current flow — `Backup-Dealality.ps1`

```
SOURCE A: C:\Dev\deal-capture-proxy
SOURCE B: %APPDATA%\Cursor\User\workspaceStorage   (best-effort)
        │
        ▼  robocopy (exclude node_modules/.git/caches/…)
LOCATION 1 (TEMP):
  C:\Dev\dealality-backups\_NEW_BACKUP_TEMP\
    deal-capture-proxy\
    Cursor-workspaceStorage\
        │
        ▼  verify (package.json, server.js, AGENTS.md,
           monthly-review\archive\index.json, public/api/lib,
           ≥5000 files, ≥50MB)
        │  FAIL → delete temp; LATEST untouched; STOP
        ▼
SAFE SWAP:
  LATEST → _PREVIOUS
  _NEW_BACKUP_TEMP → LATEST
  delete _PREVIOUS
        │
LOCATION 2 (LOCAL PERSISTENT — only one):
  C:\Dev\dealality-backups\LATEST\
        │
        ├─ keep .env* locally
        │
        ▼  cloud pack → strip .env* → ZIP
LOCATION 3 (CLOUD HISTORY):
  G:\My Drive\Dealality Backups\{yyyy-MM-dd_HHmm}\
    Dealality-backup.zip
    CLOUD_SECRET_EXCLUSIONS.json
    MANIFEST.txt
```

| Question | Answer |
|----------|--------|
| Is Backup-Staging temporary or persistent? | **Neither in current script** — it is a **historical orphan root**, not used |
| Does script write dealality-backups? | **YES** — only local root |
| Does script write Nightly-Backups? | **NO** |
| Does OneDrive archive logic remain? | **Write path removed**; report still mentions legacy OneDrive folder for manual review |
| Does another script copy afterward? | **NO active second copier found** |
| Is Cursor workspaceStorage backed up? | **YES** — into LATEST as `Cursor-workspaceStorage\` (best-effort), then into Drive ZIP |

---

## 4. Duplicate local copies (sizes + sample hashes)

### Current footprint (this scan)

| Root | Files | Size GB |
|------|------:|--------:|
| `dealality-backups` (LATEST+logs) | 65,976 | **6.05** |
| `Backup-Staging` (2 dated snaps) | 270,227 | **18.20** |
| `Backup-Scripts\dealality-snapshots` | 127,748 | **13.78** |
| `Cursor-Recovery-Archive-2026-09-08` | 805 | **5.14** |
| `Nightly-Backups` | 35,524 | **3.31** |
| **Approx total** | | **~46.5** |

Per-snapshot:

| Snapshot | GB | Notes |
|----------|---:|-------|
| Staging `2026-10-01_0200` | 11.25 | Includes `.git` + full ADP archive |
| Staging `2026-08-22_0200` | 6.94 | No ADP monthly-review archive |
| LATEST | 6.04 | **Now includes ADP archive** (post-cutover) |
| Nightly `2026-07-30_1242` | 3.30 | Older; no ADP archive |
| Crash freeze ×3 (~4.6 GB each) | ~13.8 | Near-duplicate safety trees |
| Cursor chats/git-safety | 5.14 | Unique recovery material |

### Sample SHA-256 (critical files)

| File | Live = LATEST = Staging Oct1 | Staging Aug22 | Nightly |
|------|------------------------------|---------------|---------|
| `server.js` | **identical** | different (older) | different (older) |
| monthly-review `index.json` | **identical** (136 reviews) | missing | missing |
| Cambridge archive `report.pdf` | **identical** | missing | missing |

### Overlap summary

| Content class | Where duplicated | Unique where |
|---------------|------------------|--------------|
| Current project tree + ADP archive | Live + LATEST + Staging Oct1 (+ Drive ZIP) | — |
| Older project trees | Staging Aug22, Nightly Jul30, crash freezes | Point-in-time only |
| `.git` packs in backups | Staging (yes) / LATEST (no) / Nightly (yes) | Staging/Nightly only among backups |
| Cursor workspaceStorage snapshots | LATEST, Staging, Nightly | Point-in-time variants |
| Cursor chat recovery | **Cursor-Recovery only** | **UNIQUE** |
| Crash freezes | 3× ~4.6 GB near-copies under Backup-Scripts | Mostly redundant to each other / live |

**Estimated redundant local GB:** ~**31 GB** (prior consolidation; still directionally correct)  
**Estimated unique local GB:** ~**15 GB** (Cursor recovery + Staging `.git`/provenance + residual history)

---

## 5. Canonical future design (evaluate only — NOT implemented)

```
C:\Dev\deal-capture-proxy
  (+ optional Cursor workspaceStorage)
        → C:\Dev\dealality-backups\_NEW_BACKUP_TEMP   (temp only)
        → C:\Dev\dealality-backups\LATEST             (sole local persistent)
        → G:\My Drive\Dealality Backups\YYYY-MM-DD_HHMM\  (history)
```

| Rule | Already true in script? |
|------|-------------------------|
| Temp exists only during backup | **YES** |
| LATEST is only persistent local project backup | **YES** (script); **NO** in filesystem until historical roots retired |
| Google Drive keeps history | **YES** (verified ZIP exists) |
| No separate Nightly-Backups history | Script: **YES**; Disk: **still has legacy folder** |
| No permanent Backup-Staging history | Script: **YES**; Disk: **still has 2 snaps** |
| Backup-Scripts = scripts only | Script intent: **YES**; Disk: **also holds 13.78 GB snapshots** |
| Cursor recovery only if unique | **Still unique** — retain until cloud-archived |

**Verdict:** Preferred design = current script behavior. Simplification work is **retiring orphan destinations**, not rewriting the active flow.

---

## 6. Classify current roots

| Root | Classification |
|------|----------------|
| `C:\Dev\dealality-backups` | **KEEP AS ACTIVE** |
| `C:\Dev\Backup-Scripts` (`Backup-Dealality.ps1`) | **KEEP AS ACTIVE** |
| `C:\Dev\Backup-Scripts\dealality-snapshots` | **KEEP TEMPORARILY FOR RECOVERY** → later **MOVE UNIQUE CONTENT TO GOOGLE DRIVE** / retire near-dupes after verify |
| `C:\Dev\Backup-Staging` | **REPLACE WITH CANONICAL FLOW** (already replaced as write target) + **KEEP TEMPORARILY FOR RECOVERY** until unique-gate passes |
| `C:\Dev\Nightly-Backups` | **SAFE TO RETIRE AFTER VERIFICATION** (after Drive cold-archive verify) |
| `C:\Dev\Cursor-Recovery-Archive-2026-09-08` | **KEEP TEMPORARILY FOR RECOVERY** → **MOVE UNIQUE CONTENT TO GOOGLE DRIVE** (chats/git-safety) before any delete |

---

## 7. Unique content gate (before any retirement)

A folder may be retired only when it has **ZERO unique files** not present in at least one of: Git, live working tree, `dealality-backups\LATEST`, verified Google Drive archive.

| Root | Gate status (as of this audit) |
|------|--------------------------------|
| Nightly-Backups | Likely **eligible after** verified Drive cold archive of Jul 30 tree; little product uniqueness vs LATEST |
| Staging Aug22 | **Eligible after** verified Drive cold archive; older point-in-time; no ADP archive |
| Staging Oct1 | **Not yet clear to delete** — still holds `.git` backup copy + provenance value; ADP archive now also in Live+LATEST+Drive ZIP, but treat as **KEEP until explicit Joan sign-off + Drive cold pack of full Staging tree (incl .git if desired)** |
| Crash freezes (3× ~4.6 GB) | Two of three are near-duplicates; retire extras only after one freeze + LATEST + Drive verified |
| Cursor-Recovery | **FAILS unique gate** until chats + git-safety are in a verified Drive archive |

### Special protections (must remain covered)

- ADP monthly-review PDFs/archive → now in Live + LATEST + Drive ZIP `2026-10-01_2036` (+ Staging Oct1 provenance)
- Never-committed restored assets → in Live + LATEST + Drive ZIP
- Local research/customer artifacts → in LATEST / Drive (spot-checked Hotel Intelligence / capital-provider)
- Cursor history → **Cursor-Recovery** (not in LATEST productively as chats)
- Secrets → local LATEST **yes**; Drive ZIP **excluded** (policy)

---

## 8. Proposed cleanup plan ONLY (do not execute)

### STEP 1 — Confirm Google Drive operational archive
- **Action:** Treat `G:\My Drive\Dealality Backups\2026-10-01_2036` as canonical cloud history baseline (already integrity-verified in cutover)
- **Data at risk:** none (read-only confirm)
- **Verification:** ZIP opens; no `.env*`; archive counts 136/131; hash spot-checks
- **Recoverable GB:** 0 yet
- **Rollback:** n/a

### STEP 2 — No duplicate scheduled job to retire
- **Action:** Confirm only `Dealality Nightly Backup` remains (already true). Do **not** create additional tasks.
- **Data at risk:** none
- **Verification:** `Get-ScheduledTask` Dealality count = 1
- **Recoverable GB:** 0
- **Rollback:** n/a

### STEP 3 — Cold-archive Nightly-Backups to Drive, then retire local
- **Action:** Verify/complete Drive folder `...\Dealality Backups\Nightly-Backups\` (placeholder-like child seen); make verified cold archive; then delete local `C:\Dev\Nightly-Backups` only after hash/file-count verify
- **Data at risk:** Jul 30 point-in-time + older Cursor workspaceStorage
- **Verification:** file counts, sample hashes, archive open
- **Recoverable GB:** ~**3.3**
- **Rollback:** restore from Drive cold archive

### STEP 4 — Cold-archive Staging `2026-08-22_0200`, then retire local
- **Action:** Same pattern; Drive already shows a `2026-08-22_0200` folder — verify completeness before local delete
- **Data at risk:** Aug 22 tree (no ADP archive)
- **Recoverable GB:** ~**6.9**
- **Rollback:** Drive restore

### STEP 5 — Cold-archive Cursor-Recovery unique content
- **Action:** Archive `chat-raw-gzip`, `chats`, `git-safety`, indexes to Drive; keep local until verified
- **Data at risk:** unrecovered chat history / URLs / git-safety bundle
- **Recoverable GB:** ~**5.1** only after dual-verified + Joan OK
- **Rollback:** Drive + keep one local copy until confidence high

### STEP 6 — Staging `2026-10-01_0200` provenance freeze
- **Action:** Create verified Drive cold pack of full Oct1 Staging (optional include `.git`); keep local until Joan signs unique-gate
- **Data at risk:** pre-crash provenance + `.git` backup
- **Recoverable GB:** ~**11.3** eventually
- **Rollback:** Drive + LATEST + Live

### STEP 7 — Collapse `dealality-snapshots` near-duplicates
- **Action:** Keep **one** freeze + tiny pre-restore archive snap; Drive-archive then remove the other ~4.6 GB twins
- **Data at risk:** crash-recovery freezes
- **Recoverable GB:** ~**9.2** (two of three large freezes)
- **Rollback:** remaining freeze + LATEST + Drive

### STEP 8 — Convert Backup-Staging to empty/absent (no permanent history)
- **Action:** After Steps 4–6, remove Staging root or leave empty; do **not** reintroduce as write target (canonical temp is `_NEW_BACKUP_TEMP`)
- **Recoverable GB:** included above
- **Rollback:** Drive cold packs

### STEP 9 — Optional OneDrive leftover cleanup
- **Action:** Manual review of `%USERPROFILE%\OneDrive\Dealality Backups\` dated folders (`2026-10-01_1615`, `1714`) — not written by current script; archive or delete only after confirming Drive is SoT
- **Recoverable GB:** TBD (cloud-side / OneDrive quota, not necessarily C:\Dev)

---

## 9. Target end state

| Item | Target | Current |
|------|--------|---------|
| ACTIVE SCHEDULED TASKS | **1** | **1** ✅ |
| ACTIVE BACKUP SCRIPT | `C:\Dev\Backup-Scripts\Backup-Dealality.ps1` | ✅ |
| LOCAL BACKUP ROOT | `C:\Dev\dealality-backups\LATEST` | ✅ active; extras remain |
| LOCAL HISTORICAL SNAPSHOTS | **0** | Staging + Nightly + snaps + Cursor still present |
| GOOGLE DRIVE | dated historical backups | ✅ first verified ZIP; cold archives incomplete |
| TEMP/STAGING | auto-removed after success | ✅ `_NEW_BACKUP_TEMP`; Staging root is orphan history |

---

## 10. Return card

| Metric | Value |
|--------|-------|
| **Active backup scripts** | **1** (`Backup-Dealality.ps1`) |
| **Active scheduled tasks (Dealality)** | **1** (`Dealality Nightly Backup`) |
| **Current backup destinations** | LATEST; Google Drive ZIP; orphan Staging×2; Nightly×1; crash snaps×4; Cursor-Recovery; leftover OneDrive folders |
| **Overlap** | High between Staging Oct1 / LATEST / Live / Drive on current tree+ADP; crash freezes near-duplicate; Nightly/Aug22 older supersets |
| **Unique data** | Cursor chats/git-safety; Staging `.git`; older point-in-time trees; crash freezes until collapsed |
| **Recommended canonical flow** | Source → `_NEW_BACKUP_TEMP` → `LATEST` → Google Drive dated ZIP *(already the active script)* |
| **Eventually retire after verify** | Nightly-Backups; Staging Aug22; excess crash freezes; Staging Oct1 (after provenance freeze); Cursor-Recovery (after Drive) |
| **Estimated local GB recoverable** | ~**25–31 GB** after full unique-gate + Drive verify (of ~46.5 GB backup footprint) |
| **ZERO changes made** | **YES** |

### Google Drive note (important for next phase)

Drive root already contains folders named `Nightly-Backups` and `2026-08-22_0200` with sparse/placeholder children. **Do not assume they are complete cold archives.** Next cleanup step must verify completeness before any local deletion.

### Evidence files

- `reports/backup-simplification-sizes-20261001.json`
- `reports/backup-simplification-hash-overlap-20261001.json`
- Prior: `reports/dealality-backup-architecture-audit.md`, `reports/dealality-backup-consolidation-audit.md`, `reports/google-drive-cutover-verified-20261001.md`
