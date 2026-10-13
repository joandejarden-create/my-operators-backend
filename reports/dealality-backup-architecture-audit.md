# Dealality backup architecture + consolidation audit (READ ONLY)

Generated: 2026-10-01 (local)

**ZERO files moved/deleted/renamed/compressed. ZERO changes to Backup-Dealality.ps1, scheduled tasks, or retention.**

---

## Critical correction vs prior assumption

**`C:\Dev\Backup-Staging` is NOT referenced by the current nightly script.**

Evidence from `C:\Dev\Backup-Scripts\Backup-Dealality.ps1` (read 2026-10-01):

- `findstr` for `Backup-Staging` / `BackupStaging` → **no matches** (exit 1)
- Local paths are exclusively under `C:\Dev\dealality-backups\`
- Temp staging name is `_NEW_BACKUP_TEMP` (inside `dealality-backups`), **not** `Backup-Staging`

`Backup-Staging` **does** contain dated `_0200` snapshots that match the scheduled-task clock (2:00 AM) and include `.git` (current script excludes `.git`). That pattern matches a **prior** nightly architecture. Treat Staging as **historical / recovery-valuable**, not as the live write target of tonight's job.

**Do not delete Staging snapshots** — especially `2026-10-01_0200` (unique pre-crash ADP monthly-review archive provenance). Just do not confuse it with the active write path.

---

## 1. Exact nightly backup flow (from current script)

### Scheduled task

| Field | Value |
|-------|--------|
| Name | `Dealality Nightly Backup` |
| Command | `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Dev\Backup-Scripts\Backup-Dealality.ps1"` |
| Schedule | Daily 2:00 AM (start date 2026-07-30) |
| Next run | 2026-10-02 2:00 AM |
| Last run | 2026-10-01 2:00:01 AM |
| Last result | `-1073741510` (abnormal / process terminated — not a clean script exit 0/1) |
| State | Enabled / Ready |

### Sources (exact)

1. `C:\Dev\deal-capture-proxy` (`$ProjectSource`)
2. `%APPDATA%\Cursor\User\workspaceStorage` (`$CursorSource`) — best-effort

### Exclusions (`$RobocopyExcludeDirs`)

`node_modules`, `.next`, `dist`, `build`, `coverage`, `.cache`, `.turbo`, `.vercel`, `tmp`, `temp`, `.git`

**Not excluded:** `.env` / secret-bearing files (copied into local LATEST; cloud ZIP blocked if live secrets detected).

### Verification (`Test-BackupVerified`)

Required under backup `deal-capture-proxy\`:

- `package.json`, `server.js`, `AGENTS.md`
- `reports\ai-demand-positioning\monthly-review\archive\index.json` (**ADP archive gate**)
- subtrees `public`, `api`, `lib`
- file count ≥ 5000; total size ≥ 50 MB

### Exact flow map (CURRENT script)

```
SOURCE A: C:\Dev\deal-capture-proxy
SOURCE B: %APPDATA%\Cursor\User\workspaceStorage
        │
        ▼  robocopy (excludes .git / node_modules / caches)
C:\Dev\dealality-backups\_NEW_BACKUP_TEMP\
        │  deal-capture-proxy\ + Cursor-workspaceStorage\
        ▼
VERIFY (_NEW_BACKUP_TEMP)
        │  fail → delete temp; leave existing LATEST untouched
        ▼
SAFE SWAP
  LATEST  →  _PREVIOUS  (if existed)
  _NEW_BACKUP_TEMP  →  LATEST
  delete _PREVIOUS
        │
        ▼
C:\Dev\dealality-backups\LATEST\     ← sole local operational snapshot
        │
        ├─ SECRETS GATE (Test-PlaintextSecrets)
        │    live .env* detected → CLOUD BLOCKED; LATEST kept
        │
        └─ GOOGLE DRIVE (if not blocked / not skipped)
             G:\My Drive\Dealality Backups\{yyyy-MM-dd_HHmm}\
               Dealality-backup.zip  (tar from LATEST)
               MANIFEST.txt
             Retention configured: daily 14d + weekly 8w
             Deletes ONLY if DEALALITY_BACKUP_RETENTION_DELETE=1
```

**`Backup-Staging` does not appear in this flow.**

### Role of each path in the *current* script

| Path | Role in current script |
|------|------------------------|
| `_NEW_BACKUP_TEMP` | Temporary verified staging (single-run; removed on fail or after rename) |
| `LATEST` | Persistent single local operational recovery snapshot |
| `_PREVIOUS` | Swap safety; deleted after successful promote |
| `logs\` | Run logs |
| `G:\My Drive\Dealality Backups\` | Timestamped historical ZIP archive |
| `C:\Dev\Backup-Staging` | **Not used** |

### Env flags (read-only inventory)

| Env | Effect |
|-----|--------|
| `DEALALITY_BACKUP_DRY_RUN=1` | Validate only; no swap / no ZIP |
| `DEALALITY_BACKUP_CLOUD_FROM_LATEST=1` | Skip rescan; ZIP from existing LATEST |
| `DEALALITY_BACKUP_SKIP_CLOUD=1` | Local only |
| `DEALALITY_BACKUP_RETENTION_DELETE=1` | Allow cloud retention deletes |
| `DEALALITY_GOOGLE_DRIVE_ROOT` | Override My Drive mount |
| `DEALALITY_BACKUP_TEST_FAIL_VERIFY=1` | Force verify fail (LATEST preserved) |

### Post-backup audits (non-blocking soft checks)

- `node scripts/audit-untracked-runtime-files.mjs`
- `node scripts/audit-startup-import-graph.mjs` (if present)

---

## 2. Classify backup roots by role

| Root | Classification | Evidence |
|------|----------------|----------|
| `C:\Dev\Backup-Scripts` | **A. ACTIVE BACKUP WORKFLOW** | Holds `Backup-Dealality.ps1` invoked by scheduled task; also holds crash safety snaps under `dealality-snapshots\` |
| `C:\Dev\dealality-backups` | **A + B. ACTIVE WORKFLOW + ACTIVE LOCAL RECOVERY** | `_NEW_BACKUP_TEMP` / `LATEST` / `_PREVIOUS` / `logs` — current script SoT |
| `C:\Dev\Backup-Staging` | **C. HISTORICAL ARCHIVE + D. RECOVERY-ONLY ARCHIVE** | Not in current script; dated `_0200` snapshots; Oct 1 holds unique ADP archive + `.git`; **KEEP for recovery until cloud-verified** |
| `C:\Dev\Cursor-Recovery-Archive-2026-09-08` | **D. RECOVERY-ONLY ARCHIVE** | Chats + git-safety bundle; not produced by nightly script |
| `C:\Dev\Nightly-Backups` | **E. LEGACY / POSSIBLY REDUNDANT** | Single July 30 snapshot; superseded by Staging/LATEST eras |

### Backup-Scripts sub-role note

| Path | Role |
|------|------|
| `Backup-Dealality.ps1` | **A. ACTIVE** — DO NOT TOUCH without explicit change task |
| `dealality-snapshots\*` | **D. RECOVERY-ONLY** — crash/pre-restore freezes (not written by nightly script) |

---

## 3. Backup-Staging deep analysis

| Metric | Value |
|--------|--------|
| Total size | **18.32 GB** (273,719 files) |
| Dated snapshots | **2** |
| Oldest | `2026-08-22_0200` (7.07 GB, 115,297 files) |
| Newest | `2026-10-01_0200` (11.25 GB, 158,422 files) |
| Contents shape | `deal-capture-proxy\` + `Cursor-workspaceStorage\` |
| `.git` included? | **YES** (unlike current LATEST) |
| `node_modules`? | No (prior audit) |
| ADP monthly-review archive on newest? | **YES** — 136 reviews / 131 PDFs |
| Required by current `Backup-Dealality.ps1`? | **NO** — neither snapshot is read or written |
| Auto-superseded by current script? | **NO** — script never touches this folder; snapshots accumulate manually / via old architecture |

### Per-snapshot

| Snapshot | Still required by script? | Unique value | Cloud class |
|----------|---------------------------|--------------|-------------|
| `2026-10-01_0200` | No | **HIGH** — pre-crash ADP archive provenance; archive index identical to LIVE after restore; LATEST still missing archive; includes `.git` | KEEP LOCAL — RECOVERY until cloud verify |
| `2026-08-22_0200` | No | Older historical tree; no monthly-review archive | MOVE TO GOOGLE DRIVE LATER |

### Can Staging eventually be reduced without breaking nightly backup?

**Yes — the nightly job will keep working if Staging is empty or gone**, because the current script never uses it.

**But:** do **not** remove `2026-10-01_0200` until:

1. Live working tree archive remains intact (currently YES)
2. `dealality-backups\LATEST` is refreshed to include archive (currently **NO**)
3. At least one verified Google Drive cold archive of Staging Oct 1 exists

**Do not recommend deleting the currently valuable Staging snapshot** (`2026-10-01_0200`).

---

## 4. Unique data / overlap (architecture lens)

### Footprint (from consolidation audit + live probes)

| Metric | GB |
|--------|-----|
| Total local backup roots (5) | **46.67** |
| Unique estimate | **~15.30** |
| Redundant / overlap estimate | **~31.37** |

### What is unique where

| Content | Live repo | Git | LATEST | Staging `2026-10-01_0200` | Cursor-Recovery | Nightly |
|---------|-----------|-----|--------|---------------------------|-----------------|---------|
| ADP monthly-review archive/PDFs | YES (restored) | typically not fully committed | **NO** | **YES** (provenance) | no | no |
| Working-tree source (excl .git) | YES | partial | YES (stale vs live on some files) | YES (pre-crash point-in-time) | partial | older |
| `.git` object packs | YES (live) | n/a | **NO** (excluded) | **YES** | git-safety bundle | YES |
| Cursor workspaceStorage snapshot | live AppData | no | YES (in LATEST) | YES | chats different | YES (Jul) |
| Cursor chat transcripts / recovery URLs | no | no | no | no | **YES UNIQUE** | no |
| Crash safety freezes | — | — | — | — | — | Backup-Scripts snaps |

### Overlap highlights

| Pair | Note |
|------|------|
| Staging Oct 1 archive vs LIVE | indexIdentical=True; 15/15 READY PDFs identical — archive restored to live; Staging still only pre-crash full-tree provenance |
| Staging vs LATEST | High tree overlap; **~2.75 GB unique-ish** driven largely by archive + `.git` + size delta |
| Backup-Scripts freezes vs LIVE | ~11 GB near-duplicate safety trees |
| Nightly vs Staging/LATEST | Mostly superseded (~0.87 GB unique est.) |
| Cursor-Recovery vs Git+LATEST | ~4.37 GB unique (chats + bundle) |

---

## 5. Preferred long-term architecture (evaluation only — NOT implemented)

Proposed design:

```
SOURCE (repo + optional Cursor workspaceStorage)
  → temporary verified staging (_NEW_BACKUP_TEMP)
  → local rolling LATEST (single operational backup)
  → Google Drive historical ZIP archive
```

### Verdict vs current script

| Desired trait | Current script already? |
|---------------|-------------------------|
| Single local operational backup (`LATEST`) | **YES** |
| Temp staging cleaned after success/fail | **YES** (`_NEW_BACKUP_TEMP` / `_PREVIOUS`) |
| Historical in Google Drive | **Designed YES**; currently **BLOCKED** on secrets + LATEST missing archive index |
| Staging folder as dated accumulators | **Legacy only** — not needed for current design |

### Gaps before the preferred design is fully operational

1. **Secrets policy** — live `.env*` in LATEST block cloud ZIP
2. **Refresh LATEST** — must include restored monthly-review `archive\index.json` or verification fails / LATEST stays stale
3. **Cold-archive Staging Oct 1 + Cursor chats** to Google Drive before any local prune
4. **Clarify Backup-Staging** in ops docs as historical (optional: stop using the name “staging” for the active temp — already named `_NEW_BACKUP_TEMP`)

---

## 6. What can eventually move to cloud

| Root / snapshot | Classification |
|-----------------|----------------|
| `Backup-Scripts\Backup-Dealality.ps1` | **KEEP LOCAL — ACTIVE WORKFLOW** / **DO NOT TOUCH** |
| `dealality-backups\LATEST` (+ logs) | **KEEP LOCAL — ACTIVE WORKFLOW / RECOVERY** |
| `Backup-Scripts\dealality-snapshots\*` | **KEEP LOCAL — RECOVERY** until cloud verify |
| `Backup-Staging\2026-10-01_0200` | **KEEP LOCAL — RECOVERY** → later **MOVE TO GOOGLE DRIVE** after LATEST refresh + Drive verify |
| `Backup-Staging\2026-08-22_0200` | **MOVE TO GOOGLE DRIVE LATER** → then **SAFE TO CONSIDER DELETING AFTER CLOUD VERIFY** |
| `Nightly-Backups` | **MOVE TO GOOGLE DRIVE LATER** → then **SAFE TO CONSIDER DELETING AFTER CLOUD VERIFY** |
| `Cursor-Recovery-Archive` chats / git-safety | **KEEP LOCAL — RECOVERY** → **MOVE TO GOOGLE DRIVE LATER** (keep local until verified) |
| Empty / unrelated | not in scope |

**Do not treat emptying `Backup-Staging` as part of nightly cleanup** until Joan explicitly retires the historical snapshots after cloud verification.

---

## 7. Recommended footprints (target state — not applied)

| Layer | Target |
|-------|--------|
| **Local operational** | ~6–10 GB: `C:\Dev\dealality-backups\LATEST` + `Backup-Scripts\Backup-Dealality.ps1` + logs |
| **Local recovery (interim)** | Staging `2026-10-01_0200` + Cursor-Recovery chats/git-safety + recent crash snaps until Drive verified |
| **Google Drive** | Rolling nightly ZIPs under `G:\My Drive\Dealality Backups\` + `Archive\` cold packs for Nightly, Aug 22 Staging, Cursor chats, eventually Oct 1 Staging |
| **GitHub** | Committed source history |

**True redundant local GB today:** ~31 GB of overlapping trees across Staging / Scripts snaps / Nightly / LATEST.

**Unique local GB today:** ~15 GB (Cursor recovery + Staging uniqueness + residual historical).

---

## Answers in one place

| Question | Answer |
|----------|--------|
| Exact role of Backup-Staging **today** | **Historical dated snapshot archive** from a prior nightly design; **not** the active write target of current `Backup-Dealality.ps1` |
| Exact nightly flow | Source → `dealality-backups\_NEW_BACKUP_TEMP` → verify → swap to `LATEST` → secrets gate → Google Drive ZIP |
| Active vs historical | **Active:** Backup-Scripts script + dealality-backups. **Historical/recovery:** Staging, Nightly, Cursor-Recovery, crash snaps |
| Will deleting Staging break nightly? | **No** for current script — but **do not delete** Oct 1 Staging until LATEST+cloud catch up |
| Is LATEST sufficient alone today? | **No** — missing ADP archive index |

---

## Reports related

- Prior inventory: `reports/dealality-backup-consolidation-audit.md` / `.json`
- Google Drive conversion status: `reports/google-drive-backup-conversion-20261001.md` (cloud still blocked on secrets)

---

READY FOR JOAN REVIEW — BACKUP ARCHITECTURE AUDIT COMPLETE
