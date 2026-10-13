# Google Drive backup conversion + verification

Generated: 2026-10-01 (local)

**Result: CLOUD BACKUP BLOCKED — SECRETS DECISION REQUIRED**

No Google Drive ZIP was created. No backups deleted. No C:\Dev cleanup. No Git changes. No Airtable writes.

---

## 1. Prior script inventory (before conversion)

| Item | Value |
|------|--------|
| Source | `C:\Dev\deal-capture-proxy` |
| Local temp | `C:\Dev\dealality-backups\_NEW_BACKUP_TEMP` |
| Local LATEST | `C:\Dev\dealality-backups\LATEST` |
| Previous swap | `_PREVIOUS` then delete after success |
| Verification | `package.json`, `server.js`, `AGENTS.md`, `public`/`api`/`lib`, file count ≥5000, size ≥50MB |
| Cursor workspace | Best-effort robocopy of `%APPDATA%\Cursor\User\workspaceStorage` |
| Exclusions | `node_modules`, `.next`, `dist`, `build`, `coverage`, `.cache`, `.turbo`, `.vercel`, `tmp`, `temp`, `.git` |
| Cloud (old) | `$env:USERPROFILE\OneDrive\Dealality Backups\{timestamp}\Dealality-backup.zip` via `tar` |
| Retention | daily 14d + weekly 8w (was deleting on OneDrive) |
| Secret handling | **None** — `.env` included in local + would upload |
| Audits | untracked runtime + startup import graph after backup |

---

## 2. Google Drive detection

| Check | Result |
|-------|--------|
| Mount | **`G:\My Drive`** (exists, writable) |
| DriveFS account | Single: `113953973289272511803` |
| Ambiguous? | **No** — one Google account / one My Drive |
| Archive root created | `G:\My Drive\Dealality Backups` |
| Write probe | **PASS** |

Override env if needed: `DEALALITY_GOOGLE_DRIVE_ROOT`

---

## 3–5. Script conversion done

Updated: `C:\Dev\Backup-Scripts\Backup-Dealality.ps1`

- Local rolling LATEST flow **preserved**
- OneDrive archive **writes removed** (ZERO)
- Cloud target: `G:\My Drive\Dealality Backups\YYYY-MM-DD_HHMM\Dealality-backup.zip`
- Retention configured but **delete gated** (`DEALALITY_BACKUP_RETENTION_DELETE=1` only)
- Secrets gate: blocks cloud if live `.env` / credentials detected
- Verification now **requires** ADP monthly-review archive:
  `reports\ai-demand-positioning\monthly-review\archive\index.json`
- Dry-run mode: `DEALALITY_BACKUP_DRY_RUN=1`

OneDrive string remnants in script are **report labels only** (legacy folder path for manual review) — no archive writes.

---

## 6–7. Protection + exclusions

**Protected (copied):** committed + modified tracked + untracked working-tree files, fixtures, reports (incl. monthly-review archive when present on source), Cursor workspaceStorage.

**Excluded:** `node_modules`, `.next`, `dist`, `build`, `coverage`, `.cache`, `.turbo`, `.vercel`, `tmp`, `temp`, `.git`

**Not excluded (by design):** `.env` / secret files — local snapshot may contain them; **cloud ZIP blocked** until Joan decides.

---

## 8. SECRETS (content not shown)

| | |
|--|--|
| Plaintext secret-bearing files detected | **YES** |
| Would be included in Google Drive ZIP | **YES** (with current exclusions) |
| Uploaded | **NO** |

Live secret paths under LATEST (paths only):

- `...\LATEST\deal-capture-proxy\.env`
- `...\LATEST\deal-capture-proxy\.env.local`
- `...\LATEST\deal-capture-proxy\.env.strix`
- `...\LATEST\deal-capture-proxy\matcha\.env`
- `...\LATEST\deal-capture-proxy\rail-explore\.env`

Templates also present: `.env.example` (x3) — reported separately.

**Decision needed before any cloud ZIP:**

1. Exclude live `.env*` from cloud ZIP only (keep in local LATEST), or  
2. Scrub/redact before cloud, or  
3. Explicit approve uploading encrypted/secrets-containing ZIP (not recommended)

Until then: **CLOUD BACKUP BLOCKED — SECRETS DECISION REQUIRED**

---

## 9. Dry-run results

| Check | Result |
|-------|--------|
| Google Drive path | `G:\My Drive` OK |
| Destination writable | YES |
| Local LATEST exists | YES (~5.93 GB, ~64,733 files) |
| LATEST verified | **FAIL** — missing `reports\...\monthly-review\archive\index.json` |
| Archive path creatable | YES |
| OneDrive writes | ZERO |
| ADP archive in LATEST | **NO** (stale LATEST from before archive restore) |
| ADP archive on live source | **YES** (restored earlier today) |
| Secrets | DETECTED → cloud blocked |
| ZIP created | **NO** |

Implication: next successful **local** backup run (after secrets policy for cloud) will pull live archive into new LATEST via robocopy.

---

## 10. Test cloud archive

**NOT RUN** — secrets unresolved (required stop).

---

## 11. Retention

| | |
|--|--|
| Policy | daily 14 days + weekly 8 weeks |
| Dry-run deletion candidates (Google Drive) | **0** (no timestamped archives yet under Drive root) |
| Actual deletions | **ZERO** |

---

## 12. Scheduled task

| | |
|--|--|
| Name | `Dealality Nightly Backup` |
| State | Ready |
| Action | `powershell.exe ... -File "C:\Dev\Backup-Scripts\Backup-Dealality.ps1"` |
| Competing tasks | None created |

Nightly will: local LATEST swap → runtime audits → Google Drive ZIP **only if secrets clear**.

---

## 13. Joan review summary

### LOCAL BACKUP
- path: `C:\Dev\dealality-backups\LATEST`
- verified: **NO** (missing monthly-review archive index — LATEST stale vs live restore)
- file count: ~64,733
- total size: ~5.93 GB
- previous LATEST preserved until verification: N/A this dry-run (no swap)

### GOOGLE DRIVE
- detected path: `G:\My Drive`
- archive root: `G:\My Drive\Dealality Backups`
- test archive path: **(none — blocked)**
- archive size: N/A
- ZIP verification: N/A

### CONTENT VERIFICATION (live source vs readiness)
- modified tracked / untracked: protected by robocopy working-tree copy on next local run
- ADP monthly-review archive on **live** disk: YES
- ADP archive in **current LATEST**: NO (needs local refresh)
- recovered PDFs: on live disk YES; in LATEST NO until refresh
- Cursor workspace: still included in script

### SECRETS
- detected: **YES**
- uploaded: **NO**

### ONEDRIVE
- remaining OneDrive **archive write** code: **ZERO**
- existing folders (manual review only):
  - `C:\Users\joand\OneDrive\Dealality Backups\2026-10-01_1615`
  - `C:\Users\joand\OneDrive\Dealality Backups\2026-10-01_1714`
- **Not deleted**

### RETENTION
- configured; dry-run only; deletions: **0**

### SAFETY
- source files deleted: **0**
- backups deleted: **0**
- Git changes: **0** (script lives under `C:\Dev\Backup-Scripts`, outside repo)
- Airtable writes: **0**

---

## Recommended next decision (Joan)

1. Choose secrets policy for cloud ZIP (exclude live `.env*` recommended).  
2. Approve one local refresh run to promote archive-bearing LATEST.  
3. Then approve one Google Drive test ZIP.  
4. Enable retention deletes only after that ZIP is verified.

Until secrets decision + successful cloud ZIP:

**Not yet:** `READY FOR JOAN REVIEW — GOOGLE DRIVE BACKUP VERIFIED`

**Current status:**

CLOUD BACKUP BLOCKED — SECRETS DECISION REQUIRED
