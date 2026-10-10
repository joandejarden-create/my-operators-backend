# REPORT_ARCHIVE_QA.md

## Status: PASS

### Surfaces

- Admin tab: **Report Archive**
- APIs: list / entry / pdf under `/api/admin/report-archive*`
- Store: `data/dealality-report-archive/` (fixed REPO_ROOT; was briefly mis-rooted one level too high)

### Bethesda entries

| Archive ID | Type | Class | PDF | Snapshot | Notes |
|---|---|---|---|---|---|
| `adp_baseline_2026-10-01_v1` | ADP | BASELINE | NO | YES | Oct 1; consideration 42.1 |
| `gdi_day1_2026-10-01_v1` | GDI | DAY1 | NO | YES | ready=37 frozen |
| `adp_current_2026-10-02_v1` | ADP | CURRENT | YES | YES | Client PDF |
| `gdi_current_2026-10-02_v2` | GDI | CURRENT | YES | YES | First corrected PDF |
| `gdi_current_2026-10-02_v3` | GDI | CURRENT | YES | YES | RNA dedupe; supersedes v2 |

### Retrieval

- Oct 1 ADP snapshot consideration = **42.1** — PASS  
- Oct 1 GDI ready = **37** (live 35) — PASS  
- Checksums match on load — PASS  

### Filters

ALL / ADP / GDI / BASELINE / DAY1 / WEEKLY / MONTHLY / REMEASUREMENT / CURRENT
