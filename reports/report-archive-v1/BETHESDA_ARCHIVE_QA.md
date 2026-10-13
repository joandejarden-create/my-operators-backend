# Bethesda Golden Archive QA — Report Archive V1

Hotel: Bethesda Marriott · `recLuxvwwxID7U2B8` · property `adp_bethesda_marriott`

## Oct 1 ADP — BETHESDA_ADP_BASELINE_V1

| Check | Result |
|-------|--------|
| Archive ID | `adp_baseline_2026-10-01_v1` |
| Report class | BASELINE |
| Baseline ID | `BETHESDA_ADP_BASELINE_V1` |
| Query set | `BETHESDA_ADP_OCTOBER_CONTROL_QUERY_SET_V1` |
| Data snapshot archived | **YES** |
| Snapshot checksum | `1c9cee1152dea2fcbe10f12ae3252d2396d13859aa58bc7df26bb7ab94d89f49` |
| Snapshot checksum verify | **PASS** |
| PDF archived | **NO** |
| PDF reason | `NO_HISTORICAL_CLIENT_VISIBLE_PDF_BINARY_ON_2026-10-01` (= PDF_NOT_PREVIOUSLY_ARCHIVED doctrine) |
| Fake PDF from live data | **NOT created** (correct) |
| Admin list retrieval | **PASS** |
| Admin data retrieval | **PASS** |
| Admin PDF retrieval | **N/A / FAIL** (no binary; expected) |

## Oct 1 GDI — BETHESDA_GDI_DAY1_SNAPSHOT

| Check | Result |
|-------|--------|
| Archive ID | `gdi_day1_2026-10-01_v1` |
| Report class | DAY1 |
| Data snapshot archived | **YES** (frozen Day-1; ready=37 noted in meta) |
| Snapshot checksum | `847a6861da6bc509d23d39ea4f2b8acb53a09d239d9776b666284cbbeb52d7a0` |
| Snapshot checksum verify | **PASS** |
| PDF archived | **NO** |
| PDF reason | `NO_HISTORICAL_CLIENT_VISIBLE_PDF_BINARY_ON_2026-10-01` |
| Fake PDF from live data | **NOT created** (correct) |
| External share token recorded | present on meta (Admin does not expose raw token in list UX) |
| Admin data retrieval | **PASS** |

## Later Bethesda entries (with PDFs)

| Archive | PDF | Snapshot | Checksum |
|---------|-----|----------|----------|
| `adp_current_2026-10-02_v1` | YES | YES | PASS |
| `adp_official_baseline_2026-10-02_…_v2` | YES | YES | PASS |
| `gdi_current_2026-10-02_v2` | YES | YES | PASS |
| `gdi_current_2026-10-02_v3` | YES | YES | PASS |

## Immutability

Re-publish of `adp_baseline_2026-10-01_v1` throws `ARCHIVE_IMMUTABLE` → **PASS**

## Auto-archive smoke

Temp root: auto GDI CURRENT write → retrieve PDF + checksum match → second write skips (`already_archived`) → **PASS**

## Multi-hotel empty state

| Hotel key probed | Entries | Empty UX |
|------------------|---------|----------|
| Renaissance (`adp_renaissance_times_square`) | 0 | "No archived reports yet." |
| Hilton (`adp_hilton_times_square`) | 0 | "No archived reports yet." |
| Radisson (`adp_radisson_santo_domingo`) | 0 | "No archived reports yet." |

## HISTORICAL_RETRIEVAL

**PASS** — Bethesda Oct 1 structured snapshots retrieve with valid checksums; available PDF archives (Oct 2+) retrieve with matching checksums; Oct 1 PDFs correctly absent and not fabricated.

## Redeploy note

Golden Oct 1 metas/snapshots under `data/dealality-report-archive/` ship with the repo. Runtime auto-archived PDFs require `DEALALITY_REPORT_ARCHIVE_ROOT` (or PDF root) on a persistent volume so they survive Railway redeploys.
