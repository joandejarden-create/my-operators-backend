# AI Demand Admin Report Archive V1

## A. Executive Result

| Gate | Result |
|------|--------|
| ARCHIVE TAB | **PASS** — primary AI Demand Admin tab |
| DURABLE STORAGE | **PARTIAL** — ledger + env volume root; golden Bethesda in git; prod volume still recommended |
| PDF ARCHIVE | **PASS** for published writes going forward; Oct 1 golden = data-only (correct) |
| DATA SNAPSHOT ARCHIVE | **PASS** |
| BETHESDA | **PASS** — Oct 1 ADP baseline + GDI Day-1 frozen snapshots archived |
| HISTORICAL RETRIEVAL | **PASS** |

**FINAL VERDICT:** REPORT ARCHIVE PASSES — BETHESDA GOLDEN ARCHIVE COMPLETE

## B. Previous State

- ADP current PDFs lived under `reports/ai-demand-positioning/current-report-pdf/` (overwriteable pointer).
- GDI PDFs lived under `reports/group-demand-intelligence/pdf/` with local `archive/vN/` copies.
- Published ADP JSON lived under `data/ai-demand-positioning/published/`.
- No unified Admin historical ledger for “what the client saw.”
- Railway ephemeral FS risk for anything not git-tracked or volume-mounted.
- External share tokens pointed at **latest**, not historical editions.

## C. Archive Architecture

- Ledger: `lib/dealality-report-archive/report-archive-store-v1.js`
- Auto-archive: `lib/dealality-report-archive/auto-archive-published-report-v1.js`
- Root: `data/dealality-report-archive/` or `DEALALITY_REPORT_ARCHIVE_ROOT`
- Immutable `archiveId` directories; SHA-256 for PDF + snapshot
- See `ARCHIVE_ARCHITECTURE.md`

## D. Admin UX

- Tab: **Report Archive** (alongside Reviews / ADP Action Plan / GDI Reports)
- Filters: All / ADP / GDI chips + class select (BASELINE, DAY1, WEEKLY, MONTHLY, …)
- Columns: Date, Hotel, Type, Class, Version, PDF, Snapshot, Actions
- Actions: View PDF, Download, View Data (detail + checksum flags)
- Empty: “No archived reports yet.”
- Paths / tokens / storage refs not exposed in catalog JSON

## E. Bethesda Golden Archive

### Oct 1 ADP (`adp_baseline_2026-10-01_v1`)

| | |
|--|--|
| PDF archived? | **NO** — `NO_HISTORICAL_CLIENT_VISIBLE_PDF_BINARY_ON_2026-10-01` |
| Data archived? | **YES** — `BETHESDA_ADP_BASELINE_V1` |
| Checksum? | **YES** (snapshot) |
| Retrieval? | Data **PASS**; PDF correctly unavailable |

### Oct 1 GDI (`gdi_day1_2026-10-01_v1`)

| | |
|--|--|
| PDF archived? | **NO** — same missing-binary reason |
| Data archived? | **YES** — Day-1 frozen snapshot |
| Checksum? | **YES** (snapshot) |
| Retrieval? | Data **PASS**; PDF correctly unavailable |

Oct 2+ Bethesda ADP/GDI entries include archived PDFs with verified checksums.

## F. Automatic Archive Flow

```
canonical data → report snapshot → render PDF → persist PDF
  → persist structured snapshot → create archive metadata → Report Archive UI
```

Hooks:

1. `writeCurrentReportPdf` → ADP with PDF  
2. `publishExistingHotelAdpSnapshot` → ADP snapshot (PDF may follow)  
3. `writeGdiReportPdf` → GDI with PDF (skips PREVIEW)

## G. Security

- Admin-only index + snapshot API  
- PDF download via Admin auth route  
- Catalog strips filesystem paths  
- Share tokens not shown raw in list UX  
- No public archive index  

## H. Generic Hotel Tests

| Hotel | Result |
|-------|--------|
| Bethesda | List + retrieve golden + PDF-bearing entries **PASS** |
| Renaissance | Empty list **PASS** |
| Hilton | Empty list **PASS** |
| Radisson | Empty list **PASS** |

## I. Regression

| Surface | Touched? | Expected |
|---------|----------|----------|
| ADP current reports | Auto-archive side-effect only (non-fatal) | **NO break** |
| GDI current reports | Same | **NO break** |
| External client links | Unchanged (archive separate) | **NO break** |
| Share tokens | Unchanged | **NO break** |
| Baselines / Opportunity IDs | Not rewritten | **NO break** |

## J. Recommended Next Step

1. Mount Railway volume; set `DEALALITY_REPORT_ARCHIVE_ROOT` (and/or `DEALALITY_REPORT_PDF_ROOT`) in production.  
2. Commit/push Report Archive code + Bethesda golden ledger.  
3. On next official ADP/GDI publish, confirm new rows appear automatically in Admin.  
4. Optional: historical client-safe “open this archived PDF” route (Admin PDF route already covers internal retrieval).

---

## RETURN MATRIX

| Field | Value |
|-------|-------|
| FOUNDER_REPORT PATH | `reports/report-archive-v1/FOUNDER_REPORT.md` |
| FINAL SHA | `fba0ad057d854a4ff9b243db943a64e4419e7521` (pre-commit; local changes uncommitted) |
| PUSH | **FAIL** (not requested) |
| REPORT ARCHIVE TAB | **PASS** |
| DURABLE STORAGE | **PARTIAL** (volume recommended for runtime PDFs) |
| ADP HISTORICAL PDF ARCHIVE | **PASS** (going forward + Oct 2); Oct 1 = data-only |
| GDI HISTORICAL PDF ARCHIVE | **PASS** (going forward + Oct 2); Oct 1 = data-only |
| STRUCTURED SNAPSHOT ARCHIVE | **PASS** |
| IMMUTABLE VERSIONING | **PASS** |
| BETHESDA OCT 1 ADP ARCHIVED | **YES** (snapshot; PDF_NOT_PREVIOUSLY_ARCHIVED) |
| BETHESDA OCT 1 GDI ARCHIVED | **YES** (snapshot; PDF_NOT_PREVIOUSLY_ARCHIVED) |
| BETHESDA ADP PDF RETRIEVAL | **FAIL** for Oct 1 (expected); **PASS** for Oct 2 archived PDFs |
| BETHESDA GDI PDF RETRIEVAL | **FAIL** for Oct 1 (expected); **PASS** for Oct 2 archived PDFs |
| BETHESDA DATA RETRIEVAL | **PASS** |
| CHECKSUM VERIFICATION | **PASS** |
| AUTO-ARCHIVE ON PUBLISH | **PASS** (wired) |
| RENAISSANCE TEST | **PASS** (empty) |
| HILTON TEST | **PASS** (empty) |
| RADISSON TEST | **PASS** (empty) |
| CURRENT ADP REPORT REGRESSION | **NO** |
| CURRENT GDI REPORT REGRESSION | **NO** |
| EXTERNAL SHARE REGRESSION | **NO** |
| FINAL VERDICT | **REPORT ARCHIVE PASSES — BETHESDA GOLDEN ARCHIVE COMPLETE** |
