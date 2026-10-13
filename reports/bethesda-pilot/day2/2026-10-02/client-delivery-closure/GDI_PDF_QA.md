# GDI_PDF_QA.md

## Status: PASS (production)

### Binary

| Field | Value |
|---|---|
| Production status | 200 |
| Magic | `%PDF-` |
| Bytes | 243,766 |
| Pages | 16 |
| SHA-256 | `c554c92f3558ffb52b70e52b9177a043f0afc9a4b6518e7f48a943ec0b470236` |
| Archive | `gdi_current_2026-10-02_v3` (supersedes v2) |

### Cover

- Confidential banner + GROUP & DEMAND INTELLIGENCE doc-type  
- Bethesda Marriott  
- Washington metropolitan area (not raw DMV)  
- Human date (October 2, 2026)  
- Shared DealalityReportPrintChrome footer / page numbers  

### Content QA

| Issue | Result |
|---|---|
| RNA duplicate watch | Collapsed to one: “2027 NCI RNA Biology Symposium — Hotel & Travel open” |
| Malformed nav title | Scrubbed (no “Skip to main content”) |
| ACTS Adam Joyce vs kstelmaszak@… | Provenance note: organization contact path ≠ named person |
| ACC Legislative segment | Association (generic taxonomy) |

### Day-1 history

Structured snapshot `gdi_day1_2026-10-01_v1` ready=**37** preserved; no invented Oct 1 PDF.
