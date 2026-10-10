# ADP_PDF_QA.md

## Status: PASS

### Root cause of prior FAIL

`adp-current-report-pdf-render.html` missing (404) **and** `authFetch` blocked on Memberstack (`waitForLogin`) so `data-adp-pdf-ready=1` never set.

### Fixes

1. Restored render HTML + PDF-export chrome hide CSS  
2. `authFetch` short-circuits for `pdfRender=1` (no Memberstack)  
3. Admin POST `/current-report-pdf/:propertyId/generate` + Action Plan View PDF uses current ADP PDF (generate-on-miss)

### Inspected content (production render)

| Check | Result |
|---|---|
| Hotel | Bethesda Marriott |
| AI Consideration | 42.1% |
| Scenario Presence | 81% |
| Top-3 / #1 / Reality | 76.7 / 33.3 / 26.7 present in published payload |
| Baseline / October 1 | Present |
| Internal IDs | Absent |
| Load Report chrome | Hidden |
| Pages | 6 |
| Bytes | 321,729 |
| SHA-256 | `ac07b5656c750d253138ad0df0c474428ed797bd0d6fc5fac99e198fba5ca00c` |

### Historical Oct 1 PDF

**No client-visible Oct 1 binary existed.** Not fabricated. Structured archive only: `adp_baseline_2026-10-01_v1`.  
Current client PDF archived as `adp_current_2026-10-02_v1`.
