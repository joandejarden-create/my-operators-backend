# DEALALITY — GDI CSV Export Full Content Parity V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Contract:** `gdi_customer_csv_full_detail_v1`  
**Date:** 2026-11-12

---

## 1. Diagnosis

The Export CSV button and `/opportunities/export.csv` routes already worked (HTTP 200, `text/csv`, filtered scope, auth + share). The content loss was in `buildCustomerExportCsv` / `toCustomerExportRow`: they flattened **list-card** fields only (23 sales columns) and never ran the **detail presentation** pipeline used by the ten-section opportunity drawer.

| Layer | Role | Gap |
|-------|------|-----|
| `app.js` / `share-app.js` `buildExportHref` | Passes current filtered `ids=` (or filter query) | No content loss — scope only |
| Auth `/export.csv` + share `/export.csv` | Customer-facing filter → progression attach → `sendCustomerCsv` | Same builder; previously thin columns |
| `buildExportCsv` → `buildCustomerExportCsv` | CSV body | **Lost drawer narratives, evidence, hotel fit, outreach, contacts detail** |
| `projectOpportunityForPresentation(..., view: "detail")` | Ten-section drawer | Not called by export before this change |

**Not compared against:** internal research budgets, raw model traces, continuation records, or the 48 non-approved historical Bethesda research IDs.

---

## 2. Original 23-column coverage (preserved prefix)

1. Dealality Opportunity ID  
2. Opportunity  
3. Organization  
4. Segment  
5. Priority  
6. Start Date  
7. End Date  
8. Qualification  
9. Booking Window  
10. Demand Territory  
11. Opportunity Summary  
12. Why Now  
13. Recommended Action  
14. Contact Name  
15. Contact Role  
16. Email  
17. Phone  
18. Phone Type  
19. Current Status  
20. Latest Action  
21. Latest Action Date  
22. Latest Outcome  
23. Latest Outcome Date  

---

## 3. New full-detail column contract (+41)

Identification: Commercial Maturity, Revenue Opportunity Type, Demand Confidence  

Intelligence: Opportunity Overview, What Is Happening, Why It Is Happening, Potential Guestroom Business, Potential Catering Meeting Business, Why This Hotel Fits, Why Act Now Full, Known Facts, Uncertainties, Hotel Sourcing Status, Future Cycle Or Overflow, Opportunity Timing  

Contact: Contact Organization, Contact Buying Role, Why Contact Relevant, Buyer Or Intermediary, Organization Contact Route, Contact Source Evidence, Alternate Contacts  

Action: Full Recommended Outreach, Sales Qualification Questions, Timing Recommendation, Commercial Engagement Strategy, Supplier Venue Constraints, Opportunity Risks  

Evidence: Source URLs, Supporting Evidence Summary, Evidence Verification Date, Inferred Assumptions, Uncertainty Disclosures  

Hotel-specific: Hotel Fit Explanation, Need Date Match, Matching Need Period, Revenue Priority Match, Commercial Considerations, Hotel Specific Limitations  

Sales feedback extras: Hotel Validation Status, Hotel Validation Comments  

**Total columns: 64** (23 core + 41 detail).

Enrichment: sync `enrichOpportunityForCustomerExport` → `projectOpportunityForPresentation` detail view. No Airtable N+1. Merge prefers filled source fields so presentation blanks cannot wipe booking window / titles.

---

## 4. Customer-detail → export parity matrix

Suite field checks across all 19 Bethesda customer opportunities (7 substantive fields × 19):

| Classification | Count |
|----------------|-------|
| present_and_complete | 133 |
| source_unavailable | 0 |
| present_in_source_omitted_by_export | **0** |
| present_in_export_but_truncated | **0** |

Artifacts: `parity-matrix.json`, `parity-summary.json`.

Empty export cells where the customer detail also has no text are **source unavailable / not applicable**, not failures (e.g. Known Facts on some rows; Need Date Match when no match object; sales progression when not attached in offline generation).

---

## 5. Before / after — `gdi_opp_ahima_advocacy_2027`

| Field | Before (23-col era) | After (full detail) |
|-------|---------------------|---------------------|
| Columns | 23 list summaries | 64 detail-parity |
| Potential Guestroom Business | *(absent)* | 661-char customer thesis |
| Why This Hotel Fits | *(absent as column; thin list only)* | Full hotel-fit narrative |
| Source URLs | *(absent)* | Prospectus URL exported |
| Full Recommended Outreach | Short “Recommended Action” only | Full next-step narrative |
| Drawer sections | Not exported | Mapped into named columns |

See `before-after-example.json`.

---

## 6–7. Full exported Bethesda CSV

- File: `Bethesda Marriott - GDI Opportunities.csv`  
- Opportunities: **19** (customer-facing only; not 67 / not 48 historical / not unapproved PR #214 candidates)  
- Bytes: **106,588** (~104 KB)  
- Fields added: **41**

---

## 8–10. Missing source / narrative

Always-empty on offline Bethesda file (no progression attach in offline script): Current Status, Latest Action/Date, Latest Outcome/Date, Hotel Validation Status/Comments, Need Date Match, Matching Need Period, Inferred Assumptions — **source unavailable** for this offline artifact. Live auth/share export runs `attachCommercialProgression` and fills feedback when present.

Narrative preservation: multiline cells quoted; max cell length observed **704**; no silent truncation of drawer bodies in parity checks.

---

## 11. Excel compatibility

| Check | Result |
|-------|--------|
| UTF-8 BOM | PASS |
| CRLF + quoted commas / newlines | PASS (`excel-compat-result.json`) |
| 19 data rows × 64 cols aligned | PASS |
| Formula injection (`=+-@`) | Guarded (leading `'`) |
| Filename | `Bethesda Marriott - GDI Opportunities.csv` |

---

## 12. Auth + external share

| Path | Contract |
|------|----------|
| `GET .../hotels/:hotelId/opportunities/export.csv` | `filterCustomerFacing` + Bethesda projection + `filterOpportunitiesForExport` + progression + `sendCustomerCsv` |
| `GET .../share/hotels/:hotelId/opportunities/export.csv` | Same builder; share hotel-scope 403; research-preview blocked |
| Response | `text/csv; charset=utf-8`, `X-GDI-Csv-Contract: gdi_customer_csv_full_detail_v1` |

Export scope = **currently filtered UI rows** via `ids=` (preferred) or filter query params — unchanged user-facing meaning.

---

## 13. Performance

| Hotel | Rows | Duration | Bytes |
|-------|------|----------|-------|
| Bethesda Marriott | 19 | ~192–255 ms | 106,588 |
| Larger sample `recnPB3Q02NO8Ma1o` | 47 | ~334 ms | 250,075 |
| Waterstone | 16 | ~100–218 ms | 63,467 |

No per-opportunity Airtable GET; enrichment is in-process presentation only.

---

## 14. Regression

| Gate | Result |
|------|--------|
| `npm run test:gdi-customer-csv-export` | **14/14 PASS** |
| `npm run test:gdi-live-commercial-quality-v1` | **15/15 PASS** |
| `npm run test:gdi-customer-visibility` | **PASS** |
| Customer publication gates | Unchanged (19 facing) |
| No paid research / no auto-publish | Confirmed |

---

## 15. PR / CI / merge / main SHA

Filled after merge in this deliverable turn.
