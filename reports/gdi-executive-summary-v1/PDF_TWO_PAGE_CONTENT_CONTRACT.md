# GDI Executive Summary — Two-Page PDF Content Contract (Phase 1 design)

**Status:** Design contract only — **PDF exporter not implemented in Phase 1**  
**Authorization:** `FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127`  
**Report family:** Existing Dealality report family (cover + briefing)

---

## Hard constraint

Exactly **two pages**. Future validation must use a reliable PDF page-count mechanism and **fail closed** if page count ≠ 2.

| Page | Content | Source (Phase 1 payload) |
|---:|---|---|
| 1 | Standard Dealality cover | Existing report-family cover (hotel name, period, Dealality branding) |
| 2 | One-page Executive Commercial Briefing | Compact Executive Summary sections |

Third page prohibited. Overflow must reduce density, not spill pages.

---

## Page 2 — Executive Commercial Briefing (one page)

Must fit, in order:

1. **Hotel + reporting period + data cutoff**  
2. **Executive Commercial Snapshot** — supported KPIs only (omit unsupported; never fabricate)  
3. **Research Receipt (compact)** — use `buildCompactResearchReceiptForPdf`  
   - Availability-aware; unavailable metrics shown as `—` with short reason  
   - Distinguish SHARED_MARKET / HOTEL_SPECIFIC / REUSED  
4. **Opportunity of the Week** + up to **4 additional** priority opportunities (≤5 total)  
5. **Commercial engagement** — only metrics with existing pursuit/feedback records  
6. **Footer** — publication pointer (CURRENT), “not financial advice”, Dealality mark  

**Omit from PDF (Phase 1+):**

- Internal provider costs / job ledgers  
- Restricted contact PII beyond customer-authorized presentation  
- Investment recovery numbers until hotel inputs authorized  
- STAGED-only inventory for customer PDFs  
- Synthetic ADR (165 / 195 / 235) or fabricated ROI  

---

## Validation plan (future phase — not Phase 1)

```text
1. Generate PDF via Dealality report-family exporter
2. Assert pageCount === 2 (Playwright PDF or pdf-lib / pdfjs page count)
3. Fail closed on pageCount !== 2
4. Spot-check: no hardcoded ADR strings; Research Receipt compact keys present
5. Customer PDF only when dataState === CURRENT_AVAILABLE
```

Phase 1 payload already records:

```json
"pdfPreparation": {
  "implemented": false,
  "pagesRequired": 2,
  "page1": "Dealality branded cover (existing report-family)",
  "page2": "Executive Commercial Briefing (one page)",
  "thirdPageProhibited": true
}
```

---

## Compact Research Receipt fields for Page 2

Prefer these keys when AVAILABLE_* (skip TELEMETRY_REQUIRED / HOTEL_INPUT_REQUIRED):

- uniqueOpportunitiesDiscovered  
- whoHowResearchPerformed  
- verifiedDirectBuyers  
- verifiedBuyerInfluencers  
- verifiedAlternativeEntryPoints  
- verifiedPersonSpecificOutreachRoutes  
- newlyDiscoveredIntelligence  
- materiallyChangedIntelligence  

Do not zero-fill unmeasured shared-market signal counts.
