# AI Demand Leak Audit — Template Lock V1

**Status:** LOCKED for Phase 2A Airtable-backed generation  
**Date:** 2026-09-07  
**Shell:** `/adp-leak-audit/sample` · `/adp-leak-audit/:reportId` · `/adp-leak-audit/share/:shareToken`  
**Layout mode:** `three_page_v1`

This document locks the approved 3-page free diagnostic template. Do not redesign structure, section order, or ADP-native chrome unless fixing a defect or an explicitly scoped copy/CSS polish.

---

## Final PDF structure

| Page | Role | Required content |
|------|------|------------------|
| 1 | Cover | BAS/HID Dealality cover · hotel title · location · limited diagnostic disclaimer · Page 1 of 3 |
| 2 | Executive Diagnostic | Executive Summary · Executive Signal (5 KPIs) · Demand Area to Review · Competitors Showing Up Instead · Supporting Evidence · Page 2 of 3 |
| 3 | Action + Conversion | Priority AI Demand Improvements · How We Partner on These Improvements · Book Your ADP Walkthrough · Page 3 of 3 |

Exactly **3** A4 pages. No page 4 spill.

---

## Final report sections (web + PDF)

1. Cover  
2. Executive Summary  
3. Executive Signal (AI Consideration, Scenario Presence, Reality Coverage, Leisure Advantage, Primary Area to Review)  
4. Demand Area to Review  
5. Competitors Showing Up Instead  
6. Supporting Evidence (+ Scenarios Monitored card)  
7. Priority AI Demand Improvements (3 actions)  
8. How We Partner on These Improvements (4 steps)  
9. Book Your ADP Walkthrough  

---

## Final web behaviors

- **How to read this report** opens `AdpGuidedReportTour` with Leak Audit step pack (guide + Stronger/Weaker + Look Next on every step).  
- KPI info icons open ADP-style tooltips (no evidence links on KPI cards).  
- Supporting Evidence cards open the ADP-style evidence drawer.  
- Sample and live reports share one renderer: `public/adp-leak-audit-report.html` + `adp-leak-audit-report.js` + `adp-leak-audit-shared-ui.js`.  
- Client payloads must remain free of full prompts, prompt IDs, production hotel IDs, and Airtable IDs.

---

## Final PDF behaviors

- Cover fills full A4 (navy geometric, no white gap).  
- Pages 2–3 are navy ADP product surfaces with forced light ink.  
- Cover hotel title preserves word spaces in the PDF text layer (`ala-cover-title-word` + nbsp spacers; print `letter-spacing: 0`).  
- Info icons / web-only chrome hidden in print.  
- Exactly 3 pages.

---

## Final styling source references

| Concern | Source |
|---------|--------|
| Cover geometry | `public/css/brand-alignment-snapshot.css` |
| Cover print fidelity | `public/css/hotel-intelligence-dossier.css` |
| Print chrome / logo | `public/js/dealality-report-print-chrome.js`, `public/css/dealality-report-print-chrome.css` |
| KPI + tooltips | `public/js/ai-visibility/ai-visibility-shared.css` |
| Guided tour modal | `public/js/ai-demand-positioning/adp-guided-report-tour.*` |
| Leak navy pages 2–3 + print densify | `public/css/adp-leak-audit-report-v2.css` |

---

## Locked copy notes (sample)

- Executive Summary uses **Dealality should help prepare** (not “should prepare”).  
- CTA uses **If useful, the next step is to schedule…** (no “Reply now”).  
- Partnership step 3 body stays compact: Website, OTAs, GBP, TripAdvisor.
- **Scenarios Monitored:** value `15 × 4`, sub-label `60 observations` (not `60 × 4 Providers`).  
- Cover meta: `PROVIDERS 4 · SCENARIOS 15 · OBSERVATIONS 60 · ACTION ITEMS 3`.  
- Free audits use research mode `leak_audit_lite` (`adp_lite_leak_audit` internal) — never full paid ADP.

---

## Remaining known limitations

- Live provider calls remain optional/admin-gated; Phase 2A generation can seed from copied ADP or sample-shaped records.  
- Airtable physical base may still be filesystem-backed until migration apply; contracts are documented in `AIRTABLE_SCHEMA_V1.md`.  
- Portfolio shell remains a separate HTML entry that reuses shared UI patterns; single-property shell is the locked 3-page PDF reference.

---

## Phase 2A statement

**Template is locked.** Generation uses Airtable schema mapping + filesystem live repository (`leak-audit-repository.js`). See `AIRTABLE_GENERATION_QA.md` and `PHASE_2A_HANDOFF.md`.
