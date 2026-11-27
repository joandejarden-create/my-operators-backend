# Dealality Research Receipt — Permanent Product Principles

**Status:** Permanent brand and design principle  
**Accepted:** 2026-11-27  
**First reference implementation:** Group Demand Intelligence (GDI)  
**Scope of implementation today:** **GDI only**

---

## Purpose

The **Dealality Research Receipt** makes visible the work Dealality performs on behalf of customers: evidence reviewed, intelligence qualified, contacts researched, and commercial outcomes measured — so executives can trust the platform without navigating underlying research systems.

It is a long-term, product-wide brand and design principle. Implementation proceeds **product by product**, using each product’s existing architecture and data contracts.

---

## Permanent Principles

1. **Evidence-based research accountability**  
   Receipt metrics must be grounded in durable research evidence or explicitly marked unavailable. Do not invent activity counts.

2. **Customer-attributable intelligence**  
   Customer-facing receipts show only intelligence authorized for that customer context (hotel / deal / company scope as defined by the product).

3. **Shared versus product-specific research attribution**  
   Distinguish shared market or multi-entity research from work performed for a specific customer entity. Do not attribute a full shared research run to every participant as independently performed work.

4. **Research continuity and reuse**  
   Legitimate reuse of prior research must be recognizable and attributable as reuse — not re-billed or double-counted as new discovery.

5. **Commercial relevance and outcome measurement**  
   Receipts should connect research effort to commercial relevance and, when available, customer-reported or verified outcomes — without collapsing estimated demand into booked or realized revenue.

6. **Research productivity as estimated time saved**  
   Receipts may quantify estimated manual research effort avoided using versioned, activity-specific benchmarks and verified research volumes. Customer copy must distinguish **estimated productivity value** from **verified commercial outcomes** and must never present provisional benchmarks as measured industry facts, redeployable time as payroll savings, or estimated labor value as booked revenue.

7. **Consistent Dealality branding**  
   Naming, tone, and presentation follow Dealality product language and report design standards.

8. **No fabricated metrics or financial assumptions**  
   Never fabricate research statistics, contact counts, demand estimates, or financial recovery figures. Hardcoded ADR scenarios, synthetic hotel salary assumptions, and unverified labor rates are prohibited as customer facts.

---

## Implementation Governance (binding)

| Rule | Requirement |
|---|---|
| Current build scope | **GDI only** (Executive Summary / Research Receipt V1) |
| Do not implement now | ADP, Hotel Explorer, Deal Opportunities Research Receipts |
| Do not build | Centralized cross-product reporting engine, shared reporting dashboard, or premature abstraction layer |
| Architecture | Use the **existing product’s** contracts, stores, visibility gates, and UI patterns |
| Future products | When ADP, Hotel Explorer, or Deal Opportunities enter a dedicated development cycle, **revisit these principles** and implement a **product-specific** Research Receipt (including Research Productivity) appropriate to that product’s functionality |
| Research Productivity | GDI-only today via `gdi-research-effort-benchmark-contract-v1` + `gdi-research-productivity-v1`. Document the concept for ADP / Hotel Explorer / Deal Opportunities — **do not** build a cross-product reporting engine |
| Reference | GDI is the **first reference implementation**, not a shared runtime dependency for other products |

---

## GDI reference (Phase 1)

| Artifact | Path |
|---|---|
| Contract | `lib/group-demand-intelligence/executive-summary/gdi-research-receipt-contract-v1.js` |
| Effort benchmarks | `lib/group-demand-intelligence/executive-summary/gdi-research-effort-benchmark-contract-v1.js` |
| Productivity engine | `lib/group-demand-intelligence/executive-summary/gdi-research-productivity-v1.js` |
| Aggregation | `lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v1.js` + `gdi-executive-summary-v2.js` |
| API | `GET /api/group-demand-intelligence/hotels/:hotelId/executive-summary` |
| Closeout | `reports/gdi-executive-summary-v1/PHASE_1_FOUNDATION_CLOSEOUT.md` |
| Productivity map | `reports/gdi-executive-summary-v2/02-RESEARCH_PRODUCTIVITY_METRIC_MAP.md` |
| Auth | `FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127` / V2 |

Documenting these principles does **not** expand Phase 1 scope, budget, or deliverables.

---

## Related

- Product constitution: [DEALALITY_PRODUCT_CONSTITUTION.md](./DEALALITY_PRODUCT_CONSTITUTION.md)
- Build decisions: [BUILD_DECISIONS.md](./BUILD_DECISIONS.md)
- Naming / copy: [NAMING_AND_COPY_GUIDE.md](./NAMING_AND_COPY_GUIDE.md)
- Intelligence governance: [../data-intelligence/INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)
