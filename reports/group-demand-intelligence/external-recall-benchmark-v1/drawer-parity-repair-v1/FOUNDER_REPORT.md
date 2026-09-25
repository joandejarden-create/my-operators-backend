# GDI New-Opportunity Drawer Parity Repair V1 — Founder Report

**Verdict: GDI DRAWER PARITY PASSES — SOME FIELDS UNKNOWN AFTER RESEARCH**

---

## A. PROBLEM

| Metric | Before |
|--------|--------|
| NEW CUSTOMER-VISIBLE OPPORTUNITIES | ~8–12 thin V3 rows |
| DRAWER-COMPLETE BEFORE | 0 |
| THIN | 12 |
| IDENTITY DEFECT | segment=`assoc_development_district_ass` |
| GEO DEFECT | AAO `venueStatus` = Las Vegas, NV |

## B. ROOT CAUSE

| | |
|--|--|
| PRIMARY | V3 `promoteQualifiedGdiOpportunity` wrote thin candidates with generic thesis/whyNow and **skipped** `buildOpportunity` / `enrichQualificationPrecision` |
| SECONDARY | Discovery vertical slug leaked into `segment`; city string stuffed into `venueStatus` |
| Bypassed mature enrichment? | **YES** |
| Serializer different? | **NO** — same API serializer; thin persisted fields |
| Research incomplete? | **YES** |

## C. GOLD DRAWER CONTRACT

| Section | Mature Path | New Path Before | New Path After |
|---------|-------------|-----------------|----------------|
| Summary / segment | Human segment | Internal slug | Sanitized label |
| Opportunity type | Precision-derived | Raw PRIMARY_PURSUIT | Factory + precision |
| Venue / sourcing | Classified | UNKNOWN | Classified / researched |
| Thesis | Evidence-based | Generic boilerplate | Rebuilt or insufficient |
| Why this hotel | Hotel-specific | Thesis duplicate | Distinct hotel-fit copy |
| Why now | Timing trigger | "Future cycle 2027" | Evidence-based watch trigger |
| Hotel fit | Scored | Missing / 0 | Default components + score |
| Room demand | Status + rationale | Empty UNKNOWN | UNKNOWN_AFTER_RESEARCH + plain English |
| Action | Typed | Generic monitor | State-appropriate action |

## D. AAO FORENSIC

| Field | Result |
|-------|--------|
| ORGANIZATION | American Academy of Ophthalmology (correct) |
| `assoc_development_district_ass` | **Discovery vertical slug** from SERP task family (DDAA batch contamination) — **not** org ID |
| EVENT SERIES | `series:annual_meeting` (weak generic) |
| DESTINATION | Las Vegas, NV (from mis-filed `venueStatus`) |
| VENUE STATUS | FUTURE_CYCLE / classified |
| ROOM DEMAND | UNKNOWN after research |
| HOTEL FIT | Scored; geo outside catchment |
| WHY THIS HOTEL / WHY NOW | Enriched then **DQ** |
| INTERNAL ID LEAK | **NO** after repair |
| CUSTOMER RELEVANT | **NO** — definitive Las Vegas cycle, no Bethesda housing/overflow evidence |
| VISIBLE AFTER | **false** (DISQUALIFIED) |

Las Vegas is cycle destination context (not Bethesda motion) → correctly removed from customer-visible bag.

## E. ALL NEW OPPORTUNITIES

| | Count |
|--|------:|
| TOTAL researched | 11 |
| DRAWER READY | 10 |
| DOWNGRADED / DQ (AAO geo) | 1 |
| LEAKS AFTER | 0 |
| CTN / HRHR / NCI / DDAA restored | visible WATCHLIST |

## F. COMPLETENESS (new visible)

Event identity, venue/sourcing, thesis, why hotel, why now, hotel fit, action: **repaired**.  
Attendance / peak rooms: **UNKNOWN_AFTER_RESEARCH** where not public (acceptable).

## G. INTERNAL ID LEAKS

BEFORE: ≥1 (`assoc_development_district_ass`) · AFTER: **0**

## H. GEO CONSISTENCY

MISMATCHES: 1 (AAO) · FIXED/DQ: 1 · Bethesda-core (CTN/NCI/HRHR): retained

## I. CUSTOMER DRAWER

MATURE: PASS · NEW: PASS (same enrichment path + schema) · SAME SCHEMA: YES · SAME ENRICHMENT PATH: YES

## J. DECISIONS

1. Thin drawers = promote skipped mature enrichment + generic boilerplate.  
2. Yes — bypassed `buildOpportunity` / precision.  
3. AAO org name OK; segment was wrong vertical slug (not org bind).  
4. Las Vegas = destination signal for that cycle — not Bethesda-relevant.  
5. AAO should **not** remain visible for Bethesda after CQ/geo.  
6. Room demand researched; unknown stated plainly when not public.  
7. Why This Hotel is hotel-specific (no thesis echo).  
8. Why Now has timing/source trigger (not bare year).  
9. Contacts still org-path / limited where dual-extract not run; Surfe 0.  
10. Internal IDs removed from customer copy (+ UI/API guards).  
11. New opps use same enrichment contract as established.  
12. Yes — AAO DQ’d; others restored after false fit=0 DQ fixed.  
13. Parity regression fixed for enrichment/serialization/UI; some fields remain legitimately unknown.

## K. FINAL VERDICT

**GDI DRAWER PARITY PASSES — SOME FIELDS UNKNOWN AFTER RESEARCH**

---

### Persistence

CODE: `enrich-gdi-opportunity-for-customer.js`, `promote-qualified-opportunity.js`, API segment sanitize, UI drawer segment/why-hotel, repair + test scripts  

TESTS: `test:gdi-drawer-parity-repair-v1`  

HARDCODES: 0 · SURFE: 0 · WEBHOUND: 0 · JEV PROD: NO  
