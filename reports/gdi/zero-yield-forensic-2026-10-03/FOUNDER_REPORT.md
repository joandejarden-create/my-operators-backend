# FOUNDER REPORT — GDI Zero-Yield Root Cause Forensic
**Date:** 2026-10-03  
**Mode:** Forensic only — no threshold changes, no discovery, no promotions, no ADP/share touches.

## Executive answer

All three subject hotels show **0 customer-ready** under the live contract. This is **not** a single bug. The dominant sequential kill is **FUTURE_TIMING_VALID** (78% cross-hotel reject of entity-valid rows), then **WHO not researched**, then **surface eligibility** on the last survivors — while Bethesda / Renaissance / Hilton still pass the same gates with dated, WHO-enriched evidence.

## Phase 1 — Funnels (structural re-eval; soft-DQ stamps stripped)

### YOTEL (YOTEL Geneva Lake) — discovered 13
```
13 discovered
→ 11 entity valid
→ 4 future
→ 4 geography
→ 4 lodging
→ 4 fit
→ 4 placement
→ 1 WHO
→ 1 contact path
→ 1 summary QA
→ 0 surface eligible
→ 0 customer ready
```

### SPICE (Spice Island Beach Resort) — discovered 77
```
77 discovered
→ 69 entity valid
→ 10 future
→ 10 geography
→ 8 lodging
→ 8 fit
→ 8 placement
→ 2 WHO
→ 2 contact path
→ 2 summary QA
→ 0 surface eligible
→ 0 customer ready
```

### AC (AC Hotel A Coruña) — discovered 32
```
32 discovered
→ 22 entity valid
→ 8 future
→ 6 geography
→ 6 lodging
→ 6 fit
→ 6 placement
→ 1 WHO
→ 1 contact path
→ 1 summary QA
→ 0 surface eligible
→ 0 customer ready
```

## Phase 2 — Top kill gates

### YOTEL
1. **FUTURE_TIMING_VALID** — enter 11, reject 7 (63.6%)
2. **WHO_RESOLVED** — enter 4, reject 3 (75%)
3. **ENTITY_VALID** — enter 13, reject 2 (15.4%)

### SPICE
1. **FUTURE_TIMING_VALID** — enter 69, reject 59 (85.5%)
2. **ENTITY_VALID** — enter 77, reject 8 (10.4%)
3. **WHO_RESOLVED** — enter 8, reject 6 (75%)

### AC
1. **FUTURE_TIMING_VALID** — enter 22, reject 14 (63.6%)
2. **ENTITY_VALID** — enter 32, reject 10 (31.3%)
3. **WHO_RESOLVED** — enter 6, reject 5 (83.3%)

### Cross-hotel (subjects aggregated)
- **FUTURE_TIMING_VALID**: enter 102, reject 80 (78.4%)
- **ENTITY_VALID**: enter 122, reject 20 (16.4%)
- **WHO_RESOLVED**: enter 18, reject 14 (77.8%)
- **SURFACE_ELIGIBLE**: enter 4, reject 4 (100%)
- **IN_GEOGRAPHY**: enter 22, reject 2 (9.1%)

## Phase 3 — Surface eligibility forensic

Rows passing entity+timing+geo+lodging+fit but failing customer-ready: **18**  
See `SURFACE_ELIGIBILITY_FORENSICS.csv`.

Dominant failure pattern: `classifyCustomerSurfaceOpportunity` → demand-generator / market-entity without hotel thesis depth, and/or readiness fields (`who_research_not_attempted`, `summary_quality`, `why_now`, `recommended_action`).

Surface eligibility **bug** (lodgingEvidence flags + thesis still rejected): **YES**  
Soft-DQ stamp notes (operational): 12

## Phases 4–6

See `LODGING_SIGNAL_FORENSICS.md`, `WHO_CONTACT_FORENSICS.md`, `PLACEMENT_FORENSICS.md`.

## Phase 7 — Rejection sample (n=53)

| Class | Count | % |
|---|---:|---:|
| CORRECT_REJECTION | 21 | 39.6% |
| PROMISING_BUT_UNDER-RESEARCHED | 13 | 24.5% |
| BAD_ENTITY | 13 | 24.5% |
| OVERSTRICT_GATE | 4 | 7.5% |
| INSUFFICIENT_DISCOVERY_DEPTH | 2 | 3.8% |

## Phase 8–9

See `CUSTOMER_READY_CONTRACT_AUDIT.md`, `BETHESDA_NYC_COMPARISON.md`.

## Phase 10 — Root cause

- A. DISCOVERY_COVERAGE_PROBLEM
- B. RESEARCH_DEPTH_PROBLEM
- E. WHO/CONTACT CONTRACT TOO STRICT
- F. SURFACE ELIGIBILITY BUG
- J. MIXED

## RETURN CARD

| Key | Value |
|---|---|
| YOTEL LARGEST KILL GATE | FUTURE_TIMING_VALID (7/11) |
| SPICE LARGEST KILL GATE | FUTURE_TIMING_VALID (59/69) |
| AC LARGEST KILL GATE | FUTURE_TIMING_VALID (14/22) |
| CROSS-HOTEL LARGEST KILL GATE | FUTURE_TIMING_VALID (80/102) |
| SURFACE ELIGIBILITY BUG FOUND | YES |
| PUBLIC-DATA CONTRACT UNREALISTIC | NO |
| WHO REQUIREMENT BLOCKING VALID OPPORTUNITIES | YES |
| LODGING REQUIREMENT BLOCKING VALID OPPORTUNITIES | YES |
| PLACEMENT LOGIC BLOCKING VALID OPPORTUNITIES | NO |
| DISCOVERY COVERAGE INSUFFICIENT | YES |
| RESEARCH DEPTH INSUFFICIENT | YES |
| PROMISING BUT UNDER-RESEARCHED COUNT | 13 |
| CORRECTLY REJECTED COUNT | 21 |
| BETHESDA/NYC PIPELINE DIFFERENCE FOUND | YES |
| ROOT CAUSE CLASSIFICATION | A. DISCOVERY_COVERAGE_PROBLEM + B. RESEARCH_DEPTH_PROBLEM + E. WHO/CONTACT CONTRACT TOO STRICT + F. SURFACE ELIGIBILITY BUG + J. MIXED |
| GDI THRESHOLDS CHANGED? | NO |
| DATA MUTATED? | NO |

## FINAL VERDICT

**Zero yield is mostly real under current bags: FUTURE_TIMING kills most candidates first; survivors die on WHO-not-researched then surface depth. YOTEL thin; Spice date-weak. Same contract works at Bethesda/NYC. One surface bug found: boilerplate summaryWhyMatters makes hasExplicitHotelMotionCopy ignore a real overflow thesis (AidEx). Do not lower thresholds — fix that motion-copy scan, stamp dates/future-motion, run WHO/ceiling, stamp lodging mentioned flags from known housing URLs.**

STOP.
