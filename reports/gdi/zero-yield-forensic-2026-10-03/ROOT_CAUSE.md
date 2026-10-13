# Root Cause — GDI Zero Yield Forensic 2026-10-03

## Classification

- A. DISCOVERY_COVERAGE_PROBLEM
- B. RESEARCH_DEPTH_PROBLEM
- E. WHO/CONTACT CONTRACT TOO STRICT
- F. SURFACE ELIGIBILITY BUG
- J. MIXED

## Evidence

- YOTEL bag n=13; entity survivors 11
- Sample PROMISING_BUT_UNDER-RESEARCHED=13/53; WHO NOT_RESEARCHED high on subjects
- who_research_not_attempted / WHO_RESOLVED blocks 17 otherwise-advanced rows (named person not required but stamp missing)
- FUTURE_TIMING_VALID rejects 80 across subjects (unknown/past dates without future-motion stamps) — largest cross-hotel kill
- hasExplicitHotelMotionCopy ignores hotelOpportunityThesis when summaryWhyMatters is boilerplate — AidEx-class rows: 1

## Largest kill gates

| Hotel | Gate | Entering | Rejected | Reject % |
|---|---|---:|---:|---:|
| YOTEL | FUTURE_TIMING_VALID | 11 | 7 | 63.6% |
| SPICE | FUTURE_TIMING_VALID | 69 | 59 | 85.5% |
| AC | FUTURE_TIMING_VALID | 22 | 14 | 63.6% |
| CROSS | FUTURE_TIMING_VALID | 102 | 80 | 78.4% |

## Funnel chains (structural, soft-DQ stripped for evaluation)

**YOTEL:** 13 DISCOVERED → 11 ENTITY_VALID → 4 FUTURE_TIMING_VALID → 4 IN_GEOGRAPHY → 4 LODGING_SIGNAL_PRESENT → 4 HOTEL_FIT_PASS → 4 PLACEMENT_PASS → 1 WHO_RESOLVED → 1 CONTACT_PATH_FOUND → 1 SUMMARY_QA_PASS → 0 SURFACE_ELIGIBLE → 0 CUSTOMER_READY

**SPICE:** 77 DISCOVERED → 69 ENTITY_VALID → 10 FUTURE_TIMING_VALID → 10 IN_GEOGRAPHY → 8 LODGING_SIGNAL_PRESENT → 8 HOTEL_FIT_PASS → 8 PLACEMENT_PASS → 2 WHO_RESOLVED → 2 CONTACT_PATH_FOUND → 2 SUMMARY_QA_PASS → 0 SURFACE_ELIGIBLE → 0 CUSTOMER_READY

**AC:** 32 DISCOVERED → 22 ENTITY_VALID → 8 FUTURE_TIMING_VALID → 6 IN_GEOGRAPHY → 6 LODGING_SIGNAL_PRESENT → 6 HOTEL_FIT_PASS → 6 PLACEMENT_PASS → 1 WHO_RESOLVED → 1 CONTACT_PATH_FOUND → 1 SUMMARY_QA_PASS → 0 SURFACE_ELIGIBLE → 0 CUSTOMER_READY

## Controls

**BETHESDA:** 54 → … → surface 30 → ready 30 (stamped facing 35)

**RENAISSANCE:** 29 → … → surface 9 → ready 9 (stamped facing 11)

**HILTON:** 45 → … → surface 11 → ready 11 (stamped facing 12)

## Final verdict

Zero customer-ready on YOTEL / Spice / AC is **primarily MIXED: (1) FUTURE_TIMING_VALID as the largest cross-hotel kill (unknown/past dates without future-motion stamps), (2) thin/noisy discovery, (3) WHO research not attempted on nearly all survivors, (4) surface thesis depth killing the last 1–2 rows**. Same contract Bethesda/NYC already satisfy. Placement/fit are not systemic killers. No threshold change recommended.

## Mutations

- GDI thresholds changed? **NO**
- Data mutated? **NO**
