# AVA — Platform Research Completeness Certification

**Contract:** `AVA_PLATFORM_RESEARCH_COMPLETENESS_CERTIFICATION_V1`  
**Generated:** 2026-11-05  
**Engine:** `AVA_PLATFORM_LISTING_DISCOVERY_V2`  
**Pilot cohort:** 6 hotels (see `AVA_SIX_HOTEL_PLATFORM_ASSESSMENT_RESULTS.md`)  
**Paid research this pass:** $0 · **ADP measurements:** unchanged · **Publication / deploy:** not authorized  

---

## Distinction (binding)

| Certificate | What it proves | What it does **not** prove |
|-------------|----------------|----------------------------|
| **Technical PDF certification** | PDF renders; evidence hyperlinks annotate; CQA PASS | That every major platform was field-assessed |
| **Platform research completeness** | Each applicable baseline platform has an evidence-supported assessment **or** documented bounded attempts + unresolved reason | Exact GBP place-ID; field-perfect fact parity; paid-depth inventory |

`IDENTIFIED_NOT_ASSESSED` **alone** is not sufficient for research-completeness PASS.

---

## Completeness rule

For each applicable baseline platform the system must produce one of:

1. `VERIFIED_CONSISTENT` / `VERIFIED_CORRECTION_REQUIRED` / `VERIFIED_OPPORTUNITY` with listing URL + notes; or  
2. `LISTING_FOUND_ACCESS_RESTRICTED` with listing URL (not treated as public defect proof); or  
3. `LISTING_NOT_FOUND_AFTER_RESEARCH` with research log of strategies tried; or  
4. `NOT_APPLICABLE` with applicability reason; or  
5. `RESEARCH_INCOMPLETE` **only** when a specific limitation is disclosed (e.g. Maps search aid without place confirmation) **and** research log is non-empty.

Gate implementation: `platformListingDiscoveryV2.completenessGate` + CQA defect when `pass === false`.

---

## Pilot gate results

| Hotel | Completeness gate | Notes |
|-------|:-----------------:|-------|
| Waterstone Resort & Marina | **PASS** | 6 URLs; Maps incomplete (disclosed); 4 not-found after research |
| AC Hotel A Coruña | **PASS** | 7 URLs including CVB; Maps incomplete (disclosed) |
| Castillo Hotel Son Vida | **PASS** | Visit Palma verified; Booking/Expedia not found after bounded search |
| Sheraton Mallorca Arabella Golf | **PASS** | Brand + Visit Palma; OTA/TA not found at $0 |
| Grand Fiesta Americana Chapultepec | **PASS** | Brand + Booking + Cvent verified; Maps incomplete |
| Fiesta Americana Condesa Cancún AI | **PASS** | Website verified; most OTAs not found after research (honest gap) |

**Aggregate:** 6 / 6 PASS research-completeness for the pilot definition.

---

## Permanent integration checklist

| Requirement | Status |
|-------------|:------:|
| Platform assessment mandatory module (V2) | Implemented |
| Findings merge into `platformCoverageAssessment` | Implemented |
| Correction Map / customer PDF groups | Implemented (quality cohort) |
| Clickable listing URLs in PDF | Implemented where URLs exist |
| CQA verifies correction map + high-priority action evidence + discovery gate | Extended |
| Full E2E hard-fail on incomplete research for all 38 | **Pending founder approval** |
| ADP measurement changes | **None** |
| Customer publication | **None** |

---

## What was explicitly not done

- No fabricated Tripadvisor / Booking / Expedia findings to force PASS.  
- No treating Maps search URLs as verified GBP listings.  
- No unauthorized paid Context / Exa / DataForSEO calls.  
- No 38-hotel rollout.  
- No production deploy / share-token publish.

---

## Certification decision

**PILOT CERTIFIED** for platform research completeness on the six named hotels under zero-cost bounded discovery.

**NOT CERTIFIED** for portfolio-wide Full E2E until founder approves (a) quality PDF drafts for Waterstone / AC / Castillo, and (b) expansion of mandatory V2 discovery into the Full E2E orchestrator with the dual-certificate model above.

---

## Related deliverables

1. `AVA_THIRD_PARTY_PLATFORM_RESEARCH_GAP_ANALYSIS.md`  
2. `AVA_SIX_HOTEL_PLATFORM_ASSESSMENT_RESULTS.md`  
3. `AVA_V3_CUSTOMER_PDF_QUALITY_AND_LINK_CERTIFICATION.md`  
4. `AVA_V3_QUALITY_AND_PLATFORM_RESEARCH_PILOT_RESULTS.json`
