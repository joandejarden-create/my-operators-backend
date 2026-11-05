# AVA — Hotel Facts & Content Discrepancy Review

**Contract:** `AVA_HOTEL_FACTS_AND_CONTENT_DISCREPANCY_REVIEW_V1`  
**Generated:** 2026-11-05 · **Paid:** $0  

---

## Objective

Identify commercially material contradictions and gaps — not an encyclopedia of every amenity.

---

## Register status (six-hotel pilot)

| Hotel | Facts register rows | Material notes |
|-------|--------------------:|----------------|
| Waterstone | 4 | Official site vs historical domain (`waterstoneresort.com` DNS failure) remains a customer-facing identity issue already in V3 findings |
| AC A Coruña | 2 | Brand/Marriott page is operative website; CVB listing present |
| Castillo | 2 | Adults-only (12+) positioning material on Marriott/Booking/Expedia — confirm consistent messaging |
| Sheraton | (payload DCR) | Golf-resort positioning primary; Visit Palma listing present |
| Chapultepec | (payload DCR) | Meetings/Cvent venue listing commercially relevant |
| Condesa | (payload DCR) | All-inclusive beachfront positioning; Google entity still unresolved |

---

## Confirmed / retained discrepancy themes (evidence-backed)

### Waterstone — official domain & meetings narrative
- **Where:** Hotel website / historical domain / Hilton brand page  
- **What:** Domain consistency and meetings-space story reconciliation (existing V3 priority)  
- **Why it matters:** Buyer confidence when searching the property by legacy or brand name  
- **Owner:** Hotel marketing + brand digital  
- **Class:** `VERIFIED_CORRECTION` / `CONTENT_IMPROVEMENT`  
- **Evidence:** https://www.waterstoneboca.com/ · Hilton brand property page  

### AC A Coruña — brand-operated presence
- **Where:** Marriott property page + Booking + CVB  
- **What:** No separate independent hotel site; ensure CVB and OTA facts match Marriott NAP  
- **Class:** `CONTENT_IMPROVEMENT` (consistency), not a missing-website defect  

### Castillo — adults-only policy distribution
- **Where:** Marriott · Booking · Expedia  
- **What:** Adults-only (12+) is commercially material; verify identical guest-policy language across OTAs after exact listings now resolved  
- **Class:** `CONTENT_IMPROVEMENT` / `RESEARCH_FOLLOW_UP` for field-level scrape (access often restricted)  

---

## Correction Register hygiene (important)

V3 Correction Map rows currently include some **access-restricted listing discoveries** that are *not* customer defects. Phase B V4 must filter Correction Map to:

- verified inconsistencies, or  
- missing commercially relevant content, or  
- actionable inclusion/outreach opportunities  

Do **not** present “listing found but bot-blocked” as a hotel correction.

---

## Content assessment honesty

For most brand/OTA pages in this pilot:

| Content status | Meaning |
|----------------|---------|
| `ACCESS_RESTRICTED` | Exact URL known; automated field audit incomplete |
| `CONTENT_PARTIALLY_ASSESSED` | Page reachable; limited HTML inspected |
| `CONTENT_NOT_ASSESSED` | No listing or research incomplete |

Full field comparison vs Hotel Facts Register for every OTA attribute requires authorized Context/browser verification — queued, not faked.
