# Sheraton Guadalajara Expo — Commercial E2E Remediation V2

**Template:** `GDI_FULL_E2E_HOTEL_TEMPLATE_V1` @ `13f104edb810176a64062c97278772201d9c1e84`  
**Baseline fingerprint (archived):** `60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211`  
**New staged fingerprint:** `6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092`  
**New version:** `gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2`  
**Status:** **STAGED_FOR_FOUNDER_REVIEW** — not CUSTOMER_PUBLISHED

## Root causes (Sheraton ≠ Bethesda filters)

| Symptom | Root cause |
|---------|------------|
| Market Scope Hotel 0 | `inventoryPopulations: ["hotel"]` instead of `SALES_OPPORTUNITIES` |
| Sales Opportunities chips empty | Missing `salesOpportunityQuickFilterKeys` |
| Opportunity Type “All 62” | Raw researchSegment without filter category mapping |
| Only Build Account | Hardcoded action; certified sales-action resolver unused |
| UNKNOWN sourcing | Label not normalized to Hotel Sourcing Unconfirmed |
| Generic expansion narratives | Aimbridge shortlist thesis / expansion filler not demand-rewritten |

These were **presentation + classification pipeline gaps (E/G/H)**, compounded by incomplete commercial differentiation (B) — not solely a UI bug.

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Customer-visible | 62 | 55 |
| Research-stage / demoted | 0 | 7 |
| Quarantined | 0 | 0 |
| Market Scope Hotel | 0 | 55 |
| Market Scope Competitive | 0 | 0 |
| Sales Ready | 0 | 0 |
| Build Account only | 62 | 0 |
| Narratives remediated | — | 53 |

### Commercial actions after remediation
- Qualify Opportunity: 53
- Build Venue Partnership: 2

### Sales Opportunities filter keys
- GUEST_ROOMS: 55
- HOTEL_SOURCING_NOT_CONFIRMED: 55
- LOCAL_CATERING: 17
- BOTH: 17
- FUTURE_GROUP_BUSINESS: 9
- PARTNERSHIP_OPPORTUNITIES: 2

## Sales Ready

Still **0** — commercially justified. Remediation correctly expands **Qualify Opportunity / Partnership / Monitor** differentiation without inventing confirmed buyers or open RFPs.

## Airtable

Dry-run only — **no writes**. Authorize separately before upserting 55 customer rows (existing Airtable hotel count: 0).

## Preview

`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&preview=staged`

**Browser verification: PASSED** — 55/55 cards, 55/55 drawers, Market Scope Hotel **55**, Sales Opportunities chips populated (Guest Rooms / Hotel Unconfirmed / Catering / Future Groups / Partnerships), no Bethesda fallback, no mode banner. New fingerprint loaded (not baseline `60f2d7…`).

## Paid research

**Not executed.** Continuation queue in `15-PAID_RESEARCH_CONTINUATION_QUEUE.json` (~$40 suggested, not authorized).

## Full E2E complete?

**PARTIALLY COMPLETE** — classification/presentation/narrative remediation done at Bethesda structural bar; competitive set, need dates, and verified Sales Ready buyers remain blockers without hotel input / paid research.
