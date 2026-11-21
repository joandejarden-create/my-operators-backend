# HIF V7 — Permanent No-RFP Commercial Qualification Policy

**Status:** Binding · CUSTOMER_BLOCKED  
**Module:** `lib/hotel-intelligence/sec-edgar/hif-v7-commercial-dimensions-v7.js`

## Rule

A public RFP, advertised tender, bid deadline, formally announced operator search, published franchise expiration, disclosed contracting party, or identified contact person is **not required** for commercial qualification.

A qualifying opportunity **must** have defensible hotel-specific evidence of a meaningful potential brand or management decision.

## Evidence pathways (none superior by RFP alone)

- PUBLIC_COMPETITIVE_PROCESS
- OWNER_LED_PRIVATE_DECISION
- CONTRACTUAL_DECISION_WINDOW
- COMMERCIAL_EVENT_TRIGGER
- RELATIONSHIP_LED_OPPORTUNITY (sourced, never invented)
- OTHER_VERIFIED_PATHWAY

## Independent dimensions

1. **COMMERCIAL_RELEVANCE** — VERIFIED_RELEVANT / EVIDENCE_SUPPORTED_POTENTIAL / INSUFFICIENT_EVIDENCE / NOT_RELEVANT  
2. **COMMERCIAL_TIMING** — ACTIVE_DECISION / 0–6 / 6–12 / 12–24 / 24–36 / LONGER_TERM / UNKNOWN  
3. **COMMERCIAL_ACCESSIBILITY** — ACCESSIBLE_VERIFIED / ACCESSIBLE_PROBABLE / ACCESS_ROUTE_RESEARCH_REQUIRED / ACCESS_RESTRICTED / ACCESS_CLOSED / UNKNOWN  

Missing RFP or unidentified owner → **ACCESS_ROUTE_RESEARCH_REQUIRED**, not ACCESS_CLOSED.

## First-100 metrics (separate)

- COMMERCIALLY_RELEVANT — may count without accessibility or outreach readiness  
- COMMERCIALLY_ACCESSIBLE — verified/probable engagement route  
- OUTREACH_READY — decision party + validated contact + engagement reason  

Adjacent HOTEL_DEVELOPMENT / CONCESSION / REDEVELOPMENT do not inflate principal BCT/MCT counts.

Expiration-only proxies never qualify Priority A / first-100.
