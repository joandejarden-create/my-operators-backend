# GDI PDI-2 — Read-only portfolio API results

Generated: 2026-11-19T13:30:05.073Z

## Safety
- Paid calls: 0 · Airtable writes: 0 · Publication writes: 0 · New tables: 0

## Authorization
- Unauthenticated → 401
- Company-name / fuzzy → rejected
- Cross-operator → 403
- Cancún hotel excluded from Guadalajara portfolio
- Wrong-market hotel request → HOTEL_NOT_IN_PORTFOLIO

## Portfolio intelligence
- Shared market id stable across hotels
- Sheraton vs unknown-meetings → different fit classes
- Confidential feedback stripped from shared packet
- Customer surface hides unpublished / RESEARCH_ONLY rows
- Drawer detail paths returned for existing GDI opportunity routes

## Remaining gaps
- Live Memberstack→operator steward registry (production) not yet provisioned
- PDI-3 portfolio tab UI deferred until founder review of this API
- Brief v2 narrative quality is next Track A slice after this PR
- Airtable production membership read-back still founder-gated

## Next
Founder review → Track A Commercial Intelligence Brief v2; then PDI-3 tab reusing Brief v2 + existing drawer
