# GDI PDI-2 — Security amendment results

Generated: 2026-11-19T13:35:46.283Z

## Before → After

| Vulnerability | Mitigation | Regression |
|---|---|---|
| Unauthenticated access | serverVerified required | PASS |
| Spoofed member/session headers | callerFromVerifiedRequest flags + ignores | PASS |
| Query-supplied sessionId | ignored; not used for auth | PASS |
| Company-name impersonation | FUZZY_NAME_MATCH_REJECTED | PASS |
| Cross-operator access | CROSS_OPERATOR_DENIED | PASS |
| Missing operator registry member | OPERATOR_NOT_MAPPED | PASS |
| Missing market allowlist | MARKET_ALLOWLIST_REQUIRED (never broadens) | PASS |
| Empty market allowlist | denies all markets | PASS |
| Wrong-market hotel (Cancún under Guadalajara) | HOTEL_NOT_IN_PORTFOLIO | PASS |
| Opportunity with no marketId | excluded from corpus | PASS |
| Research-only without customerSurface (default customer) | unpublished/research-only filtered by default | PASS |
| customerSurface=0 bypass attempt | requires GDI_INTERNAL_RESEARCH_READ | PASS |
| Market-level publication without hotel-level authorization | hotel rows omitted; no drawer | PASS |
| Unpublished hotel drawer link withheld | drawer.detailPath null | PASS |
| Fixture auth on non-test runtime | live path fail-closed without data/ | PASS |
| Valid authorized customer access after corrections | Sheraton published only | PASS |

## Safety
Paid/Airtable/publication writes: **0**. Fixtures never loaded on deployed HTTP path.

## Next
Founder review gate → Integration Controller merge → Track A Brief v2 plan execution.
