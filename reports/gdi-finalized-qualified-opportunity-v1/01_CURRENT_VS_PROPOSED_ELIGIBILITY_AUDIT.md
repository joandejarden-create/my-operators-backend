# Current vs proposed eligibility — Finalized Qualified Opportunity

**As of:** 2026-11-24  
**Contract:** `gdi-finalized-qualified-opportunity-contract-v1`

## Current (pre-FQO)

| Layer | Controlling signal | Problem |
|-------|--------------------|---------|
| Commercial eligibility | `salesReady` / `SALES_READY` | Allows org-route-only contact paths; often treated as the “ready” bar even when local review only needs a reachable named person |
| Named coverage | ≥1 named person | Does not require person-specific email/phone/LinkedIn |
| Local UI | STAGED default on GDI iframe, but shell defaulted to **Research** | Research store leaked into local presentation; empty STAGED fell back to research workflows / tabs |
| Publication packages | Quality gate named-contact + prose | No person-specific channel gate; Sales Ready not required for package bytes but scoring still centered on SR |

## Proposed (FQO)

Local STAGED candidacy requires **all** of:

1. Evidence-backed demand  
2. Hotel geo/commercial fit  
3. Valid future sourcing / next-cycle pathway  
4. Verified commercially relevant named person  
5. **Person-specific** professional channel (direct email / direct phone / person LinkedIn)  
6. Hotel-specific recommended commercial action  

Competitive also requires incumbent + defensible overflow/future pathway.

**Sales Ready is not required** for FQO. SR fields are preserved for scoring compatibility.

## Compatibility

- Historical STAGED/CURRENT packages: **not rewritten**; quality-gate FQO enforcement is **opt-in** (`enforceFinalizedQualifiedOpportunity: true`) for new candidacy.
- Research store remains internal (`?gdiAdminResearch=1&live=1` only).
- No automatic STAGED/CURRENT pointer activation in this PR.
