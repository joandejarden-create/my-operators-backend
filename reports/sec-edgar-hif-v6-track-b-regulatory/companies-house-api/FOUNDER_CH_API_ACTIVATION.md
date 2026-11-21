# Companies House API activation — IC status

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Spend:** $0

## Access validation

| Check | Result |
|---|---|
| REST base reachable | Yes (`api.company-information.service.gov.uk`) |
| Unauthenticated company profile GET | **HTTP 401** (`Empty Authorization header`) |
| `COMPANIES_HOUSE_API_KEY` in local `.env` | **Absent** (at sample time) |
| Credentials committed | **No** |

## Required founder setup (authorized key)

1. Sign in at https://developer.company-information.service.gov.uk/
2. Create a **Live** application (Dealality HIF research).
3. Create a **REST** API key (not stream).
4. Store only in local `.env` as `COMPANIES_HOUSE_API_KEY` (never commit; never paste into reports, chat, or CI logs).
5. Authentication: HTTP Basic with the REST API key as the username and an empty password. Prefer the HIF client (`lib/hotel-intelligence/sec-edgar/companies-house-client-v1.js` when present) or an Authorization header built at runtime from the environment variable — do **not** paste keys into shell history examples in docs.

Official docs: https://developer.company-information.service.gov.uk/authentication/  
Specs: https://developer-specs.company-information.service.gov.uk/

## Public HTML sample (lawful interim)

Without the API key, IC enriched **15 / 61** wave-1 candidates via public company overview pages:

- Legal identities confirmed: **15**
- Active: **8** · Dissolved: **7**
- SIC 55100 (hotels) present: **7**
- Verified hotel-property relationships: **0**
- Verified franchise/management expirations: **0**

Governance preserved: registered office ≠ hotel; company-name “Hotel Management” ≠ property manager; PSC ≠ verified property owner without property-level evidence.

## Next research wave (after key)

1. Authenticated company profile + filing history + charges + officers for Active + SIC 55100 cohort.
2. Extract hotel-named filings / charges; independent property match to Hotel Explorer identities.
3. Expand BR CVM / JP EDINET / SGX / HKEX / SEDAR+ beyond access probes into hotel-specific disclosures.
4. Update weekly regional scorecard with only independently verified increments.

## Gitleaks note (PR #509)

A prior documentation example used a shell Basic-auth form that tripped the `curl-auth-user` rule. That line referenced only an environment-variable placeholder — **not a live credential**. Rewritten above to avoid the false-positive pattern without weakening repository secret scanning.
