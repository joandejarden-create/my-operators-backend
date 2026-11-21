# HIF V6 Track B — HM Land Registry activation

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Spend:** $0  

## Phase 1 — Authenticated access

| Item | Result |
|------|--------|
| `HM_LAND_REGISTRY_KEY` | PRESENT (length 36; value not logged) |
| Product | **Use land and property data** (ULPD) REST API |
| Base URL | `https://use-land-property-data.service.gov.uk/api/v1` |
| Auth model | API key in `Authorization` header |
| Auth probe `GET /datasets` | **AUTHENTICATED_OK** |
| Datasets listed | ccod, ocod, nps, nps_sample, res_cov, leases |
| CCOD download ticket | **LICENCE_NOT_SIGNED** |
| OCOD | Licence required (same pattern as CCOD) |
| NPS / leases / res_cov | Permission denied or unpaid — **not** used |
| Business Gateway (live title register) | **NOT** granted by this key |
| `COMPANIES_HOUSE_API_KEY` | **ABSENT** (Land Registry research not blocked) |

### What this credential permits

- List datasets and read metadata — **yes**
- Download **CCOD / OCOD** bulk corporate ownership CSVs — **only after signing free dataset licences** on the ULPD website
- National Polygon Service — **paid / no access** (founder authorization required)
- Per-address official copies / live proprietor API — **no** (that is Business Gateway + fees)

## Phase 2–4 — Property ownership research

| Metric | Count |
|--------|------:|
| Hotel properties prepared for investigation | 8 |
| Strong title/address matches | 0 |
| Ambiguous postcode-only matches | 0 |
| Verified legal property owners | **0** |
| Corporate entities resolved to CH numbers | 0 |
| Independently corroborated hotel→owner links | **0** |

### Blocker (action required)

Authenticated API works, but downloads return:

> You have not yet signed the licence for this dataset: ccod

**Founder action:** sign in at [Use land and property data](https://use-land-property-data.service.gov.uk/), agree the **CCOD** and **OCOD** licences (free), then re-run `node scripts/hif-v6-track-b-hmlr-ownership-research.mjs`.

No automatic paid title purchases were attempted.


## Governance

- CUSTOMER_BLOCKED · no HPC/Meta writes · no customer publication
- Credential never printed in reports
- Operator/franchisee not treated as legal proprietor
- CH company-number resolution deferred until verified proprietor rows exist

## Recommended next steps

1. Sign CCOD + OCOD licences (free) on ULPD — unlocks ~4.5M UK corporate title rows.
2. Re-run ownership matcher against HE UK hotels with postcodes (prefer COU then FULL).
3. For each strong postcode+name hit: treat as **candidate** until HE identity + title number corroboration.
4. Resolve proprietor company numbers via Companies House (set `COMPANIES_HOUSE_API_KEY` for scale; public HTML remains available).
5. Do **not** buy NPS or Business Gateway official copies without a specific budget ask.
