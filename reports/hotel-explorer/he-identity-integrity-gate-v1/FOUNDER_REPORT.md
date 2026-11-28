# Hotel Explorer Identity Integrity Gate V1 — Founder Report

**Classification:** `HE_IDENTITY_INTEGRITY_CERTIFIED`  
**Date:** 2026-11-28

## Root cause of blank-ID API requests

`HotelExplorer.open(bareString)` passed a string into `mergeHotelState` / `Object.assign`. Character-index properties replaced a real hotel object, blanking `id` / `recordId`, which produced:

- `GET /api/contact-intelligence/hotels/?audience=internal`
- `GET /api/golden-demo/ownership/hotel/`

**Fix:** `normalizeHotelInput` + fail-closed UI (`HOTEL_IDENTITY_UNRESOLVED`); contact/ownership clients refuse empty IDs; ownership payloads with mismatched hotel IDs are dropped (`STALE_OWNERSHIP_IDENTITY_DROPPED`).

## Permanent gates

| Gate | Result |
|---|---|
| HE_CANONICAL_HOTEL_ID_VALIDATED | PASS |
| HE_IDENTITY_PROPAGATION_VALIDATED | PASS |
| HE_API_HOTEL_ID_PARITY | PASS |
| HE_API_RESPONSE_HOTEL_PARITY | PASS |
| HE_RENDERED_HOTEL_IDENTITY_VALIDATED | PASS |
| HE_CROSS_HOTEL_CACHE_ISOLATION | PASS |
| HE_NO_BLANK_HOTEL_REQUESTS | PASS |
| HE_ALL_TABS_IDENTITY_VALIDATED | PASS |

Wired into `Invoke-HotelExplorerFactory.ps1` for Audit / Validate / Full / Refresh / CrossTabValidate. Identity FAIL keeps `FULL_HE_E2E_VALIDATED` NO.

## Per-tab matrix (Bethesda)

All 11 active tabs: **PASS** (identity resolution + API request identity + rendered identity). Guest Sentiment disabled / N/A.

## API request/response parity

Live read-only:

- Contact: `…/hotels/recLuxvwwxID7U2B8` → 200, body hotel id matches  
- Ownership: `…/ownership/hotel/recLuxvwwxID7U2B8` → 200, body hotel id matches  

## Bethesda ↔ AC Hotel switching

Bethesda → AC Newtown Square → Bethesda: all **ok**, zero blank URLs, zero cross-hotel URL mismatches.

## Rapid-switch race

Delayed stale Bethesda ownership response after switch to AC: **raceOk=true**; stale payload not rendered.

## Eight-hotel coverage

All eight HE50 HPC IDs: open + render identity **ok**, blank URL count **0**.

## Evidence

`reports/hotel-explorer/he-identity-integrity-gate-v1/`  
Command: `npm run test:he-identity-integrity-gate-v1 -- --browser`

## PR / 8080

Filled after PR open. Pre-merge 8080 may use overlayed `ws-gdi` assets for local cert.
