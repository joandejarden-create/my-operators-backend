# HPC Coordinate Completeness Gate v1

**Contract:** `HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1`  
**Library:** `lib/hotel-census/hpc-coordinate-completeness-gate-v1.js`  
**Tests:** `npm run test:hpc-coordinate-completeness-gate-v1`

## Rule

Hotel Property Census records with **Radar Status = Open or Pipeline** must have valid, verified property-level Latitude and Longitude for map completeness.

| Status | Coordinates required |
| --- | --- |
| Open / Pipeline | Yes (activation hard-blocked when missing/invalid) |
| Hidden / blank | No |

Existing Open/Pipeline exceptions are **flagged** in diagnostics. Statuses are **not** auto-demoted during remediation.

## Hard-block (new activation)

- Missing Latitude or Longitude  
- `0,0`  
- Out of geographic range  
- Country-bounds mismatch  
- Address-unrelated coordinates  
- Confirmed city-center fallback (locality/admin geocode)

## Soft / manual review

- Low confidence  
- Incomplete address match  
- Potential (unconfirmed) city-center proximity — **do not** hard-block a verified hotel pin merely for nearness to a city center  
- Missing provenance on new activation (when `requireProvenance` / `isNewActivation`)

## Radar integration

- Dynamic HPC Open/Pipeline formula only — no hotel-ID geocode allowlists  
- Admin diagnostics expose `diagnostics.coordinateCompleteness`  
- Map eligibility still requires coordinates; search/Explorer behavior is owned by Radar product PRs separately  

## Paid geocode budget

Use `createGeocodeRequestBudget({ maxPaidRequests })`. Every paid provider call (initial, retry, fallback, recovery) must `tryConsume` first. Divergent-candidate recovery must reuse the **same** provider response (`recoverTopCandidateFromDivergentGeocode`) — **0** extra paid calls. Exceeding the budget requires new founder authorization.

## Write-path enforcement

- Census Autopilot: `Radar Status` is **forbidden**.
- Future Node writers: call `guardHpcRadarStatusPatch` before applying Open/Pipeline.
- Manual Airtable UI remains an enforcement gap (documented in the write-path enforcement report).

## Commands

```bash
npm run test:hpc-coordinate-completeness-gate-v1
npm run audit:hpc-open-pipeline-coords
npm run stage:hpc-coordinate-completeness-geocode-v1 -- \
  --ids recH9h3McgMNFQEla,recHXCkkKD5Xwue3p \
  --founder-auth FOUNDER_AUTH_HPC_COORD_GEOCODE_LOOKUP_V1 \
  --max-paid-requests 2
```

Offline refresh of prior staging evidence (no paid calls):

```bash
npm run stage:hpc-coordinate-completeness-geocode-v1 -- \
  --ids recH9h3McgMNFQEla,recHXCkkKD5Xwue3p \
  --founder-auth FOUNDER_AUTH_HPC_COORD_GEOCODE_LOOKUP_V1 \
  --offline-from-prior --max-paid-requests 2
```

Geocode staging never writes Airtable. Apply requires a separate founder authorization.
