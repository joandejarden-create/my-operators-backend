# HPC Coordinate Completeness Gate — Implementation Plan v1

**Contract:** `HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1`  
**Status:** Design for founder review — **not** implemented on HE-002 PR #398  
**Airtable writes / paid geocoding / deploy:** blocked until founder authorization

## Goal

Every Hotel Property Census (HPC) record with **Radar Status = Open or Pipeline** must have valid, verified Latitude and Longitude before it is treated as map-complete. Hidden/blank may remain without coordinates.

## Where validation belongs

| Layer | Responsibility |
| --- | --- |
| 1. Shared library | `lib/hotel-census/hpc-coordinate-completeness-gate-v1.js` — pure validators (missing/0,0/range/country bounds/city-center traps), issue codes, batch audit helpers |
| 2. Census Autopilot / activation | Block or hold **Radar Status → Open/Pipeline** transitions when hard coordinate failures remain; reuse existing `coordinate_completion` queue + Mapbox permanent path (`docs/data-intelligence/production-census-mapbox-coordinate-completion.md`) |
| 3. Admin diagnostics | Surface exceptions in HPC / Radar internal-preview diagnostics (`diagnostics.missingCoordinates` already exists; extend with hard/soft issue codes) |
| 4. Radar runtime | Continue dynamic HPC formula merge — **no hotel ID allowlists**. Map markers require `hotelHasMapCoordinates`; search/Explorer may retain missing-coord rows until corrected |
| 5. CI / regression | `scripts/test-hpc-coordinate-completeness-gate-v1.mjs` + wire into `test:adp-phase1-radar-he-gates` or a dedicated census gate |

## Phase sequencing (after founder approval)

1. **Land gate library + tests** on a dedicated branch (not #398) from latest `origin/main`.
2. **Ship read-only audit CLI** (already drafted: `scripts/audit-hpc-open-pipeline-coordinate-completeness-v1.mjs`) under `npm run audit:hpc-open-pipeline-coords`.
3. **Authorize coordinate resolution batch** for hard exceptions only (today: Radisson Toronto + UMusic Madrid) via permanent geocode flags — dry-run first, then apply.
4. **Wire activation precheck** so new Open/Pipeline promotions fail closed without verified coords.
5. **Extend Radar diagnostics** to list hard exceptions in admin/internal notice (already partially present for missing coords).

## Radar integration rules

- Eligibility remains **dynamic** `Radar Status ∈ {Open, Pipeline}`.
- Coordinates come **only** from HPC Latitude/Longitude fields after verification.
- Do **not** hardcode hotel IDs, special-case geocodes, or separate Radar coordinate allowlists.
- Do **not** silently invent or default to city/country centroids.
- After authorized HPC writes, Radar picks up coords via existing cache TTL / refresh — no Radar-side patch.

## Regression coverage (planned)

- Missing lat or lng → hard fail  
- `0,0` → hard fail  
- Out-of-range → hard fail  
- Country-bounds mismatch → hard fail  
- Exact city-center trap → soft flag (review; do not auto-write)  
- Open/Pipeline with valid coords → pass  
- Hidden/blank without coords → out of gate scope (pass / not applicable)  
- Status transition Open→Hidden does not require coords  
- No hotel-id special cases in Radar merge path  

## Existing reusable machinery

- Autopilot queue `coordinate_completion` + Mapbox permanent geocoding  
- Fields: Latitude, Longitude, Coordinate Source Type, Coordinate Confidence, Geocode Provider, Geocode Method, Geocode Reviewed Date  
- Address geocode resolver: `lib/research-engine-v2/production-census-address-geocode-resolver.js`  
- Radar: `hotelHasMapCoordinates`, `diagnostics.missingCoordinates`, search-eligible vs map-eligible split  

## Separation from HE-002 / PR #398

- This gate is a **permanent HPC data-quality** workstream.
- PR #398 may continue authenticated `/app` acceptance without absorbing this gate unless a coordinate-validation **defect** in Radar itself needs a surgical fix.
- Audit artifacts under `reports/hotel-explorer/hpc-*-coordinate-completeness-*` stay review-only until a dedicated PR is authorized.

## Founder decisions needed

1. Approve permanent gate contract and placement (library + autopilot activation + diagnostics).  
2. Authorize official geocode apply for:
   - `recH9h3McgMNFQEla` Radisson Blu Toronto Downtown (staged consensus coords exist; prefer permanent address geocode)
   - `recHXCkkKD5Xwue3p` UMusic Hotel Madrid (address-ready; no invented coords)
3. Confirm whether soft city-center flags should block Open/Pipeline activation or only warn.
4. Authorize a dedicated feature branch / PR (not #398) to implement the gate after coordinate applies (or in parallel as dry-run-only).
