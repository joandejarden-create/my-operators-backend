# HPC Coordinate Completeness — Founder Review Pack v1

**PR branch:** `feat/hpc-coordinate-completeness-gate-v1`  
**Contract:** `HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1`  
**Airtable writes:** **none** (STOP before apply)  
**Statuses:** preserved (both remain Open)

## Exact proposed Airtable changes

### 1) Radisson Blu Toronto Downtown — `recH9h3McgMNFQEla`

| Field | Before | After |
| --- | --- | --- |
| Latitude | `null` | `43.638654` |
| Longitude | `null` | `-79.385713` |
| Coordinate Source Type | `null` | `official_address_geocode` |
| Coordinate Confidence | `null` | `Medium` |
| Geocode Provider | `null` | `Mapbox` |
| Geocode Method | `null` | `mapbox_permanent_geocode` |
| Geocode Reviewed Date | `null` | `2026-11-18` |

Evidence: Mapbox permanent geocode of street address matched `249 Queens Quay West, Toronto, Ontario M5J 2N5, Canada` (relevance 1.0). Rejected Toronto city-center `43.6532, -79.3832` (≈0.015° away). Soft: divergent candidate set reviewed → Medium confidence.

### 2) UMusic Hotel Madrid — `recHXCkkKD5Xwue3p`

| Field | Before | After |
| --- | --- | --- |
| Latitude | `null` | `40.41544` |
| Longitude | `null` | `-3.704038` |
| Coordinate Source Type | `null` | `official_address_geocode` |
| Coordinate Confidence | `null` | `Medium` |
| Geocode Provider | `null` | `Mapbox` |
| Geocode Method | `null` | `mapbox_permanent_geocode` |
| Geocode Reviewed Date | `null` | `2026-11-18` |

Evidence: Mapbox permanent geocode matched `Calle De La Paz 11, 28012 Madrid, Madrid, Spain` (relevance ≈0.88). Soft: downtown proximity to Madrid center flagged for review (property is on Calle de la Paz near Sol — **not** a confirmed city-center fallback).

## Permanent validation contract

- Library: `lib/hotel-census/hpc-coordinate-completeness-gate-v1.js`
- Doc: `docs/data-intelligence/hpc-coordinate-completeness-gate-v1.md`
- Activation precheck: `evaluateOpenPipelineActivation` (hard-block new Open/Pipeline without valid coords; never auto-demotes existing status)
- Admin diagnostics: `diagnostics.coordinateCompleteness` on Radar HPC internal-preview payload
- Tests: `npm run test:hpc-coordinate-completeness-gate-v1`

## Risks / notes

1. Shared Mapbox helper initially returned `multiple_divergent_candidates`; staging recovered the top address feature and re-validated through the gate (Medium confidence + soft flags).
2. UMusic soft near-center flag is expected for a central Madrid street address — do not treat as city-centroid rejection.
3. Geocode apply to Airtable still requires explicit founder authorization (separate from this PR merge if desired).
4. HE-002 PR #398 remains separate; this branch does not include that work.

## STOP

No Airtable write, no merge, no deploy, no production activation until founder approves the two patches and/or IC merge of this PR.
