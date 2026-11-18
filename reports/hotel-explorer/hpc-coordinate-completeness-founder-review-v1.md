# HPC Coordinate Completeness — Founder Review Pack v1 (corrected)

**PR:** [#403](https://github.com/joandejarden-create/my-operators-backend/pull/403)  
**Branch:** `feat/hpc-coordinate-completeness-gate-v1`  
**Contract:** `HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1`  
**Airtable writes:** **none** (STOP before apply)

## 1) Geocoding budget

| Item | Value |
| --- | --- |
| Founder-authorized paid calls | **2** (one per hotel) |
| Historical calls before budget fix | **4** (1 initial + 1 re-query recovery × 2 hotels) |
| Additional paid calls this correction | **0** (`--offline-from-prior`) |
| Enforcement now | `createGeocodeRequestBudget` — any paid path must `tryConsume`; divergent recovery uses **same response** (`paidCallsAdded: 0`) |

Exceeding the budget returns `GEOCODE_BUDGET_EXCEEDED` and does not call the provider. Additional paid calls require new founder authorization.

## 2) Open/Pipeline activation enforcement

See `reports/hotel-explorer/hpc-coordinate-completeness-write-path-enforcement-v1.md`.

**Covered:** Autopilot forbid of `Radar Status`; shared `guardHpcRadarStatusPatch`; diagnostics for existing exceptions (status preserved).

**Outstanding gap:** Manual Airtable UI (and any raw API script that bypasses the guard) can still set Open/Pipeline without coordinates. The validator library alone is **not** permanent write enforcement.

## 3) Final coordinate evidence (no new paid geocode)

### Radisson Blu Toronto Downtown — `recH9h3McgMNFQEla`

| Field | Before | After (proposed) |
| --- | --- | --- |
| Latitude | null | **43.638654** |
| Longitude | null | **-79.385713** |
| Coordinate Source Type | null | `official_address_geocode` |
| Coordinate Confidence | null | `Medium` |
| Geocode Provider | null | `Mapbox` |
| Geocode Method | null | `mapbox_permanent_geocode` |
| Geocode Reviewed Date | null | `2026-11-18` |

- Matched place: `249 Queens Quay West, Toronto, Ontario M5J 2N5, Canada` (relevance 1.0)
- Street-level harbourfront pin — **not** Toronto city center (`43.6532, -79.3832`; ≈0.015° away)
- Soft: divergent candidates in the same Mapbox response → Medium confidence
- Provenance: **Mapbox permanent geocode of official street address** — official website did **not** supply coordinates
- Env: `MAPBOX_PERMANENT_GEOCODING=1` was enabled for the staging lookup

### UMusic Hotel Madrid — `recHXCkkKD5Xwue3p`

| Field | Before | After (proposed) |
| --- | --- | --- |
| Latitude | null | **40.415440** |
| Longitude | null | **-3.704038** |
| Coordinate Source Type | null | `official_address_geocode` |
| Coordinate Confidence | null | `Medium` |
| Geocode Provider | null | `Mapbox` |
| Geocode Method | null | `mapbox_permanent_geocode` |
| Geocode Reviewed Date | null | `2026-11-18` |

- Matched place: `Calle De La Paz 11, 28012 Madrid, Madrid, Spain` (relevance ≈0.88)
- Street-level address on Calle de la Paz (downtown near Sol) — soft near-center flag only; **not** a confirmed city-center fallback
- Provenance: **Mapbox permanent geocode** — not hotel website coordinates
- Status preserved: **Open**

## 4) STOP

No PR merge. No Airtable apply. No additional paid geocoding. After final approval, Integration Controller merges #403 and runs post-merge HE sync to `ws/hotel-explorer`.
