# Bethesda Marriott — Five-Field Provenance Audit & Remediation

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**HPC base / table:** `appCCUsuGsE1ifoLk` / `tbl9aY5ijiuIzzWam`  
**Runtime:** `http://127.0.0.1:8091`  
**Date:** 2026-11-19  
**Paid research:** still **BLOCKED**

## Evidence table (inspected values)

| Field | Displayed (after) | Actual runtime source | Canonical Airtable / established source | Evidence | Confidence | Decision |
|---|---|---|---|---|---|---|
| `parentCompany` | Marriott International (brand parent) | HPC formatter → `Brand Family` | HPC `Brand Family` = Marriott International (`Parent Company` empty) | Live HPC record; UI label clarifies brand parent | HIGH | **A VERIFIED_HPC** (semantic: brand parent, **not** economic owner) |
| `chainScale` | Upper Upscale | HPC formatter alias | HPC `Hotel Class / Segment` = Upper Upscale | Live HPC; taxonomy = Dealality class band (not STR import) | HIGH | **A VERIFIED_HPC** |
| `totalMeetingSpace` | 18,719 sq ft · 27 rooms · Grand Ballroom 4,592 sq ft | Overview enrichment → ADP/HI meeting profile | Not on HPC numeric fields; HPC `Meeting Space Flag`=true; ADP profile + Marriott events URL | `fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json`; source `https://www.marriott.com/.../events/` | HIGH | **C VERIFIED_DERIVED** (sq ft total meeting space) |
| `operationType` | Management Agreement | HPC formatter alias | HPC `Management Model` = Management Agreement; corroboration `Operator Type`=Brand-Managed | Live HPC | HIGH | **A VERIFIED_HPC** |
| `locationType` | Suburban | HPC formatter alias | HPC `Asset Context` = Suburban | Live HPC Asset Context taxonomy | HIGH | **A VERIFIED_HPC** |

### Field-specific notes

- **parentCompany:** Means **brand parent**, not property owner / PropCo / UBO. Economic ownership remains unresolved on Ownership tab.
- **chainScale:** From HPC `Hotel Class / Segment`, not an unsupported brand-level constant.
- **totalMeetingSpace:** Unit = **sq ft**; metric = **total** meeting space (plus room count and largest room from same official events source). Not persisted to HPC yet — optional steward backfill later (no paid research required for display).
- **operationType:** Management Agreement from HPC Management Model — not inferred from “Marriott” alone.
- **locationType:** HPC Asset Context (Suburban), not a heuristic geocode guess.

## Before → after

| Field | Before disposition | After |
|---|---|---|
| parentCompany | Blank in UI / ADP overlay suppressed; Gate 1 treated as gap or unclear | Brand Family mapped with explicit brand-parent provenance |
| chainScale | Shown but Gate 1 `UI_POPULATED_NOT_FROM_VERIFIED_HPC_FIELD` (auditor missed alias) | `LIVE_HPC_TO_UI` via Hotel Class / Segment |
| totalMeetingSpace | Shown; flagged as unverified HPC | `LIVE_VERIFIED_DERIVED` with evidence URL |
| operationType | Shown; flagged | `LIVE_HPC_TO_UI` via Management Model |
| locationType | Shown; flagged | `LIVE_HPC_TO_UI` via Asset Context |

## Code changes

- `lib/hotel-intelligence/radar-internal-preview-hotels-v1.js` — Brand Family → brand parent; keep Asset Context / Management Model / Hotel Class aliases
- `lib/hotel-intelligence/he-overview-field-provenance-v1.js` — **new** `fieldProvenance` attachment
- `api/brand-presence.js` — attach provenance after enrichment; HPC Brand Family wins over ADP parent overlay
- `public/js/hotel-detail-panel.js` — “Parent company (brand)” label when semantic is brand parent
- Gate 1 metrics regenerator — correct HPC alias lists; `LIVE_VERIFIED_DERIVED` class
- `scripts/test-he-beth-five-field-provenance-v1.mjs` + package script

## Metrics impact (Gate 1 regenerator)

| Metric | Before | After |
|---|---:|---:|
| Bethesda existing Airtable coverage | 57.1% | **63.5%** |
| Live Airtable→UI | 38.1% | **46.0%** |
| UI without verified HPC field | 6.3% | **0%** |
| Genuine research gaps | 25.4% | **23.8%** |
| Integration remaining | 17 | **13** |

## Tests

- `npm run test:he-beth-five-field-provenance-v1` — PASS (unit + live :8091)
- `npm run test:hotel-explorer-overview-enrichment-v1` — PASS
- `npm run test:brand-presence-census-field-aliases-v1` — PASS
- `npm run test:he-property-census-identity-v1` — PASS
- `npm run test:radar-internal-preview-hotels-v1` — PASS
- `npm run gate1:he-tabs-1-5-kgpv-gold-beth-metrics` — PASS

## Persistence / isolation

- No Airtable writes
- No Contact Intelligence mutation on GET
- HE :8091 isolation preserved
- Fixtures not promoted into HPC

## Remaining (non-blocking)

- Optional steward: persist meeting-space sq ft onto HPC `Total Meeting Space Sq Ft` from the same official events source (no paid research)
- Economic owner / PropCo still genuine research gaps (blocked until founder approval)

## Browser evidence

Golden-demo HE Overview on `:8091` shows Parent company (brand) = Marriott International (brand parent); Chain scale / Operation type / Location type / Meeting space as above.
