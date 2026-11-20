# HE Demand Drivers & Access / Connectivity — Airtable Recovery (HE-QL-037)

**Date:** 2026-11-19  
**Hotel identity (Bethesda Marriott):** `recLuxvwwxID7U2B8`  
**PR:** #463  
**Status:** Read integration restored. US Demand Anchors / Travel Infrastructure currently empty — tabs stay honest-empty until research/import.

---

## 1. Inventory of recovered historical code

| Area | Path | Role |
|------|------|------|
| Demand Anchors fields | `lib/demand-anchors/airtable-demand-anchors-fields.js` | Table/field map, point types, demand segments |
| Demand Anchors I/O | `lib/demand-anchors/airtable-demand-anchors-io.js` | Fetch / normalize / Radar points |
| Demand Anchors base | `lib/demand-anchors/demand-anchors-base.js` | Platform base routing (`AIRTABLE_BASE_ID_ALT`) |
| Demand Anchors import | `lib/demand-anchors/import-commit.js`, `scripts/import-demand-anchors-*.mjs` | Preview/commit imports (apply gated) |
| Demand Anchors Radar API | `api/demand-anchors.js` | Country/region Radar GET |
| Demand Anchors client | `public/js/demand-anchors-radar-layer.js` | `DemandAnchorsRadar` |
| Travel Infrastructure fields | `lib/travel-infrastructure/airtable-travel-infrastructure-fields.js` | Table/field map |
| Travel Infrastructure I/O | `lib/travel-infrastructure/airtable-travel-infrastructure-io.js` | Fetch + Estimated Drive Time |
| Travel Infrastructure base | `lib/travel-infrastructure/travel-infrastructure-base.js` | Platform base routing |
| TI import / audit | `scripts/import-travel-infrastructure-*.mjs`, `scripts/audit-market-travel-infrastructure.mjs` | Market TI research + import |
| TI Radar API | `api/travel-infrastructure.js` | Country Radar GET |
| TI client | `public/js/travel-infrastructure-radar-layer.js` | `TravelInfrastructureRadar` |
| HE read model (new) | `lib/hotel-intelligence/he-demand-access-read-v1.js` | HPC + haversine + empty reasons |
| HE API (new) | `api/hotel-explorer-demand-access.js` | Hotel-scoped GET routes |
| HE UI | `public/js/hotel-detail-panel.js` | Demand Drivers + Access tabs |

**Not rebuilt:** research agents, import schemas, or Radar map layers. HE now reuses them via hotel-scoped GET wrappers.

---

## 2. Existing Airtable base / table / field mapping

### Hotel identity (canonical)

| Item | Value |
|------|--------|
| Base | Deal Capture Platform (`AIRTABLE_BASE_ID_ALT` / graph canonical `appCCUsuGsE1ifoLk`) |
| Table | Hotel Property Census (`tbl9aY5ijiuIzzWam`) |
| Relationship | Demand/Access distance origin = HPC lat/lng for the selected hotel |

### Demand Drivers → Demand Anchors

| Item | Value |
|------|--------|
| Base | Same Platform base (`AIRTABLE_DEMAND_ANCHORS_BASE_ID` \|\| `AIRTABLE_BASE_ID_ALT`) |
| Table name | `Demand Anchors` |
| Table ID | Resolved at runtime via Airtable meta (name-stable; no new table) |
| Key fields | Demand Anchor Name, Point Type / Subtype, City, Country, Region, Submarket, Lat/Lng, Distance From Deal, Estimated Drive Time, Demand Relevance / Segment / Pattern, Hotel Demand Rationale, Source / Source URL, Data Confidence, Last Verified, Include On Radar Map |
| HPC relationship | **Reusable entities** — not duplicated per hotel. HE computes straight-line distance from HPC coords. Optional deal-linked fields exist for Radar deal context. |
| Population (live probe) | **United States: 0 records.** Mexico and other CALA markets: populated (e.g. MX Demand Anchors ≈ 347 historically probed). |
| Research / refresh | `import-demand-anchors-preview/commit`, geocode/verify scripts, `ensure-demand-anchors-schema` — **apply requires founder auth** |
| API / read model | Legacy: `GET` demand-anchors Radar. **HE:** `GET /api/hotel-explorer/hotels/:hotelId/demand-drivers` |

### Access & Connectivity → Travel Infrastructure Data

| Item | Value |
|------|--------|
| Base | Same Platform base (`AIRTABLE_TRAVEL_INFRASTRUCTURE_BASE_ID` \|\| `AIRTABLE_BASE_ID_ALT`) |
| Table name | `Travel Infrastructure Data` (legacy casing `Travel Infrastructure data` accepted) |
| Key fields | Name, Type / Point Type / Subtype, Lat/Lng, City, Country, Region, IATA/ICAO, Scale Tier, Infrastructure Role, Airport Type, Estimated Drive Time, Demand Relevance / Pattern, Hotel Demand Rationale, Source, Last Verified, Include On Radar Map |
| HPC relationship | **Reusable hubs** (one airport → many hotels). HE distances from HPC coords; min-2 closest airports policy when available. |
| Population (live probe) | **United States: 0 records.** Mexico TI ≈ 145 historically probed. |
| Research / refresh | `audit-market-travel-infrastructure`, `import-travel-infrastructure-preview/commit`, schema ensure / radar field backfill — **apply gated** |
| API / read model | Legacy: travel-infrastructure Radar. **HE:** `GET /api/hotel-explorer/hotels/:hotelId/access-connectivity` |

### Schema changes

**None proposed.** Existing tables cover corporate/institutional/tourism demand types and airports/rail/roads. Do not create hotel-specific duplicate entity tables.

---

## 3. Pipelines that can be reused

| Pipeline | Trigger | Providers | Paid? | Writes | Batch? | Runnable now? |
|----------|---------|-----------|-------|--------|--------|---------------|
| Demand Anchors import preview/commit | CLI | Curated fixtures / Places verification | Places verify may cost | Airtable upsert on `--apply` | Per-market | Yes dry-run; apply needs auth |
| Demand Anchors coordinate verify | CLI | Google / Maps audit scripts | Possible | Optional apply | Per-market | Yes dry-run |
| Travel Infrastructure market audit | `npm run audit:<country>-ti` / `audit-market-travel-infrastructure.mjs` | Config-driven keyword / research | Depends on config | JSON under `data/` | Per market | Yes read/audit |
| TI import preview/commit | CLI | Audit outputs | No on preview | Airtable on `--apply` | Per-market | Yes dry-run |
| HE tab open | Browser GET | Airtable read only | **No** | **No** | N/A | Yes |

**Target workflow:** HPC hotel → reuse existing DA/TI entities → identify gaps → research once → verify → persist canonically → HE auto-populates.

---

## 4–5. Read integration (shipped)

- Demand Drivers: HPC resolve + Demand Anchors fetch + straight-line filter + dedupe + empty messaging.
- Access & Connectivity: same for Travel Infrastructure; mi/km toggle on Access tab; no invented drive times.
- Founder Preview hydrates thin hotel payloads via `/api/brand-presence/hotel/:id` then HE APIs.

---

## 6. Research workflow restoration plan (no paid/apply in this PR)

1. For US / Bethesda: run TI + Demand Anchors **preview** against Montgomery County / DC catchment (dry-run JSON).
2. Founder review CREATE/UPDATE packet.
3. Authorized `--apply` into Demand Anchors + Travel Infrastructure Data (entity-level, not per-hotel copies).
4. Re-open Bethesda HE — tabs populate without frontend changes.
5. Coordinate GDI demand-driver research reuse with GDI stream owner if hotel-specific demand scoring is needed later (separate from geographic Demand Anchors).

---

## 7. Proposed Airtable writes / schema

- **Writes:** none in this PR.
- **Schema:** none required for read path.
- Future optional: ensure US region/country select options exist before import (use `ensure-*-schema` dry-run first).

---

## 8. Bethesda Founder Preview validation

- Hotel: Bethesda Marriott `recLuxvwwxID7U2B8`.
- Expected until US import: **empty Demand Drivers** and **empty Access** with source meta pointing at Demand Anchors / Travel Infrastructure Data + HPC id.
- When MX (or other populated) hotels are opened with coords, tabs show live entities (proves auto-population path).

---

## 9. Auto-population confirmation

UI calls hotel-scoped GET on every open. After canonical Airtable rows exist for the hotel’s country/region and fall within radius, panels render without a new frontend deploy beyond this integration.

---

## 10. PR / blockers

- PR **#463** OPEN — Integration Controller merges.
- Blocker for **populated** Bethesda tabs: US Demand Anchors + TI research/import not yet authorized.
- No paid research / production writes / deploy in this task.
