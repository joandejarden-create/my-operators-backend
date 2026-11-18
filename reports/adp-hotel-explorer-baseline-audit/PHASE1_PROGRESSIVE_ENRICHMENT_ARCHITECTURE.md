# Phase 1 — Progressive enrichment architecture (Radar ↔ Hotel Explorer)

**Status:** Approved Phase 1 design (founder clarification incorporated).  
**Host:** `public/deal-capture-radar-with-ranked-list.html`  
**Branch:** `cursor/adp-hotel-explorer-audit-1ed2`  
**Rule:** Preserve existing Radar coverage; HE becomes the unified shell; no customer visibility / publish / Airtable production writes in Phase 1.

---

## 1. Existing Radar popup data-source inventory

| Tab | Primary client | APIs | Airtable base / table | Identity |
|---|---|---|---|---|
| **Hotel Overview** | `hotel-detail-panel.js` → `buildOverviewRows` | `GET /api/brand-presence/hotel/{recId}` (enrich) | Platform `AIRTABLE_BASE_ID_ALT` · **Hotel Census** | `hotel.id` = Airtable `rec…` |
| **Submarket Snapshot** | HDP + `radar-branded-penetration.js` | Local `window.allHotels`; fallback `GET /api/scout/market-coverage` | Hotel Census (Scout) | Geography from census Market/Submarket |
| **Area Hotels** | HDP distance/submarket scope | Prefers `window.allHotels`; fallback `GET /api/brand-presence?search=` | Hotel Census | Exclude subject by `rec…` |
| **Demand Drivers** | `demand-anchors-radar-layer.js` | `GET /api/radar-map-points/demand-anchors?view=map&country=` | **Demand Anchors** (`AIRTABLE_DEMAND_ANCHORS_BASE_ID` \|\| ALT) | Distance to hotel lat/lng |
| **Access & Connectivity** | `travel-infrastructure-radar-layer.js` | `GET /api/radar-map-points/travel-infrastructure?view=map&country=` | **Travel Infrastructure Data** | Distance + airport floor |

**Map pin pool:** `brand-presence-mapping.js` → `GET /api/brand-presence?view=map&limit=100000` → `window.allHotels`.

**Do not duplicate** these tables into Hotel Intelligence. HE embeds the same HDP Radar tab renderer (`HotelDetailPanel.embed`).

---

## 2. Canonical hotel identity strategy

| Rule | Detail |
|---|---|
| **Primary key** | Hotel Census Airtable record id (`rec…`) on `hotel.id` / `hotel.recordId` |
| **ADP link** | Optional `adp_*` property id via census link / published manifest `censusRecordId` — never required for Radar HE |
| **Forbidden** | Matching by hotel name alone |
| **Unresolved** | If no `rec…` id → open Explorer in degraded mode with explicit “identity unresolved”; still show map-object fields; do not invent HI |
| **Conflicts** | Census wins for identity/geo/rooms/status; verified HI attributes may enrich/override **display** of the same semantic field only when verification ≥ MEDIUM and source policy allows; show provenance + unverified empty states otherwise |

Module: `lib/hotel-intelligence/radar-hotel-explorer-progressive-enrichment-v1.js`

---

## 3. Progressive enrichment (derived, not persisted)

Presentation completeness is **computed at open time** — not a new Airtable select / classification table.

| Level | When | What the user sees |
|---|---|---|
| `RADAR_BASELINE` | Census pin exists (any Radar hotel) | HE shell + Radar tabs (Overview / Submarket / Area / Demand / Access) via HDP embed |
| `ENRICHED` | Baseline + ≥1 client-safe verified HI/ADP attribute or ownership/contact payload | Baseline plus enriched HI tabs / attribute strip where available |
| `FULL` | Enriched + complete validated HI section set for the hotel (Golden Four bar) | Full Explorer tabs populated |

- ADP certification is **not** required for internal Radar HE.
- Levels never grant publication, share tokens, or customer visibility.
- Share/customer routes remain fail-closed (`share=1` / client-safe projection).

---

## 4. Tab mapping into Hotel Explorer

| HE tab id | Source in Phase 1 |
|---|---|
| `hotel` (Overview) | **Radar HDP embed** (`overview`) — census SoT |
| `submarket` | **Radar HDP embed** |
| `area` | **Radar HDP embed** |
| `demand` | **Radar HDP embed** |
| `access` | **Radar HDP embed** |
| `ownership` / `organization` / `relationships` / `brand` / `sources` / `contacts` | HI workspace when loaded; else explicit “not available yet” / unverified empty — **never inferred** |

Packet 2.7-R1 already routes all hotels through the same Radar embed path for market tabs — Phase 1 enables that path on the ranked-list Radar host.

---

## 5. Files that change (Phase 1)

| File | Change |
|---|---|
| `public/deal-capture-radar-with-ranked-list.html` | Load `hotel-explorer.css` + `hotel-explorer.js` (and minimal deps) so `__DEALALITY_HOTEL_EXPLORER_PRIMARY` is set |
| `lib/hotel-intelligence/radar-hotel-explorer-progressive-enrichment-v1.js` | **New** — identity + completeness + precedence helpers |
| `scripts/test-radar-hotel-explorer-progressive-enrichment-v1.mjs` | **New** — baseline / enriched / full / unresolved / conflict / visibility tests |
| `scripts/test-radar-host-hotel-explorer-wiring-v1.mjs` | **New** — static assert ranked-list host loads HE |
| `lib/ai-demand-positioning/brand-portfolio/bpp-client-readiness-doctrine-v1.js` | Null-safe `profile` in `classifyBppEligibility` |
| `scripts/test-ai-demand-positioning-published-foundation.mjs` | Assert Airtable overlay contract (compaction/stubs), not raw FS evidence size |
| `scripts/rebuild-adp-existing-hotel-certification-baseline-v1.mjs` | **New** — rebuild baseline from published manifests (authoritative FS evidence) |
| `reports/ai-demand-positioning/certification-baseline/ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json` | Restored / reconstructed |
| `package.json` | npm aliases for permanence + HE Radar tests |
| `lib/dealality-pr-check-matrix.js` | PR suggest rules |
| `.github/workflows/adp-hotel-explorer-radar-phase1.yml` | CI for free Phase 1 gates |
| `reports/adp-hotel-explorer-baseline-audit/*` | Architecture + handoff + Phase 2 roadmap updates |

**Unchanged by design:** HE share packs, GDI publication, Bethesda customer visibility, production Airtable, paid research.

---

## 6. Coverage preservation confirmation

| Guarantee | How |
|---|---|
| No Radar hotel disappears without HE record | HE opens from census pin; Wave 0 = census itself |
| Radar tabs keep working | Same `HotelDetailPanel.embed` + same APIs |
| Click / map / list sync preserved | Still `HotelDetailPanel.open` → now delegates to `HotelExplorer.open` when primary |
| Filters preserved | No filter code changes |
| Client-safe on share routes | Enrichment module marks `customerSafeEligible` false unless share mode + verified client-safe attrs; no new packs |
| Bethesda internal preview | Census id eligible; **not** added to `SHARE_PACKS` |

---

## 7. Field precedence (deterministic)

1. **Identity:** Census `rec…` only.  
2. **Geo / rooms / status / brand / parent (baseline):** Census Hotel Census fields.  
3. **Verified attribute overlay:** HI / Hotel ADP Attributes with `Active?` + confidence HIGH|MEDIUM + `canDisplayToCustomer` — may replace display value for that attribute key with provenance note.  
4. **Unverified / missing:** Show unavailable / unverified empty state — never invent.  
5. **Conflicts:** Prefer census for SoT identity fields; prefer verified HI for attribute strip; surface both in Sources when both present (Phase 2 UI polish).
