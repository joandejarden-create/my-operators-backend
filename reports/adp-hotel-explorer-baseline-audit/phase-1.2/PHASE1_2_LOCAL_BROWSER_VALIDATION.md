# ADP Phase 1.2 — Local Radar / Hotel Explorer browser validation

**Date:** 2026-11-18  
**Worktree:** `C:\Dev\dealality\my-operators-backend-adp-audit`  
**Branch:** `cursor/adp-hotel-explorer-audit-1ed2`  
**Commit:** `dfc93df33d662aede60d01cf5e290eb211eaf4a2`  
**PR:** [#364](https://github.com/joandejarden-create/my-operators-backend/pull/364) — **not merged**  
**Server:** `PORT=8091` (GDI on `8080` left running)

---

## Git status (at STOP)

```
 M .gitattributes
 M api/brand-presence.js
?? reports/adp-hotel-explorer-baseline-audit/phase-1.2/
```

Tracked test-generated reports restored to HEAD after review:
- `reports/adp-six-hotel-e2e/renaissance/DURABILITY_PROBE.json`
- `reports/dealality-adp-airtable-persistence-historical-traceability-v1/PERSISTENCE_TEST_RESULTS.json`

Meaningful local copies preserved under `phase-1.2/evidence/`.

---

## 1) Fixture LF checksum finalization

### `.gitattributes` (retained)

```
fixtures/ai-demand-positioning/permanent-full-e2e-offline/**/*.json text eol=lf
```

### EOL + exact-byte checksums

| File | `git ls-files --eol` | SHA-256 vs MANIFEST |
|------|----------------------|---------------------|
| `property-profile.json` | `i/lf w/lf` | MATCH `f75cde7078ecdc70c41a9752f017a1c1994775ecba026f99b2bfec7eef2a3d7c` |
| `observations.json` | `i/lf w/lf` | MATCH |
| `baked-package.json` | `i/lf w/lf` | MATCH |
| `prepared-package.json` | `i/lf w/lf` | MATCH |
| `MANIFEST.json` | `i/lf w/lf` | (not checksummed) |

Expected checksums were **not** changed.

### Preserved generated-report results (reviewed)

| Artifact | Meaningful result |
|----------|-------------------|
| Durability probe | `ok: true`, adapter `filesystem_PRIMARY_SOT_published`, checks: write_atomic / read_back / checksum_stable / reread_after_close / cleanup_removed |
| Persistence Layer A | `passed: 23` synthetic contract checks |
| Persistence Layer B | `failed: 5` all `BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE` (durable measured-dates) |

Evidence copies:
- `evidence/DURABILITY_PROBE.phase12-local.json`
- `evidence/DURABILITY_PROBE.phase12-local-rerun.json`
- `evidence/PERSISTENCE_TEST_RESULTS.phase12-local.json`

---

## 2) Local environment (credentials not exposed)

| Item | Value |
|------|-------|
| Env source | Copied gitignored `.env` from primary checkout into ADP worktree |
| Airtable | Key present; `AIRTABLE_BASE_ID_ALT` present (platform / Hotel Census) |
| Mapbox | Token present |
| Write/publish overrides for this run | `ADP_AIRTABLE_PUBLISH_APPLY=0`, `ADP_HISTORY_WRITES_ENABLED=0`, `ADP_HISTORY_AIRTABLE_WRITE_APPLY=0`, `ENABLE_HBX_CENSUS_WRITES=0`, `ENABLE_DATAFORSEO_VALIDATED_WRITES=0`, `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0`, `ADP_AIRTABLE_READ_LIVE=0` |
| Census map boot | `records=15692` build ok |
| Brand-presence API | HTTP 200, ~15.7k hotels |
| GDI server | Untouched on `8080` |

---

## 3) Automated tests (re-run after browser work)

| Gate | Result |
|------|--------|
| `npm run test:adp-permanent-full-e2e-certification-v1` | **PASS** Layer A; Layer B `BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE` (expected) |
| `npm run test:adp-phase1-radar-he-gates` | **PASS** (wiring, progressive enrichment, share assets, durability, published foundation) |
| Bethesda in `hotel-explorer-share.html` | **Absent** (no customer share pack) |

---

## 4) Browser validation matrix

Base URL: `http://127.0.0.1:8091/deal-capture-radar-with-ranked-list.html`

| Check | Result | Evidence |
|-------|--------|----------|
| Hotel Census pins populate Radar | **PASS** | 15,692 total hotels; clustered pins; `01-radar-census-pins.png` |
| Clicking a pin / selection opens unified Hotel Explorer | **PASS** | `HotelDetailPanel` → `HotelExplorer.open`; HE primary flag true; `02` / `06` |
| Baseline-only hotel keeps Overview / Submarket / Area / Demand / Access | **PASS** | Agualina Kite Resort `recfVWIaXlGCfyI5k`; all five tabs rendered content; `02`, `03` |
| Enriched hotel shows verified Hotel Intelligence attributes | **PASS** | Casas Del XVI `recHBTqnDeXJOc7FZ` — Hotel Equities operator, boutique, “economic owner not yet verified”; `04` |
| Identity + provenance correct (CALA census path) | **PASS** | Census `rec…` identity preserved; unverified owner not invented |
| Map/list selection + filters continue working | **PASS** | Brand filter applied (AC Hotels by Marriott → 25 hotels); reopen HE for AC Hotel SLP; `06` |
| Bethesda opens in internal preview only | **PASS** (forced by-id + `internalPreview=1`) | After alias fix: Bethesda Marriott identity; `07`. **No map pin** (see defects). |
| No new public share packs / customer visibility | **PASS** | Share HTML has no Bethesda; progressive enrichment strips customer/share |

Screenshots directory: `reports/adp-hotel-explorer-baseline-audit/phase-1.2/screenshots/`

---

## 5) Newly discovered defects

### D1 — Dual census identity schema (narrowly fixed)

**Symptom:** `GET /api/brand-presence/hotel/recLuxvwwxID7U2B8` returned `Unknown Hotel` / `Unknown City` / `Unknown Country` while Airtable GET-by-id clearly has `Property Name=Bethesda Marriott`, `City`, `Country`, `Current Brand`.

**Root cause:** `formatHotelRecord` in `api/brand-presence.js` only read legacy CALA fields (`name`, `city`, `country`, `Affiliation`). Production-census Title Case fields were ignored.

**Fix applied (narrow):** alias fallbacks in `api/brand-presence.js`:
- name ← `name` \| `Property Name` \| `Canonical Property Name`
- city ← `city` \| `City`
- country ← `country` \| `Country`
- brand ← `Affiliation` \| `Current Brand` \| `Brand Family`

**Verified:** API + Hotel Explorer now show **Bethesda Marriott**, Bethesda, United States, Marriott Hotels.

### D2 — Bethesda absent from Radar map pin universe (open / not fixed here)

**Symptom:** `recLuxvwwxID7U2B8` is not in `window.allHotels` (15,692). Airtable **GET by id works**; **list/select with `RECORD_ID()` returns 0 records**.

**Impact:** No census pin to click; handoff deep-link `?hotelExplorer=recLuxvwwxID7U2B8` cannot resolve from the live map pool. Internal preview was validated by fetching hotel-by-id then `HotelExplorer.open`.

**Scope decision:** Census list/sync inclusion is outside narrow HE wiring. Do not redesign census store in Phase 1.2.

### D3 — Stale handoff census IDs

| Handoff ID | Live observation |
|------------|------------------|
| Casas `recjDsNzu93CFfe87` | Not in map pool; live Casas Del XVI is `recHBTqnDeXJOc7FZ` |
| Hilton TS `rec35fExUxCClpOP6` | Not in CALA map pool (US) |

### D4 — Ranked-list host does not auto-boot `hotelExplorer=` query

Deep-link query updates when HE opens, but ranked-list host has no golden-demo-style autopen bootstrap. Not required once pin click / programmatic open works; document for later UX polish.

---

## 6) PR #364 merge-readiness recommendation

| Dimension | Assessment |
|-----------|------------|
| CI (latest observed) | Required checks **SUCCESS** (Radar HE Phase 1, gitleaks, GDI isolation/regression, admin tabs) |
| Mergeable | `MERGEABLE` |
| Local Layer A permanence | **PASS** (Windows LF fixtures fixed) |
| Local Radar HE gates | **PASS** |
| Local browser (CALA census path) | **PASS** |
| Bethesda internal preview identity | **PASS after D1 fix** (uncommitted) |
| Bethesda map pin | **FAIL / BLOCKED** by Airtable list exclusion (D2) — does **not** block CALA Radar HE Phase 1 code path |
| Customer share / Bethesda pack | Still absent — good |
| Layer B permanence / durable measured-dates | Still blocked by missing local durable evidence — expected |

### Recommendation

**Conditional merge-ready for Phase 1 Radar HE code**, after founder review of:
1. Uncommitted `.gitattributes` LF rule  
2. Uncommitted narrow `api/brand-presence.js` identity alias fix  
3. This Phase 1.2 validation report  

**Do not claim** Bethesda is clickable from the live Radar pin layer until D2 (census list inclusion / schema projection) is separately resolved.  
**Do not merge/deploy from this agent turn** — STOP for founder review.

---

## 7) Out of scope / unchanged

- No primary GDI checkout changes  
- No merge / deploy  
- No Airtable production writes / paid research  
- No Phase 2 implementation  
- No new share packs or intelligence store  
