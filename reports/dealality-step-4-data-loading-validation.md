# Dealality Step 4 — Data Loading, Caching, Pagination & Empty States

**Audit date:** 2026-05-20  
**Scope:** Demo-facing list/explorer pages in the app shell (`public/app.js` routes).  
**Constraints:** Read-only audit — no file, API, or Airtable schema changes.

---

## A. Executive summary

Most demo-critical surfaces load **live data through Express APIs backed by Airtable**, not direct browser-to-Airtable calls. The main demo risks are **not missing pagination** but **perceived slowness and copy that undersells the product**:

1. **My Deals** — Heavy initial `GET /api/my-deals?view=initial` plus lazy secondary fetches; prominent **“Estimated time: 10–30 seconds”** on multiple tabs. Wave loaders are good; the time estimate trains users to expect a slow product.
2. **Dealality Radar** — Single request `GET /api/brand-presence?limit=100000` loads the full census into the browser; server has **5-minute cache**, client has none. Visible **“Loading hotel data from Airtable…”** and **“3–5 seconds”** status copy. Failure leaves an **empty map** (looks broken in a demo if API/env fails).
3. **LOI Market Hub** — **100% in-page mock** (`DEAL_POOL`) with a permanent **Mock Data Display** banner. Fine as a “sample benchmark” story; misleading if presented as live LOI data.
4. **Operator Explorer** — **Hybrid:** live operator list from API; **detail panel is static mock** (`operator-explorer-gold-mock.html` + `operator-explorer-gold-mock-data.js`). Click-through in a demo exposes mock content behind a live directory.
5. **Brand Explorer, Market Alerts, Partner Directory, Deal Compare** — Generally credible: API-backed, client-side filter/sort on loaded sets, wave/spinner loaders, reasonable empty states. **Brand Explorer** shows **Mock Data Display** only for known demo brand names/IDs. **Partner Directory** labels some stats as sample figures.

**Skeleton loaders:** Not used on these eight modules (used elsewhere, e.g. clause/financial libraries). **Wave + progress bar + text** is the standard pattern.

**Pagination:** Only **LOI Market Hub** has real UI pagination (client-side, 100 rows/page). Other pages load full result sets (or cap at API `limit`, e.g. Market Alerts 100) and filter in the browser.

**Recommended demo posture:** Warm caches before external demo (reload My Deals, Brand Explorer, Radar once); avoid clicking Operator detail unless explaining mock; position LOI Hub as illustrative; soften or remove long **estimated-time** strings (P1 copy, not P0 unless API is down).

---

## B. Page-by-page table

| # | Page / module | Main file(s) | Data source | API / function | Pagination | Caching | Filter / search | Skeleton | Spinner / loading message | Long-load copy | Empty state | Broken / empty demo risk | Recommended action | Priority |
|---|---------------|--------------|-------------|----------------|------------|---------|-----------------|----------|---------------------------|----------------|-------------|--------------------------|-------------------|----------|
| 1 | **My Deals** | `public/my-deals.html` (shell: `/my-deals`) | **Hybrid — Backend API** (Airtable via server) | Primary: `GET /api/my-deals?view=initial` (`DealalityMemberstackAuth.fetchMyDealsList`). Lazy: `loadAllTargetLists` → `/api/target-list/*`; `fetchContactedPairs`; `/api/deal-compare/proposals`; per-deal `/api/my-deals/:id`, outreach, etc. | **No** (all deals in memory; table shows full filtered set) | **Session in-memory:** `targetListCache`, `brandMetaCache`; no HTTP cache on main list | **Client-side** search/filters on `allDeals` | No | Yes — wave loader `#loadingState`; repeated on Matched Brands / Deal Compare tabs | **Yes:** “Estimated time: **10–30 seconds**” (Deals, Matched Brands, Deal Compare loaders) | Yes — `#emptyState`: “No deals match the current filters…”; tab-specific empties | **Medium:** Long load + scary time copy; **High** if API/auth fails (empty table after loader). Matched/Compare tab counts stay **0 until tab opened** (lazy `ensureNonCriticalDataLoaded`) — can look “broken” if presenter opens those tabs cold | Replace loading time copy; optional warm `view=initial` before demo; keep lazy tabs but explain counts; fix nav links (fit-list) separately (Step 1/2) | **P1** copy/UX; **P0** only if env/API down |
| 2 | **Brand Explorer** | `public/brand-explorer-combined.html`, `public/js/brand-explorer-atelier-from-api.js`, `public/js/brand-explorer-gold-detail.js` | **Hybrid — Backend API** | List: `GET /api/brand-library/brands` (`?refresh=1` if `?fresh=1`). Detail: `GET /api/brand-library/brand?brandId=`. Favorites: `BrandExplorerFavorites` (API-backed when configured) | **No** (full brand list client-filtered) | **Server:** in-memory list cache ~120s (`BRAND_LIST_CACHE_TTL_MS` in `api/brand-library.js`). **Client:** `fetch(..., { cache: 'no-store' })`; per-brand detail held in memory after load | **Client-side** search + dropdown filters (`filterBrands`, 300ms debounce) | No | Yes — list loader “Estimated time: **1–2 seconds**”; detail “**2–6 seconds**” / “Waiting for live brand data…” | **Yes:** 1–2s, 2–6s estimates | Yes — “No brands found” + error state on fetch failure | **Low** with warm cache; **Medium** on cold `refresh=1` or slow Airtable. **Demo brands** (e.g. Atelier North, Voco) show **Mock Data Display** banner | Keep API; soften time copy; demo with non-demo brands or acknowledge banner; optional `?fresh=1` only when needed | **P1** copy; **P2** server pagination if brand count grows |
| 3 | **Operator Explorer** | `public/operator-explorer.html`, `public/js/operator-explorer.js`, `public/operator-explorer-gold-mock.html`, `public/js/operator-explorer-gold-mock-data.js` | **Hybrid** — list **API**, detail **static mock** | List: `GET /api/third-party-operators?activeOnly=1`. Detail popup: iframe `operator-explorer-gold-mock.html?id=` (no API) | **No** | **None** on list (full fetch each visit) | **Client-side** filters/search | No | Yes — `#loadingState`; popup “Loading operator…” **2–6 seconds** | **Yes:** 2–6s on gold popup | Yes — “No operators found” / error HTML | **High on click-through:** directory looks live, detail is mock. Empty list if API empty | Label detail as preview/mock in UI; or wire detail to real operator API later; soften 2–6s copy | **P1** demo credibility; **P3** real detail API |
| 4 | **Dealality Radar** | `public/deal-capture-radar-with-ranked-list.html`, `public/brand-presence-mapping.js`, `public/radar-page.js` | **Backend API** (no live mock path used) | Map: `GET /api/brand-presence?limit=100000`. Ranked list: `GET /api/operators-by-brand-region?…`. Travel layer: `GET /api/travel-infrastructure` | **No** (all hotels loaded; map filters client-side). Ranked table is client-sorted filtered subset | **Server:** 5 min in-memory cache (`api/brand-presence.js`). **Client:** none | **Client-side** map filters + ranked list filters | No | Yes — `#loadingMessage` spinner; `showSystemStatus`; ranked `#rlLoading` | **Yes:** “Loading hotel data from **Airtable**…”; status **“3–5 seconds”**, “1–2 seconds”, “1 second”. `generateMockHotelData()` exists but **is not called** | Partial — API failure: `showNoResultsMessage('Unable to load live hotel census data…')` on empty map | **High** first paint (large JSON + Leaflet markers). **P0** if `brand-presence` 500/misconfigured — empty map. Operator role may load **different** radar HTML per `app.js` | Warm server cache; replace Airtable-facing copy; monitor `/api/brand-presence` in demo env; **P2** bbox/limit pagination later | **P0** if API fails; **P1** copy; **P2** payload size |
| 5 | **Market Alerts** | `public/market-alerts.html`, `public/market-alerts.js` | **Backend API** | `GET /api/market-alerts?timeWindow&limit&category&regionGroup&search`; rail: `GET /api/market-alerts/rail`; save/read via POST endpoints | **Cap only** (`DEFAULT_LIMIT = 100`), no “page 2” UI | **None** (refetch on filter change). **localStorage** for saved alert IDs only | **Server-side** query params + **client** saved-id filter for “Saved” view | No | Yes — `#feedLoading` “Loading alerts…” | No long second estimates | Yes — `#feedEmpty` with helpful messages; network/Airtable errors surfaced | **Low** if Airtable populated. Demo banner in HTML is **`display: none`** unless `.visible` (JS does not toggle it today) | Keep API; ensure MarketAlerts table has published rows for demo window | **P2** if feed often empty; **P3** pagination past 100 |
| 6 | **LOI Market Hub** | `public/loi-database-dashboard.html` | **Mock — hardcoded** `DEAL_POOL` (~100 deals), comment: replace with Airtable later | No live API | **Yes — client** (`pageSize` 100, prev/next) | **In-memory** `CACHE_TTL` 5 min for derived stats (same page) | **Client-side** search/sort on `allData` | No | Implicit via table render (no global wave on first paint) | No “10–30s” style | Yes — `#emptyState` when filters match zero rows | **Low** for “empty” (always has sample data). **Medium** if presenter claims live LOI — **banner always visible** | Keep for demo as **sample benchmark**; do not claim live LOI; populate via API later | **P1** positioning/copy; **P3** backend integration |
| 7 | **Partner Directory** | `public/partner-directory.html`, `public/partner-directory.js` | **Backend API** | `GET /api/partner-directory`; favorites: `/api/partner-directory/favorites` | **No** (full companies/individuals lists) | **Client TTL** ~5 min (`PARTNERS_CACHE_TTL_MS`); tab DOM cache; modal team/brand maps | **Client-side** search, sort, tab filters | No | Yes — `#loadingState` | No long estimates | Yes — `#emptyState` hidden until no results | **Low** with cache warm. Stats footnote: **“Sample figures for demo — not live pipeline totals.”** on some cards | Keep API; hide or clarify sample stats if confusing | **P2**; **P3** pagination if directory grows |
| 8 | **Deal Compare** | `public/deal-compare.html`; also tab inside `my-deals.html` | **Backend API** | `GET /api/deal-compare/proposals?dealId=`; `GET /api/brand-library/brands` + per-brand `GET /api/brand-library/brand?brandId=` | **No** | **In-memory** `brandDetailsCache` per session | **Client** brand chip selection; proposals filtered server-side by deal | No | Yes — “Loading proposals…” **“Estimated time: 5–15 seconds”** | **Yes:** 5–15s | Yes — instructs to select 2+ brands; no contacted / no submitted proposal messages | **Medium** empty deal or no submitted proposals — looks “empty” but copy explains. **P0** if opened without `dealId` and no brands | Soften 5–15s copy; open from My Deals with a deal that has submitted proposals for demo | **P1** copy; **P2** empty-deal demo script |

---

## C. P0 / P1 recommended fixes

### P0 — Demo-breaking (environment or total failure)

| ID | Item | Mitigation before external demo |
|----|------|----------------------------------|
| S4-P0-1 | **Radar map empty** when `/api/brand-presence` fails or env vars missing | Verify `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID_ALT`; hit Radar once; confirm map markers render |
| S4-P0-2 | **My Deals blank** after loader (auth or `getMyDeals` error) | Confirm Memberstack session + `MY_DEALS` Airtable access; test `view=initial` in staging |
| S4-P0-3 | **Market Alerts empty feed** with no explanation | Seed **MarketAlerts** with `Published At` in 7d window, or widen time pill to **All** during demo |

### P1 — Should fix before external demo (credibility, not necessarily outage)

| ID | Item | Action |
|----|------|--------|
| S4-P1-1 | **Long estimated-time copy** (My Deals 10–30s, Deal Compare 5–15s, Radar 3–5s, Operator 2–6s, Brand Explorer 1–6s) | Replace with neutral copy (“Loading deals…”) or remove time line; keep wave/progress |
| S4-P1-2 | **Radar “Loading hotel data from Airtable…”** | User-facing: “Loading hotel census…” (align with Step 3 Airtable cleanup) |
| S4-P1-3 | **Operator Explorer detail = mock** | Add visible “Preview profile (sample layout)” on gold popup, or avoid clicking into detail in demo |
| S4-P1-4 | **LOI Market Hub** permanent mock banner | Script demo as “sample LOI benchmarks”; or hide route from nav for strict “all live” story |
| S4-P1-5 | **My Deals lazy tab counts** (Matched / Deal Compare show 0 until tab visited) | Pre-open tabs once after load, or document presenter flow |
| S4-P1-6 | **Brand Explorer demo brands** mock banner | Use real brand records in demo script, or acknowledge sample brands |

---

## D. Longer-term performance & data architecture

1. **My Deals** — `view=initial` already trims payload; consider persisted server-side cache per user, background refresh, and **server-side filter** for large deal portfolios. Keep secondary data lazy but **prefetch** after first paint for tab counts.
2. **Radar / brand presence** — Replace `limit=100000` full dump with **viewport/bbox queries**, incremental marker loading, and CDN-friendly static tiles for census baseline. Server cache is good; add **ETag** or short CDN cache for repeat visits.
3. **Brand library list** — Server list cache (~2 min) is appropriate; add **cursor pagination** or virtualized list when brand count exceeds ~200.
4. **Market Alerts** — Raise limit or add **cursor pagination** if feed exceeds 100 items; optional edge cache for anonymous rail.
5. **Partner Directory** — Extend existing 5 min client cache; paginate or virtualize card grid for large directories.
6. **LOI Hub** — Planned Airtable (or warehouse) integration with same pagination/filter pattern as other hubs; until then, keep mock isolated from “live data” nav narrative.
7. **Operator Explorer** — Replace `operator-explorer-gold-mock` with `GET /api/third-party-operators/:id` (or equivalent) for parity with Brand Explorer detail.
8. **Cross-cutting** — Standardize on **one loading pattern** (wave OK; avoid advertising seconds). Introduce **skeleton cards** only where layout shift hurts (large grids). Separate **performance** work from **copy** work in tickets.

---

## E. Manual QA checklist

Use staging/production with the same auth as the demo user.

### Environment prep

- [ ] `GET /api/my-deals?view=initial` returns 200 and `deals` array within acceptable time (&lt;15s cold, &lt;5s warm).
- [ ] `GET /api/brand-library/brands` returns 200 with non-empty `brands`.
- [ ] `GET /api/brand-presence?limit=100000` returns `success: true` and `hotels.length` &gt; 0.
- [ ] `GET /api/market-alerts?timeWindow=7d&limit=100` returns items (or confirm empty is acceptable with copy).
- [ ] `GET /api/third-party-operators?activeOnly=1` returns operators.
- [ ] `GET /api/partner-directory` returns companies/individuals.

### Per-page (app shell iframe)

**My Deals**

- [ ] First visit: wave loader → table populates; no indefinite spinner.
- [ ] Note whether “10–30 seconds” appears (flag for copy fix).
- [ ] Apply search/filter: empty state message is readable.
- [ ] Open **Matched Brands** tab: data or clear empty; tab count updates.
- [ ] Open **Deal Compare** tab (in-page): proposals load or empty message explains why.

**Brand Explorer**

- [ ] Directory loads; filters reduce list; empty filter shows “No brands found”.
- [ ] Open a **production** brand: detail loads without Mock Data banner (unless demo brand).
- [ ] Open detail: no stuck “Waiting for live brand data…”.

**Operator Explorer**

- [ ] List loads; search works.
- [ ] Click operator: popup loads mock page within ~6s; note mock vs live for presenter script.

**Dealality Radar**

- [ ] Map shows markers after load; statistics sidebar populates.
- [ ] Note “Airtable” in loading message (flag).
- [ ] Switch to ranked list tab: load completes or `rlEmpty` with error text.
- [ ] Throttle network (DevTools): failure shows message, not silent blank chrome.

**Market Alerts**

- [ ] 7d feed shows cards or friendly empty.
- [ ] Category/region/search refetch; spinner then results.
- [ ] Demo banner stays hidden (unless intentionally enabled).

**LOI Market Hub**

- [ ] Mock banner visible; table paginates (100/page).
- [ ] Search narrows rows; empty state when no matches.

**Partner Directory**

- [ ] Companies tab loads; switch tabs without full reload hang.
- [ ] Search empty state; sample stats footnote acceptable for demo.

**Deal Compare** (standalone `/deal-compare?dealId=…`)

- [ ] With valid `dealId` and submitted proposals: comparison table renders.
- [ ] Without proposals: empty copy mentions Matched Brands / Submitted status.
- [ ] Note “5–15 seconds” on load (flag).

### Demo script safeguards

- [ ] Pre-warm: open My Deals, Brand Explorer, Radar once before screen share.
- [ ] Avoid Operator detail click unless explaining mock preview.
- [ ] Position LOI Hub as illustrative sample data.
- [ ] Do not use `?fresh=1` on Brand Explorer unless demonstrating cache bypass.

---

## References

| API module | Path |
|------------|------|
| My Deals | `api/my-deals.js` → `GET /api/my-deals` |
| Brand library | `api/brand-library.js` → `/api/brand-library/brands`, `/brand` |
| Brand presence | `api/brand-presence.js` → `/api/brand-presence` |
| Market alerts | `api/market-alerts.js` → `/api/market-alerts`, `/rail` |
| Third-party operators | server route → `/api/third-party-operators` |
| Partner directory | server route → `/api/partner-directory` |
| Deal compare | server route → `/api/deal-compare/proposals` |

**Related audits:** `reports/dealality-step-3-visible-copy-cleanup-audit.md`, `reports/frontend-links-audit.json` (Step 1 routes).
