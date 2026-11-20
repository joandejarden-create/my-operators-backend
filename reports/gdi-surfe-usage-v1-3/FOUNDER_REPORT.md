# Surfe Usage V1.3 — Founder Report

**Date:** 2026-11-20  
**Branch:** `feat/gdi-surfe-usage-v1-3-ui-attribution` (from PR #497 tip `706fc97d0`)  
**Persistence classification:** `PERSISTENCE_DESIGNED` (unchanged from V1.2)  
**STOP:** No production deploy. No Airtable table create. No paid Surfe calls.

---

## 1. UI root causes

| Issue | Cause |
|-------|--------|
| Horizontal overflow / clipped columns | Three summary tables forced into equal `minmax(280px)` columns with 8–9 cramped fields |
| Nested scrolling | Panel `max-height: 360px` inside page scroll |
| Truncated hotel / opportunity names | Single-line cells with technical IDs only; no dual-line identity |
| UNKNOWN everywhere | All 29 live ledger rows are `FIXTURE_TEST` (`hotel_parallel` / `hotel_demo` / `opp_demo_*`) — correctly unresolved against the GDI hotel registry |

## 2. Layout corrections

Stacked sections (not three equal narrow columns):

1. **Top** — filters (Hide fixture/test **on by default**), KPIs, trends  
2. **Middle** — full-width Usage by Opportunity Hotel → Requesting Organization → Most Researched Opportunities  
3. **Bottom** — full-width Activity History + Operational Visibility sidebar  

Per-table horizontal scroll only; shell `overflow-x: hidden`; responsive stack under 1100px / 720px.

## 3. Updated screenshots

Captured from V1.3 layout preview (real hotel/opportunity titles) at:

- `screenshots/01-desktop-1920.png`
- `screenshots/02-desktop-1440.png`
- `screenshots/03-desktop-1280.png`
- `screenshots/04-narrow-mobile.png`

Live `localhost:8080` still serves V1.2 until this PR is merged / Integrated Staging cut over. Preview URL used for capture: worktree `:8095/app/admin/surfe-usage-v13-layout-preview.html`.

## 4. Hotel and market attribution results

| Hotel ID | Resolved name | Location |
|----------|---------------|----------|
| `recLuxvwwxID7U2B8` | Bethesda Marriott | Bethesda, Maryland |
| `recCAGqkERSXgiku4` | Sheraton Guadalajara Expo | Guadalajara, Mexico |
| `recH9h3McgMNFQEla` | Radisson Blu Toronto Downtown | Toronto, Canada |
| `hotel_parallel` / fixtures | **UNKNOWN** + Investigate flag | (correct — not in registry) |

Canonical source: `listGdiSelectableHotels()` via `gdi-surfe-usage-context-v1.js`. Names are never invented from technical IDs.

## 5. Opportunity title / deep-link results

| Opportunity ID | Title | Drawer |
|----------------|-------|--------|
| `gdi_opp_nado_ddaa_washcon_2028` | NADO & DDAA Washington Conference 2028… | `/app#/group-demand-intelligence?hotelId=recLuxvwwxID7U2B8&opportunityId=gdi_opp_nado_ddaa_washcon_2028` |
| `disc_1490e4966ee8` | Insignia Yo también juego rumbo a la Copa Mundial FIFA 2026 | hotel `recCAGqkERSXgiku4` |
| `rb_gdi_disc_dc428c1bd4c4e412` | ISCT 2027 Toronto Annual Meeting | hotel `recH9h3McgMNFQEla` |

GDI `app.js` now auto-opens `opportunityId` from query after `loadAll()` (`openOpportunityFromQueryOnce`).

## 6. Request-origin attribution results

Opportunity hotel and Requested From are separate columns.

Demo examples:

- Opportunity hotel: Sheraton Guadalajara Expo (Guadalajara)  
- Requested from: Jalisco Regional Sales Desk · `office_gdl_sales` · Guadalajara  

Never defaults requesting office to opportunity hotel. No IP geolocation. Missing origin → **UNKNOWN**.

## 7. Real-record simulation results

`scripts/test-gdi-surfe-usage-v1-3.mjs` + seed dry-run:

1. Load real hotel + opportunity from GDI data  
2. Enrich context  
3. Append mocked Surfe accounting (no provider call)  
4. Verify local ledger fields, privacy strip, dedupe on `clientRequestId`  
5. Verify summary projections + drawer href  

Seed (apply when reviewing locally):

```bash
npm run gdi:surfe-usage-seed-layout-demos-v1-3 -- --dry-run
npm run gdi:surfe-usage-seed-layout-demos-v1-3 -- --apply
```

Class: **DEVELOPMENT** + `layoutValidationOnly` — not PRODUCTION adoption.

## 8. Fixture / test handling

- Existing **29** ledger rows remain **`FIXTURE_TEST`**
- Default filter: **Hide fixture/test** checked  
- Fixtures do not invent hotel/opportunity names  
- KPI still surfaces fixture count; adoption note preserved  

## 9. Airtable status

**PROPOSED_AWAITING_FOUNDER** — unchanged from V1.2. No table created. Sync gated / dry-run. Local filesystem ledger remains SoT.

## 10. Test and CI results

| Gate | Result |
|------|--------|
| `npm run test:gdi-surfe-usage-v1-3` | PASS |
| `npm run test:gdi-surfe-usage-v1-2` | PASS |
| Layout CSS/HTML regression asserts | PASS |
| Real hotel/opp resolution | PASS |
| Deep-link wiring present | PASS |
| Privacy / no duplicate | PASS |

CI runs on the PR after push.

## 11. PR and integration status

- Builds on open PR **#497** (V1.2) tip  
- Focused V1.3 PR opened from `feat/gdi-surfe-usage-v1-3-ui-attribution`  
- Registered `ws-gdi` left on dirty `feat/gdi-google-style-contact-search-before-surfe-v1` (preserved)  
- **Do not** cut over production or Integrated Staging until founder approval  

### Final acceptance (local V1.3)

Founder can see **hotel name + market**, **GDI opportunity title (clickable)**, and **requesting org/office** without critical clipping in the stacked layout. Fixtures remain explicitly classified.

---

**AWAITING_INTEGRATION_CONTROLLER**
