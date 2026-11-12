# Restore Hotel Need Times & Priorities as GDI Tab 2

## 1. Historical source

| Item | Evidence |
|------|----------|
| Branch | `feat/gdi-universal-need-date-intelligence-v1` |
| Commit | `7fa87ebfd` — `feat(gdi): add universal hotel need-date intelligence v1` |
| Tip (later polish) | `fb7086b8b` on same branch |
| Ancestor of main? | **No** — never merged as a hotel management portal |

**Why it disappeared (determinable):** Need-Date V4 shipped to main as a **presentation-only** cut (`6d400c465` / PR #154): opportunity-card Need-Date SVGs + hotel status legend. The standalone Need Dates management portal (`public/hotel-need-dates.html` + `hotel-need-dates-app.js`) stayed on the feature branch and was never wired as a GDI tab. A separate `hotel-commercial-priorities-app.js` was **referenced** by a stub HTML title but **never committed** to git.

## 2. Recovered originals

| Asset | Status |
|-------|--------|
| `hotel-need-dates-app.js` (831 lines) | Recovered → adapted as Tab 2 module |
| `hotel-need-dates.css` | Recovered → `public/css/hotel-need-times-priorities.css` |
| Need-period CRUD service + Airtable/local stores | Already on main |
| `hotel-commercial-priorities-contract-v1.js` + profile store | Already on main |
| Soft ranking boost via commercial profile | Already in `need-period-strategy-selection-v1.js` |
| `hotel-commercial-priorities-app.js` full portal UI | **Missing** — never in git |
| API routes `/need-dates`, `/need-periods` | **Were missing on main** — restored in this PR |

## 3. Final tab layout

1. **Opportunities** — existing customer feed (unchanged; Reset View only; no Run Research / Publish)
2. **Hotel Need Times & Priorities** — restored need-period calendar + commercial priorities form (contract enums)

## 4. Persistence

- Need periods: local `data/group-demand-intelligence/hotels/{hotelId}/need-periods.json` or Airtable when live-enabled; isolated shadow via `isolation=portal_v1_test`
- Commercial priorities: local `data/group-demand-intelligence/hotel-commercial-priorities/{hotelId}.json` or Airtable `GDI Hotel Commercial Profiles`
- Hotel-scoped auth via `assertNeedDatesHotelAccess` (cross-hotel writes denied)
- Writes: Memberstack user **or** `GDI_NEED_DATES_PORTAL_LOCAL_WRITE=1` for local certification

## 5. Existing hotel preferences

- Bethesda / Waterstone GDI `commercialPriorities` in hotel config **unchanged**
- No overwrite of research evidence or opportunity maturity
- Preference match lines are soft explanations only

## 6. GDI supervisor integration

- Existing soft boosts in `selectStrategiesForCampaign` (priority segments / revenue chips)
- New `hotel-preference-match-explanation-v1.js` attaches lines such as “Matches hotel's stated weekday group-room priority.”
- Preferences **do not** reject opportunities outside need windows

## 7. Local URL

`http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8` → Tab **Hotel Need Times & Priorities**

Optional: `&airtable=0` or `&isolation=portal_v1_test` for isolated local shadow writes.

## 8–10. Verification

- `npm run test:gdi-hotel-need-times-priorities-tab-v1` — pass
- `npm run test:gdi-hotel-toolbar-reset-view-only-v1` — run with PR
- Opportunities listing / Reset View / Decision Events — not modified by Tab 2 mount path

## 11. Files modified / added

- `public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js` (new)
- `public/css/hotel-need-times-priorities.css` (new)
- `public/js/group-demand-intelligence/app.js`
- `public/js/group-demand-intelligence/dealality-gdi-ui.js`
- `public/group-demand-intelligence.html`
- `api/gdi-need-dates-portal.js` (new)
- `middleware/gdiNeedDatesWriteAuth.js` (new)
- `server.js`
- `lib/.../need-dates-management-service-v1.js`
- `lib/.../need-period-drawer-component-v1.js`
- `lib/.../hotel-preference-match-explanation-v1.js` (new)
- `lib/.../index.js`
- `scripts/test-gdi-hotel-need-times-priorities-tab-v1.mjs` (new)
- `package.json`

## 12. PR / merge

Filled after GitHub PR create.
