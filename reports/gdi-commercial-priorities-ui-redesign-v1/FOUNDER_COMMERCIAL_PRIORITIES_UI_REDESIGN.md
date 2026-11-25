# FOUNDER — GDI Commercial Priorities UI Redesign V1

**Scope:** UI/UX only · same data · same APIs · same contracts  
**Hotel verified:** Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o`  
**Preview URL:** `http://localhost:8080/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged&isolation=portal_v1_test`

## Files changed

- `public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js`
- `public/css/hotel-commercial-priorities.css`
- `public/js/group-demand-intelligence/app.js` (pass `hotelLabel` into Tab 2 mount)
- `public/group-demand-intelligence.html` (cache bust)
- `lib/group-demand-intelligence/need-date-demand/need-dates-management-service-v1.js` (additive hotel location fields for display)
- `scripts/test-gdi-hotel-need-times-priorities-tab-v1.mjs`
- `scripts/test-gdi-hcp-v3-multi-hotel-isolation-v1.mjs`
- `scripts/test-gdi-no-login-alert-v1.mjs`

## Design system reused (My Deals / intake shell)

- `dealality-intake-form-shell.css` tokens (`--accent--primary-1`, neutrals, chip/button base)
- `.intake-shell` / `.btn-primary` / `.btn-secondary` / `.form-section-title` / `.form-input`
- `.hcp-chip` selected purple/lavender treatment (strengthened, not replaced)
- Sticky save pattern refined to a single bottom bar

## Browser verification

| Check | Result |
|---|---|
| Wyndham name + `San Diego, California` | PASS |
| Planning year 2027 | PASS |
| Seven sections present | PASS |
| Summary strip live updates (chip → “1 of 5”) | PASS |
| Airtable / PR language removed | PASS |
| Sticky save: All changes saved / Unsaved / disabled when clean | PASS |
| Isolation mode (no live Wyndham Airtable mutation for UI checks) | PASS |
| Desktop screenshot | `desktop-wyndham-commercial-priorities.png` |
| Mobile 390 screenshot | `mobile-390-wyndham-commercial-priorities.png` |
| Opportunities tab still loads CLEAN 11 | PASS |

## Tests

- `npm run test:gdi-hotel-need-times-priorities-tab-v1` PASS
- `npm run test:gdi-no-login-alert-v1` PASS
- Isolation suite: hotel profile isolation + soft-bridge PASS; `bethesda_customer_feed_still_19` FAIL is **pre-existing local inventory count (67≠19)**, unrelated to this UI redesign

## Remaining UI notes

- Guided-tour overlay can still appear over Tab 2 on first visit (existing product behavior).
- Hotel selector chrome remains on Opportunities; Tab 2 uses hotel identity card + label enrichment instead.
