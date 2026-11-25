# FOUNDER — PR #611 UI correction: full-width sections + middle tab

**PR:** [#611](https://github.com/joandejarden-create/my-operators-backend/pull/611)  
**Scope:** Layout + tab position only · no API / contract / persistence changes  
**Hotel:** Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o`  
**Preview:** `http://localhost:8080/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged&isolation=portal_v1_test`

## Corrections

1. **Full-width section cards** — removed `.form-sections-wrap` `max-width: 960px` and overrode intake-shell `920px` so all seven section cards share the same usable width as the hotel identity card + summary strip.
2. **Middle tab** — customer order is now Opportunities → Commercial Priorities → How to Read This Report → Portfolio (IDs/routes preserved).

## Browser confirmation (measured)

| Check | Result |
|---|---|
| Hotel / summary / all 7 sections same width | PASS (`deltaW=0` at 1440 / 1280 / 768 / 390) |
| Horizontal scroll | none |
| Tab order | Opportunities → Priorities → How to Read → Portfolio |
| Sticky save usable | PASS |
| CLEAN 11 Opportunities still loads | PASS (`All (11)`) |
| Isolation preview (no live preference mutation) | PASS |
| Console / role=alert errors | none observed |

## Screenshots

- `desktop-1440-wyndham-cp-fullwidth.png`
- `desktop-1280-wyndham-cp-fullwidth.png`
- `tablet-768-wyndham-cp-fullwidth.png`
- `mobile-390-wyndham-cp-fullwidth.png`

## Tests

- `npm run test:gdi-hotel-need-times-priorities-tab-v1` PASS
- `npm run test:gdi-pdi-3-portfolio-tab-ui-v1` PASS
- `npm run test:gdi-no-login-alert-v1` PASS
- `npm run test:gdi-guided-report-tour-v1` PASS

**Status:** OPEN → hand to Integration Controller after CI green.  
GDI does not merge.
