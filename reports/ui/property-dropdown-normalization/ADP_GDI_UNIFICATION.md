# Property label unification — ADP + GDI

**Date:** 2026-10-05

## Shared formatter
`lib/dealality/property-display-label.js`

- `formatPropertyLocationLine({ city, state, region, country })`
- `formatPropertySelectorLabel({ name|hotelName|displayName, city, state, region, country })`
- ISO-2 countries expand when known (`IT` → `Italy`)

## Rules
| Inputs | Output |
|---|---|
| city + state | `City, State` (US and international region) |
| city + country (no state) | `City, Country` |
| city only | `City` |

Never: dangling commas, `null`/`undefined`, duplicate country after state.

## Wires
| Surface | Path |
|---|---|
| GDI selectable hotels | `lib/group-demand-intelligence/repository.js` |
| GDI API legacy fallback | `api/group-demand-intelligence.js` |
| ADP property list | `lib/ai-demand-positioning/data-model.js` → `label` / `locationLine` |
| ADP share display | `lib/ai-demand-positioning/share/adp-share-property-display-v1.js` |
| ADP report property | `api/ai-demand-positioning.js` |
| ADP UI selector + brief | `public/js/ai-demand-positioning/ai-demand-positioning.js` |
| Select width / ellipsis | `public/css/deal-workspace-shell.css` |

## Regression
`npm run test:property-display-label`

Hardcoded W Rome special case: **NO**
