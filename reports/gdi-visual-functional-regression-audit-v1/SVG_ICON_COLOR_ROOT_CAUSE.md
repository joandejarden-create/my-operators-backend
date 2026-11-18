# SVG icon color root cause

## Why the SVG colors disappeared

Opportunity cards still use `stroke="currentColor"` and CSS gold for `--active`. Icons appeared muted gray because **almost every Research Preview row resolved to `revenueStreamIcons = { guestrooms: muted, eventFnb: muted }`**.

For Sheraton Guadalajara Expo research store:

| Metric | Before fix |
|---|---|
| Rows with `revenueStreamClass` GUESTROOMS_ONLY / COMBINED | 43 / 43 |
| Rows with `salesOpportunityQuickFilterKeys` | 43 / 43 |
| Rows with frozen `revenueStreamIcons` | 0 |
| Thesis prose (`guestroomThesis` / `eventFnbThesis`) | empty |
| `resolveOpportunityRevenueStreamIcons` active guest / catering | **0 / 0** |
| Direct class→icon map (no consistency demotion) | **43 / 25** |

## Mechanism

1. Research inventories keep commercial classification (`revenueStreamClass` + Sales Opportunities keys).
2. Customer presentation sanitize / thesis scrub removes or never carries thesis prose.
3. `applyDrawerConsistencyGuard` then demotes class-mapped active icons to muted when thesis/evidence text is absent.
4. Final class collapses to `REVENUE_STREAM_UNCONFIRMED`.
5. UI prefers DTO / `canonicalCustomerPresentation.icons` → both muted → gray SVGs.
6. PACKAGE attach already had `restoreIconsFromPersistedClassAndSalesKeys` for this exact failure mode, but **live Research Preview sanitize did not share that restore before consistency demotion**.

## Which change introduced it

Not a single CSS PR. The demotion/consistency path is intentional anti-invention protection. The **unintended gap** is that Research Preview lost the package-side restore that was added during the Sheraton SVG repair (`reports/gdi-sheraton-need-dates-svg-cmi-v1/`, package attach V1). Recent Full E2E / inventory work made the gap highly visible because Research Preview again shows the full classified inventory (PR #357) without frozen icon bytes.

## Fix

1. Promote `restoreIconsFromPersistedClassAndSalesKeys` into `opportunity-revenue-stream-icons-v1.js`.
2. Call it inside `resolveOpportunityRevenueStreamIcons` before consistency demotion.
3. Reuse the same helper from PACKAGE attach (no dual policy).
4. UI: trust explicit API/package icon states; restore from class+sales keys when API icons are both muted; do not re-demote trusted states after thesis scrub.

## What was not the cause

- Gold CSS token removal
- SVG path markup loss
- Hotel-specific stylesheet overrides
- Publication pointer corruption
- Need-date calendar CSS deletion (calendar gray remains a legitimate `not-assessed` / `no-match` state when evidence is absent)
