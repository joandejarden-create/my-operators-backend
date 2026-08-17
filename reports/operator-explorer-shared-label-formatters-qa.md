# Operator Explorer — shared label formatters QA

## Summary

Operator Explorer list UI now loads shared scripts instead of ~580 lines of inline JavaScript. Badge and type labels use `dealality-ui-labels.js`; page behavior lives in `operator-explorer.js` (favorites tabs, filters, gold-mock popup preserved).

## Files changed

| File | Change |
|------|--------|
| `public/operator-explorer.html` | Removed inline `DOMContentLoaded` script; added `dealality-ui-labels.js` + `operator-explorer.js` |
| `public/js/operator-explorer.js` | Favorites tab + stars merged from HTML; badges/tooltips delegate to `DEALALITY_UI_LABELS`; popup token flow from HTML |
| `public/js/dealality-ui-labels.js` | Added `formatOperatorExplorerTypeLabel`, `buildOperatorExplorerCardBadges`, `getOperatorExplorerCardBadgesTooltip` |
| `reports/dealality-ui-labels-proper-case.md` | Note that Explorer is wired |

**Not changed:** `lib/company-workspace-access.js` (eligibility/API badges already use `Third-Party Management:`), Airtable, Memberstack, workspace switcher.

## Inline logic

| Status | Detail |
|--------|--------|
| **Removed** | Entire inline list script in `operator-explorer.html` (previously lines 977–1552) |
| **Retained as fallback** | `operator-explorer.js` `buildOperatorBadgesHtml` / `operatorTypeLabel` — minimal copy if `window.DEALALITY_UI_LABELS` fails to load |

## Badge formatting examples (Proper Case)

| Input | Display badge |
|-------|----------------|
| `isOwnerOperator: true` | `Hotel Owner - Operator` |
| `thirdPartyManagementAvailabilityStatus: "Yes"` | `Third-Party Management: Yes` |
| `thirdPartyManagementAvailabilityStatus: "Selectively"` | `Third-Party Management: Selectively` |
| `thirdPartyManagementAvailabilityStatus: "Case-by-Case"` | `Third-Party Management: Case-by-Case` |
| `reviewBeforeOutreach: true` | `Review Availability Before Outreach` |
| `workspaceAccess: ["Demo"]` | `Demo Mode` |
| Server `companyDisplayBadges` array | Used as-is (includes aligned `Third-Party Management:` prefix from API) |

Card subtitle (`brand-card__type`): `Hotel Owner - Operator` or `3rd Party Operator` via `formatOperatorExplorerTypeLabel`.

## Tooltip / helper text (sentence case)

Badge row `title` (single tooltip on `.operator-card__badges`):

> Owner-Operator means this company owns or controls hotel assets and also operates hotels. Availability for third-party management may vary by market and deal type.

Favorite star `title`: `Save to favorites` (unchanged, short action label).

## Missing badge fields

- Operators without owner-operator / third-party / review / demo fields: **no badge row** (`buildOperatorBadgesHtml` returns `""`).
- Cards still render: name, type line, regions, summary, website, View Operator, favorite star.
- List fetch and filters unchanged; eligibility still enforced server-side (`operatorExplorerEligible`).

## Script load order

1. `operator-explorer-favorites.js` (defer)
2. `dealality-ui-labels.js` (sync — available before deferred bundles run)
3. `operator-explorer.js` (defer)
4. `embed-chrome.js` (defer)

## Manual regression checklist

- [ ] Open `/operator-explorer.html` — list loads, no console errors.
- [ ] Owner-operator row shows `Hotel Owner - Operator` type and badges with Proper Case.
- [ ] Hover badges — tooltip is sentence case (see above).
- [ ] Operators / Favorites tabs and star save still work.
- [ ] Click card / View Operator — gold-mock popup opens for `rec…` ids.
- [ ] Operator with no company badge fields — card renders without badge strip.
- [ ] Clear filters, chain-scale legend, sort — unchanged behavior.

## Change impact

**Low–medium** — UI/display and script bundling only; no write paths or eligibility rules modified.
