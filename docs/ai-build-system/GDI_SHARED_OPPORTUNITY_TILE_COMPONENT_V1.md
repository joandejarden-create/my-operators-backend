# GDI Shared Opportunity Tile Component (permanent)

**Status:** Binding  
**Authorization:** Founder direction — Executive Summary Simplification & Shared Opportunity Tiles

## Rule

The same opportunity must look identical, contain identical information, and open the same drawer regardless of where it is displayed within GDI.

## Canonical component

- Markup / styling / field mapping: `DealalityGdiUi.opportunityTileHtml` in `public/js/group-demand-intelligence/dealality-gdi-ui.js`
- Grid helper: `DealalityGdiUi.opportunityCardsGridHtml`
- Brief adapter: `DealalityGdiUi.opportunityToBriefItem`

## Surfaces that must reuse it

1. Group & Demand Opportunities — Tiles View
2. Executive Summary — Priority Commercial Opportunities (up to 4)

## Prohibited

- Surface-specific opportunity card markup or CSS clones
- Surface-specific commercial field remapping for tiles
- Separate drawer openers that diverge from Opportunities detail behavior
- Executive-facing research-instruction copy on tiles (e.g. “No invented urgency”)

## Opportunity of the Week

Identify OOTW **outside** the tile component so the underlying tile remains identical across surfaces.

## Propagation

Future changes to the canonical tile automatically apply to every surface that calls `opportunityTileHtml` / `opportunityCardsGridHtml`. Do not fork renderers.
