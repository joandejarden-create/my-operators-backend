# Ecosystem rebuild return notes — 2026-07-31

## Webflow (unpublished)
- Page: Old Home `68108c2a063eeb5d1bd7ae90`
- Section marker: `section#ecosystem[data-oh-ecosystem="owner-advisor-led"]`
- Page freeform head CSS:
  `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cd7cd3737b3abf85f7993_dealality-old-home-ecosystem.v20260731i.css`
- Site freeform head: unchanged (root `/` untouched)
- Publish: not performed

## Structure
eyebrow → h2 → lead → `#oh-eco-stage` → `#oh-eco-process` → `#oh-eco-close`

Desktop stage grid:
Brands ↔ Owners | or | Advisors ↔ Capital
+ owner-control band under center leads

## Existing classes reused
- Section: `oh-eco`
- Inner: `oh-eco-inner`, `oh-eco-inner--wide`
- Split eyebrow: `oh-faq-badges`, `oh-faq-badge`, `oh-faq-badge-left`, `oh-faq-badge-right`, `oh-eco-badge-left`
- Headline/lead: `oh-eco-h2`, `oh-eco-lead`
- Icons: `mod-icon` (+ ecosystem combo modifiers)
- Cards: `oh-eco-card-1`, `oh-eco-card-h-1`, `oh-eco-list`

## New classes added
- `oh-eco-stage`, `oh-eco-stage-grid`, `oh-eco-core`, `oh-eco-core-label`
- `oh-eco-brands`, `oh-eco-owners`, `oh-eco-advisors`, `oh-eco-capital`
- `oh-eco-card--participant`, `oh-eco-card--lead-owner`, `oh-eco-card--lead-advisor`
- `oh-eco-role`, `oh-eco-role--owner`, `oh-eco-role--advisor`
- `oh-eco-icon--brands|owner|advisor|capital`
- `oh-eco-conn`, `oh-eco-conn-l|r`, `oh-eco-conn-item`, `oh-eco-conn-label`, `oh-eco-conn-line`
- `oh-eco-or`, `oh-eco-or-pill`, `oh-eco-band`, `oh-eco-band-pill`
- `oh-eco-process`, `oh-eco-step*`, `oh-eco-close*`

## Connectors
Native Webflow DivBlocks + CSS two-way arrow lines (`.oh-eco-conn-line`) with real text labels:
Opportunity Context / Responses / Capital Criteria.

## Icon method
Reuse Benefits `mod-icon` wrappers; glyph via scoped CSS `::after` data-URI (Benefits span-hidden pattern).

## Recommendation
**B. Minor visual refinement required** — composition, hierarchy, and copy match the approved owner/advisor-led story. Final staging-preview pixel pass still needed because Designer canvas does not fully apply page freeform CSS.
