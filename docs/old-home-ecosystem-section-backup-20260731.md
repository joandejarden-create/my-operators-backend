# Old Home — Ecosystem section backup (2026-07-31)

## Scope lock
- Page: Old Home `/old-home` (`68108c2a063eeb5d1bd7ae90`) — **not** root `/`
- Do not publish
- Do not change hero, Problem (`#about`), Owner Benefits (`#modules`), or How Dealality Works (`#oh-how-we-do-it`, JS-injected)

## Section order before edit (Designer top-level under `#dc-premium`)

1. `#nav` / `#mnav`
2. `#hero`
3. `#about` (Problem / Deal Desk)
4. `#about-backup` / hidden `oh-about` (if present)
5. `#perspectives`
6. `#platform-features` (hidden)
7. `#many-futures` (product-feature visuals)
8. `#modules` (Owner Benefits)
9. `#trust` (testimonials)
10. `#pricing`
11. `#faq`
12. `#insights`
13. `#cta-band`
14. `#footer-new`

## Live runtime note
`#oh-how-we-do-it` is injected by `old-home-how-we-do-it` **before** `#platform-features` (fallback `#modules`).

## Planned insertion
- **Designer:** new section inserted **before** `#many-futures` (`6ca557cf-1f72-acc8-c85a-502c8f198b4a`)
- **Live order:** How Dealality Works → **Ecosystem** → Many Futures → Benefits

## Benefits icon system (reuse)
- Wrapper class: `.mod-icon`
- Size: 72×72, radius 999px
- Background: radial purple glow
- Box-shadow: `0 0 32px rgba(108,114,255,.32)`
- SVG: 52×52, stroke `#9B8AFB`, stroke-width ~1.5, viewBox `0 0 40 40`
- Source styling: `public/marketing/old-home-modules-icons.v20260730f.js` (`.mod-icon` rule)
- Card surface reference: `#mod-*` gradient `#0E1630 → #0A1228`, radius 18px, border `rgba(255,255,255,.08)`

## Split eyebrow system (reuse)
- Classes: `oh-faq-badges`, `oh-faq-badge`, `oh-faq-badge-left`, `oh-faq-badge-right`
- Parallel: `oh-cta-band-badge*`

## Built section (Designer, unpublished)

- Section `#ecosystem` (`dec6a9c1-4a5f-d3be-d91d-5e7c32e1719b`) — class `oh-eco`
- Insertion confirmed: after hidden `#platform-features`, **before** `#many-futures`
- Live runtime: How Dealality Works (JS) → Ecosystem → Many Futures → Benefits

### Structure
`#eco-inner` → badges (`oh-faq-*`) → `#eco-h2` → `#eco-lead` → `#eco-grid` → 4× `article.oh-eco-card`
- Card 1 lead: `oh-eco-card--lead` + `Decision Lead` + `mod-icon oh-eco-icon--lead`
- Icons: HtmlEmbed SVG (people / building / document-search / rising bars), stroke `#9B8AFB`, 52×52

### New classes
`oh-eco`, `oh-eco-inner`, `oh-eco-h2`, `oh-eco-lead`, `oh-eco-grid`, `oh-eco-card`, `oh-eco-card--lead`, `oh-eco-lead-label`, `oh-eco-card-h`, `oh-eco-card-p`, `oh-eco-icon--lead`, `oh-eco-badge-left`, plus native `mod-icon` mirror of Benefits icon wrapper

## Restore
Delete the new `#ecosystem` section (or hide it) if rollback is needed. No other sections should be moved.
