# GDI ↔ Dealality UI System Audit

**Date:** 2026-09-12  
**Goal:** Make Group Demand Intelligence feel like a first-class Dealality module, not a bolted-on microsite.

---

## Verdict of audit

GDI’s **product shape** matches **Market Demand** (standalone intelligence page).  
GDI’s **visual language** must match **ADP / Brand AI / Market Demand** tokens (`#080f25`, `#101935`, `#6c72ff`, Segoe UI / Mona Sans), not the current Fraunces + `#3d8bfd` prototype palette.

---

## App shell

| Piece | Location |
|-------|----------|
| Authenticated shell | `public/app.html`, `public/app.css`, `public/app.js` |
| Sidebar + iframe | Products load as iframe children; pages use `embed-mode` |
| Nav model | `public/app.js` Market Intelligence group |

**Action:** Register `/group-demand-intelligence` in `app.js` under Market Intelligence (owner/admin pilot) so internal users get the real Dealality shell.

Market Demand itself is also often opened standalone; shell registration is the native path for authenticated product modules.

---

## Design tokens (canonical)

From `deal-workspace-shell.css`, `ai-visibility-shared.css`, `market-demand.css`, `app.css`:

| Token | Value |
|-------|--------|
| Page bg | `#080f25` (`--neutral--800`) |
| Panel / card | `#101935` (`--secondary--color-1`) |
| Border | `#2a3560` / `#37446b` |
| Body / muted | `#e8ecff` / `#9aa8d8` |
| Accent | `#6c72ff` |
| Status green / warn / red | `#14ca74` / `#d5691b` / `#dc2b2b` |
| Font | Segoe UI / Mona Sans / Inter — **not** Fraunces |

---

## Components to reuse (patterns)

| UI need | Canonical pattern | Source |
|---------|-------------------|--------|
| Page container | `.md-shell` / `.dashboard-container.aiv-dashboard` | Market Demand / AIV |
| Page title + subtitle | `.md-head` / `.dashboard-header` | MD / ADP |
| KPI cards | `.md-card` / `.aiv-kpi` | MD / AIV |
| Tabs | `.bdd-section-nav` / `.section-nav-item` OR MD-style section tabs | DWS / AIV |
| Tables | `.md-table` / `.aiv-table` | MD / AIV |
| Badges | ADP `.adp-opp-badge` / AIV `.aiv-badge` semantic colors | ADP / AIV |
| Drawer | `<dialog class="aiv-drawer">` pattern | AIV |
| Buttons | Accent primary `#6c72ff`, secondary outline | MD / AIV |
| Share shell | Same UI + noindex; BES/OES for wrapper; BAI share for same-page | BAI / BES |

---

## Visual references (side-by-side QA)

1. **Market Demand** — `public/market-demand.html` + `css/market-demand.css`
2. **AI Demand Positioning** — `public/owner-ai-demand.html` + DWS + AIV CSS
3. **Brand AI share** — `public/brand-ai-visibility-share.html` (external report feel)

---

## What GDI can reuse directly

- Dealality color / typography tokens (map `--gdi-*` → platform values)
- Market Demand layout: max-width shell, KPI grid, section cards, table wrap
- App shell route + sidebar for internal
- Share: noindex + branded report header (Dealality mark + Pilot + Read-only)

## Unavoidable thin local CSS

- Weekly Brief opportunity card layout (domain-specific)
- Priority → High/Medium/Watchlist pill mapping
- Hotel Fit / Evidence score chip grid in drawer
- Evidence table columns
- Share-only brand strip / read-only chip

---

## Gaps before this pass

| Gap | Prototype GDI | Target |
|-----|---------------|--------|
| Fonts | Fraunces + Source Sans 3 | Product sans |
| Colors | `#0f1419` / `#3d8bfd` | `#080f25` / `#6c72ff` |
| KPI blocks | Oversized serif values | MD/AIV KPI cards |
| Tabs | Custom underline | Section-nav style |
| Shell | Standalone only | Register in `app.js` |
| Cost in salesperson KPI | Shown | Move to Research Audit only |
| Share branding | None | Dealality report header |

---

## Implementation order

1. Rewrite `group-demand-intelligence.css` onto Dealality tokens (kill Fraunces/prototype gradients).
2. Rework internal `app.js` markup hierarchy (header, entity, KPIs, tabs, brief cards, table, drawer).
3. Rework `share-app.js` + share HTML for branded read-only report.
4. Register shell route/nav.
5. Visual QA vs Market Demand / ADP / BAI share; run GDI tests.
