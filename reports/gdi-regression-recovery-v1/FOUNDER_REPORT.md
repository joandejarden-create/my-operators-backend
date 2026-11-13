# GDI Regression Recovery — Founder Report

**Branch (local):** `fix/gdi-regression-recovery-v1`  
**Date:** 2026-11-13  
**Scope:** Local recovery only — not published / not deployed to production.

## Root cause

Completed improvements were **not lost to a hard reset**. They were **split across unmerged Git branches and uncommitted working-tree polish**, while the local server was effectively serving a **main-line baseline** that lacked those commits.

| Improvement | Where it lived | Why it disappeared locally |
|-------------|----------------|----------------------------|
| Card quality + Market Scope 48px toolbar tokens + title/contact recovery | Open PR **#230** · `80be5b88d` (`fix/gdi-opportunity-card-quality-v1`) | Never merged to `main` |
| Surfe Find Email/Phone + How-to-Read tour + Competitive-on-Market-Scope polish | Open PR **#231** · `1fdd835cc` (`fix/gdi-surfe-on-demand-contact-reveal-v1`) | Never merged to `main` |
| Permanent customer-facing title contract | Open PR **#232** · `8c586ba23` | On a separate branch; local session had switched away from UI polish branch |
| Reset View badge, Group & Demand tab label, remove “X updated”, compact research status | **Uncommitted** working tree on `fix/gdi-remove-weekly-updated-summary` | Stashed / left behind when creating the title-contract branch |

**Served-asset confirmation:** local `:8080` now loads `group-demand-intelligence.css?v=gdi-regression-recovery-v1` (browser verify).

No wholesale rollback was performed. Recovery = merge approved PR tips + re-apply verified uncommitted polish.

## Exact commits / files restored

### Merged into recovery branch
1. `80be5b88d` — Recover GDI card contacts, titles, demand icons, and toolbar alignment (PR #230)  
2. `1fdd835cc` — Restore GDI on-demand Surfe Find Email/Phone and Opportunities UI polish (PR #231)  
3. `8c586ba23` — Permanent customer-facing opportunity title contract (PR #232)  

### Key files
- `public/css/group-demand-intelligence.css` — `--gdi-toolbar-control-height: 3rem` Market Scope alignment + Reset View badge CSS  
- `public/js/group-demand-intelligence/dealality-gdi-ui.js` — Market Scope UI, tab label, weekly-summary removal, Reset View badge helpers, research-compact status  
- `public/js/group-demand-intelligence/app.js` — Reset View uses `UI.resetViewButtonHtml`  
- `lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js` — canonical titles  
- `lib/group-demand-intelligence/opportunity-list-dto.js` — DTO finishes through title contract + research-commentary strip  
- `lib/group-demand-intelligence/customer-presentation-sanitize-v1.js` — sales-facing research copy (provider brand scrub)  
- `api/group-demand-intelligence.js` — title contract on internal-research projection path  

## Changes restored

### A. Opportunity name cleanup
Permanent title contract restored and wired through DTO / sanitize / API / upsert / hunters. Founder examples:

- `AMWA 2027 Interim Meeting — Sep 11–12` → `AMWA 2027 Interim Meeting`  
- `PDA TRI Bethesda Training — December 2026 Lodging` → `PDA TRI Bethesda Training`  
- `ASAE Annual Meeting & Exposition 2029 — Washington, DC (Early Watch)` → `ASAE Annual Meeting & Exposition 2029`  

### B. Simplified research text
Restored the **approved** `customer-presentation-sanitize-v1` + DTO `salesFacingSummaryField` path (from card-quality / mainline sanitizer) — provider brands scrubbed; commercial facts retained. **Did not invent a new summarization system.**

### C. Market Scope toggle height
Restored PR #230 toolbar tokens. Browser CDP: Market Scope **48px** vs adjacent selects **48px** (Δ0) on Bethesda and Wyndham.

## Already present and preserved

- Market Scope as the only competitive filtering control (Competitor chip / Competitive Status segmented control remain removed)  
- Competitive Verified / Needs Verification / Historical badge language from competitive-recovery lineage  
- Bethesda browse inventory **Showing 67 of 67** with Market Scope All/Hotel/Competitive counts  
- Opportunities-only guided tour V1.1 scripts still loaded (`gdi-guided-report-tour.js`)  
- Authorization, CSV, demand segments, classification, sales feedback paths untouched  

## Regression tests added

- `npm run test:gdi-regression-recovery-v1` — titles, research copy scrub, Market Scope CSS tokens, competitive control removal, UI polish locks  
- Existing locks re-run green:  
  - `test:gdi-customer-facing-opportunity-title-contract-v1`  
  - `test:gdi-opportunity-card-quality-v1`  
  - `test:gdi-market-scope-toggle-v1`  
- Browser: `npm run verify:gdi-regression-recovery-browser-v1`

## Browser verification

| Hotel | Result |
|-------|--------|
| Bethesda Marriott | PASS — recovery CSS served; titles clean; Market Scope Δ0; drawer captured; Showing 67 of 67 |
| Wyndham San Diego Bayside | PASS — recovery CSS served; Market Scope Δ0; drawer captured; Showing 47 of 47 |

Screenshots: `reports/gdi-regression-recovery-v1/screenshots/`

## Could not recover exactly / notes

1. **Bethesda Market Scope Hotel/Competitive exact 61/10** — live page showed All (67) with Hotel/Competitive counts from current inventory projection; competitive badge presentation from #230 is restored, but founder-cited 61/10 may reflect a prior CMI ledger snapshot. No force-edit of counts.  
2. **Uncommitted polish** had never been committed; recovery re-applied it surgically after merging PRs so Surfe/tour (#231) were not overwritten.  
3. Recovery remains **local** on `fix/gdi-regression-recovery-v1` until an explicit publish/merge authorization.

## Mechanism fix (so this does not repeat)

Local GDI work must not switch to a clean `main`-based branch for a single PR without either:
1. merging the other open approved GDI PRs first, or  
2. keeping UI polish committed on the active branch before branch-hopping.

Cache-bust query `?v=gdi-regression-recovery-v1` forces the browser to load the recovered CSS/JS.
