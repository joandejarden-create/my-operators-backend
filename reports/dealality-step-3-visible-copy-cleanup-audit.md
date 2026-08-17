# Dealality Step 3 — Visible copy & demo credibility cleanup audit

**Date:** 2026-05-20  
**Scope:** `public/**/*.{html,js}`, `public/app/**/*`, `api/dashboard-home.js` (frontend-visible labels/copy)  
**Prerequisite audits:** Frontend route/link audit; 404 / broken-route remediation plan (Step 2)  
**Status:** Read-only — no code changes in this pass  

**Search terms used:** Airtable, AirTable, Record ID, User Record, User City State, Dashdark, Webflow Template, Layout -, Test Mode, mock data, Mock Data Display, State/Providence, archieve, `href="#"`, `javascript:void(0)`, TODO, placeholder, coming soon, lorem, sample, demo only  

**Not found in frontend (repo):** `archieve`, `User Record`, `User City State`, `javascript:void(0)`, `lorem`, `Test Mode` (visible UI), `State/Providence` (typo; correct copy uses *State/Province*). Track `/archieve/company-settings` as **Webflow/external** only.

---

## Executive summary

| Priority | Count | Theme |
|----------|------:|-------|
| **P0** | 2 | Always-on “Mock Data Display” on Command Center; hardcoded personal name in API home payload |
| **P1** | 18 | Airtable/backend strings in product UI; demo banners; fictional deal names; stub CTAs; signup/dev leaks |
| **P2** | 12 | `href="#"` stubs, coming soon, Record ID toast, template asset URLs, admin-only labels |
| **P3** | 8+ | Archive pages, legacy sidebars, internal comments, Partner Directory field filtering |

**Sprint 0B** should treat **visible copy** and **route fixes (Step 2)** together for anything on `/app` home, Brand Explorer, My Deals, Market Alerts, LOI Hub, and Brand Development Dashboard.

---

## A. P0 / P1 cleanup table only

| ID | Priority | Issue (short) | Visible to demo user? | Action | Suggested replacement | Risk |
|----|----------|---------------|------------------------|--------|----------------------|------|
| **C-001** | **P0** | Command Center always shows “Mock Data Display” banner | Yes — `/app#/home` | Hide for demo **or** show only when API/mock fallback active | “Data refreshes every few minutes.” / remove banner when live | Low |
| **C-002** | **P0** | Home API returns `userName: 'Joan'` | Yes — header area when wired | Replace copy | Use authenticated display name or “Welcome back” | Low |
| **C-003** | **P1** | `api/dashboard-home.js` fictional deals (Guadalajara, Punta Cana, Hilton, etc.) | Yes — signals, actions, activity when API used | Replace copy **or** hide mock fallback | Real deal names from API only; empty state if none | Medium |
| **C-004** | **P1** | Market Alerts: “Showing demo data. Connect Airtable…” | Yes — `/market-alerts` | Hide for demo **or** replace | “No alerts in your markets yet.” | Low |
| **C-005** | **P1** | LOI Market Hub: “Mock Data Display” + “Search sample deals…” | Yes — `/loi-database-dashboard` (shell nav) | Hide banner for demo **or** replace | “LOI deal volume by region.” | Low |
| **C-006** | **P1** | Brand Explorer (live brands): hero meta “Live Airtable / Brand Setup data” | Yes — `/brand-explorer-combined` | Replace copy | “Brand profile” / “Updated from brand library” | Low |
| **C-007** | **P1** | Brand Explorer demo brands: “Mock Data Display” banner | Yes — demo brand names (Atelier North, Voco, etc.) | Keep for internal demo brands only; hide for production brands | “Sample brand profile for preview.” | Low |
| **C-008** | **P1** | Brand Explorer empty state: “Add rows in Airtable with slot `materials.caseStudy`” | Yes — empty case studies section | Replace copy | “No case studies published for this brand yet.” | Low |
| **C-009** | **P1** | The Radar loading: “Loading hotel data from Airtable…” | Yes — `/opportunity-radar` | Replace copy | “Loading market data…” | Low |
| **C-010** | **P1** | Deal readiness: “Saving to Airtable is not configured yet.” | Yes — deal setup / new deal setup AI review | Replace copy | “Review saved. Sync to your workspace is pending.” | Low |
| **C-011** | **P1** | My Deals error: `npm run add-brand-deal-requests-fields` | Yes — on save failure | Replace copy | “Contact support if this continues.” | Low |
| **C-012** | **P1** | Brand Development: modal hint references “in Airtable” | Yes — brand response modal | Replace copy | “Team-only notes (internal).” | Low |
| **C-013** | **P1** | Brand Development: owner-voice error mentions “Strategic Intent record linked in Airtable” | Yes — owner workspace empty state | Replace copy | “Owner deal setup isn’t linked yet. Refresh or ask the owner to complete setup.” | Low |
| **C-014** | **P1** | Outreach Inbox: “Saves to the Messages table in Airtable” | Yes — compose helper text | Replace copy | “Logged messages appear in your outreach history.” | Low |
| **C-015** | **P1** | Deal Room (brand): placeholder “recXXXX”, filter “matches Airtable Brand Deal Requests” | Yes — admin/dev deal room setup | Replace copy | “Deal ID” / “Exact brand name” | Low |
| **C-016** | **P1** | Partner Directory: “Sample figures for demo — not live pipeline totals.” | Yes — individual cards when stats mocked | Hide for demo **or** replace | Remove note when live stats; else hide stat row | Low |
| **C-017** | **P1** | Signup pilot success: “Memberstack… Airtable Unique ID updated.” | Yes — `/signup-pilot` | Replace copy | “Account created. Check your email to verify.” | Low |
| **C-018** | **P1** | My Operators error: “check Airtable configuration” | Yes — operators list error state | Replace copy | “We couldn’t load operators. Try again or contact support.” | Low |
| **C-019** | **P1** | Command Center: “Invite Partner” + market intel `href="#"` | Yes — header + intel ticker (mock API) | Hide for demo **or** link to `/partner-directory` | Wire CTAs or remove | Low |
| **C-020** | **P1** | Home: “Open responsiveness →” `href="#"` | Yes — `/app#/home` | Hide for demo **or** link | `/outreach/inbox` or remove footer link | Low |

---

## B. Full findings table

| ID | Term | File | Snippet / location | Where in product | Demo visible? | Classification | Action | Suggested replacement | Priority | Risk |
|----|------|------|-------------------|------------------|---------------|----------------|--------|----------------------|----------|------|
| C-001 | Mock Data Display | `public/app/home.html` | `Mock Data Display: This dashboard shows sample data…` | Command Center header | **Yes** | Debug/test artifact | Hide for demo | Remove or gate on `?demo=1` | P0 | Low |
| C-002 | sample / mock | `api/dashboard-home.js` | `userName: 'Joan'` | Home dashboard API | **Yes** | Visible technical text | Replace copy | Auth user name | P0 | Low |
| C-003 | mock data / sample | `api/dashboard-home.js` | `Guadalajara`, `Punta Cana`, `Hilton Guadalajara`, `Cancun Boutique` | KPIs, pipeline, signals, activity (API path) | **Yes** | Debug/test artifact | Replace copy / wire real data | Empty or API-driven labels | P1 | Med |
| C-004 | mock data | `api/dashboard-home.js` | `Mock data for now; wire to Airtable/DB later` | Comment only | No | Acceptable internal | Keep | — | P3 | — |
| C-005 | Mock Data Display | `public/market-alerts.html` | `Showing demo data. Connect Airtable for real alerts.` | Market Alerts banner | **Yes** | Debug/test artifact | Hide for demo | Neutral empty-state | P1 | Low |
| C-006 | Airtable | `public/market-alerts-back.html` | `Connect Airtable (see AIRTABLE_MARKET_ALERTS_SCHEMA.md)` | Back variant page | If linked | Debug/test artifact | Defer | — | P3 | Low |
| C-007 | Mock Data Display | `public/loi-database-dashboard.html` | `Mock Data Display… Real LOI deal data will be integrated…` | LOI Market Hub header | **Yes** | Debug/test artifact | Hide for demo | Product copy only | P1 | Low |
| C-008 | sample | `public/loi-database-dashboard.html` | `placeholder="Search sample deals..."` | LOI search field | **Yes** | Debug/test artifact | Replace copy | `Search deals…` | P1 | Low |
| C-009 | mock data | `public/loi-database-dashboard.html` | `uses mock data (DEAL_POOL) only` | JS comment | No | Acceptable internal | Keep | — | P3 | — |
| C-010 | Airtable | `public/js/brand-explorer-gold-detail.js` | `'Live Airtable / Brand Setup data'` | Brand Explorer hero meta (non-demo brands) | **Yes** | Visible technical text | Replace copy | “Brand library” | P1 | Low |
| C-011 | Mock Data Display | `public/js/brand-explorer-gold-detail.js` | `Mock Data Display: This dashboard shows sample data…` | Brand Explorer hero (demo brands) | **Yes** (demo brands) | Debug/test artifact | Hide for demo on prod brands | Sample profile note | P1 | Low |
| C-012 | Airtable | `public/js/brand-explorer-atelier-from-api.js` | `Add rows in Airtable with slot materials.caseStudy` | Case studies empty state | **Yes** | Visible technical text | Replace copy | User-facing empty state | P1 | Low |
| C-013 | Airtable | `public/deal-capture-radar-with-ranked-list.html` | `Loading hotel data from Airtable...` | The Radar loading state | **Yes** | Visible technical text | Replace copy | “Loading market data…” | P1 | Low |
| C-014 | Airtable | `public/deal-capture-radar-clean.html`, `standalone` | Same loading string | Radar variants | If used | Visible technical text | Replace copy | Same as C-013 | P2 | Low |
| C-015 | Airtable | `public/my-deals.html`, `new-deal-setup.html` | `Saving to Airtable is not configured yet` | Deal readiness AI status | **Yes** | Visible technical text | Replace copy | Neutral status | P1 | Low |
| C-016 | Airtable | `public/my-deals.html` | `npm run add-brand-deal-requests-fields` | Error toast on notes save | **Yes** (on error) | Visible technical text | Replace copy | Support message | P1 | Low |
| C-017 | Airtable | `public/brand-development-dashboard.html` | `Next Follow-up Notes (Internal) in Airtable` | Deal response modal hint | **Yes** | Visible technical text | Replace copy | “Internal team notes” | P1 | Low |
| C-018 | Airtable | `public/brand-development-dashboard.js` | `linked in Airtable` (owner-voice error) | Brand workspace owner panel | **Yes** (edge) | Visible technical text | Replace copy | Plain-language linkage | P1 | Low |
| C-019 | Airtable | `public/outreach-inbox.html` | `Saves to the Messages table in Airtable` | Inbox compose hint | **Yes** | Visible technical text | Replace copy | Product language | P1 | Low |
| C-020 | Airtable | `public/outreach-plan-wizard.html` | `No deals in Airtable`, `Check Airtable (Deals table…)` | Plan wizard errors | **Yes** (errors) | Visible technical text | Replace copy | “No deals found” | P2 | Low |
| C-021 | recXXXX | `public/deal-room-brand.html` | `placeholder="recXXXX — only if someone sent you this ID"` | Deal Room brand dev filter | **Yes** (brand room) | Visible technical text | Replace copy | “Record ID (optional)” | P1 | Low |
| C-022 | Airtable | `public/deal-room-brand.html` | `matches Airtable Brand Deal Requests` | Brand context input label | **Yes** | Visible technical text | Replace copy | “Exact brand name” | P1 | Low |
| C-023 | Airtable | `public/deal-room-brand.html` | `confirm in Airtable that Deal Room Access = Granted` | Empty state copy | **Yes** (empty) | Visible technical text | Replace copy | “Ask the owner to grant Deal Room access.” | P1 | Med |
| C-024 | demo | `public/partner-directory.js` | `Sample figures for demo — not live pipeline totals.` | Individual card/modal stats | **Yes** (when shown) | Debug/test artifact | Hide for demo | Remove when live | P1 | Low |
| C-025 | Airtable | `public/my-third-party-operators-new.html` | `check Airtable configuration` | Operators list error | **Yes** (error) | Visible technical text | Replace copy | Generic error | P1 | Low |
| C-026 | Airtable | `public/signup-pilot.html` | `Airtable Unique ID updated` | Signup success message | **Yes** on pilot | Visible technical text | Replace copy | Verification email sent | P1 | Low |
| C-027 | Dashdark / Webflow Template | `public/signup.html`, `signup-pilot.html`, `signup-temp.html` | CDN SVG: `…dashdark-webflow-template.svg` | Signup hero background | URL only (not readable text) | Template artifact | Defer **or** replace asset | Dealality-branded art | P2 | Low |
| C-028 | Record ID | `public/company-settings.html` | `Record ID: —` toast footer | Company Settings save toast | **Yes** (on save) | Visible technical text | Hide for demo | Remove line for external users | P2 | Low |
| C-029 | Record ID | `public/partner-directory.js` | `'Record ID'` in `excludedFieldNames` | Filter only — not shown | No | Acceptable internal | Keep | — | P3 | — |
| C-030 | coming soon | `public/app.js` | `Coming soon` srcdoc for `/support`, `/route-map` | Shell placeholder routes | **Yes** if nav clicked | Placeholder/stub | Hide for demo **or** replace | “Help center launching soon.” | P2 | Low |
| C-031 | coming soon | `public/js/brand-explorer-gold-detail.js` | `Request Introduction` disabled, `title="Coming soon"` | Brand Explorer CTA | **Yes** | Placeholder/stub | Keep (disabled OK) | — | P2 | Low |
| C-032 | coming soon | `public/outreach-analytics.html` | `More dashboards coming soon.` | Outreach Analytics | **Yes** | Placeholder/stub | Replace copy | “Additional metrics in development.” | P2 | Low |
| C-033 | coming soon | `public/partner-directory.html` | `Connect functionality coming soon!` | Partner Directory modal | **Yes** | Placeholder/stub | Hide for demo | Remove Connect until live | P2 | Low |
| C-034 | coming soon | `public/brand-review.html` | `Contact feature coming soon!` (alert) | Brand Review (legacy page) | If page used | Placeholder/stub | Defer | — | P3 | Low |
| C-035 | href="#" | `public/app/home.html` | `Open responsiveness →` | Command Center footer | **Yes** | Placeholder/stub | Hide for demo | Link or remove | P1 | Low |
| C-036 | href="#" | `api/dashboard-home.js` | `Invite Partner`, `ctaHref: '#'` (intel) | Home API CTAs | **Yes** | Placeholder/stub | Hide / wire | `/partner-directory` | P1 | Low |
| C-037 | href="#" | `public/app/dashboard-adapter.js` | Same as C-036 (mock adapter) | Mock home | **Yes** (fallback) | Placeholder/stub | Same as C-036 | Same | P1 | Low |
| C-038 | href="#" | `public/my-deals.html` | Sidebar: Market Intelligence, Deal Room, Outreach, Settings | My Deals local sidebar | **Yes** (standalone embed) | Placeholder/stub | Defer **or** remove sidebar in embed | Use shell nav only | P2 | Med |
| C-039 | href="#" | `public/brand-setup.html`, `brand-library*.html`, etc. | Legacy nav stubs (~50 anchors) | Embedded legacy pages | If not in shell | Legacy/archive only | Defer | — | P3 | Low |
| C-040 | href="#" | `public/index.html`, landing nav | `Sign In` → `#` | Marketing site | **Yes** (marketing) | Placeholder/stub | Wire Memberstack login | Real sign-in URL | P2 | Med |
| C-041 | TODO | `public/production-brand-dashboard.js` | `// TODO: Implement chat` | JS only | No | Acceptable internal | Keep | — | P3 | — |
| C-042 | TODO | `public/my-deals.html` | `/* TODO: dropdown menu */` | More actions menu | No (inert) | Acceptable internal | Keep | — | P3 | — |
| C-043 | recXXX | `public/brand-deal-request.html` | `?requestId=recXXX` | Error when missing id | **Yes** (error) | Visible technical text | Replace copy | “Missing deal request link.” | P2 | Low |
| C-044 | placeholder | `public/deal-room-owner.html` | `placeholder="recXXXX"` | Owner deal room | Dev/admin | Visible technical text | Replace copy | “Deal ID” | P2 | Low |
| C-045 | Mock Up | `public/app.js` NAV | `Brand Explorer (Mock Up)` | Admin-only nav item | Admin demo only | Debug/test artifact | Keep (admin) | — | P3 | Low |
| C-046 | mock | `public/operator-explorer-gold-mock.html` | Mock operator explorer page | `/operator-explorer-mockup` | Admin only | Debug/test artifact | Keep (admin) | — | P3 | Low |
| C-047 | Webflow | `public/app.html` | `Open Webflow Preview` | Dev workspace UI | Dev only (`devNav`) | Debug/test artifact | Keep (internal) | — | P3 | Low |
| C-048 | sample | `public/app/dashboard-adapter.js` | Header warning text (duplicate of home.html) | Via adapter if injected | If used | Debug/test artifact | Same as C-001 | Same | P1 | Low |
| C-049 | Airtable | `public/brand-library-brand.html` | `not live STR ADR/RevPAR/Occ` | Brand profile microcopy | **Yes** | Acceptable product copy | Keep | Clarifies data source | P3 | Low |
| C-050 | mock data | `public/brand-review.html` | `using mock data or if the Brand Fit Analyzer hasn't been run` | Brand Review empty | If page shown | Legacy/archive only | Defer | — | P3 | Low |
| C-051 | archieve | — | Not found in repo | Webflow external | N/A | Webflow-only | Track externally | Fix typo → `archive` or `/company-settings` | P3 | N/A |
| C-052 | hotel-owner | `public/dealality-webflow-nav.js` | `/hotel-owner/my-deals-new` | Webflow embed parent | Webflow only | Webflow-only | Track externally | Webflow QA doc | P3 | N/A |
| C-053 | G-XXXXXXXXXX | `public/index.html` | `gtag('config', 'G-XXXXXXXXXX')` | Marketing analytics | Source view only | Debug/test artifact | Replace before prod | Real GA ID or remove | P2 | Low |
| C-054 | mock | `public/archive/webflow-brand-dashboard.html` | Archived dashboard | Not in active nav | No | Legacy/archive only | Defer | — | P3 | Low |
| C-055 | Terms href="#" | `public/signup.html` | Terms & Privacy `href="#"` | Signup form | **Yes** | Placeholder/stub | Replace copy | `/terms.html`, `/privacy.html` | P2 | Low |

---

## C. Recommended Sprint 0B cleanup plan

Sprint **0B** = demo credibility pass (copy + routes). Run after or in parallel with **0A** (route aliases / broken links from Step 2).

### Phase 0B-1 — Command Center & home API (highest demo traffic)

| Task | IDs | Work |
|------|-----|------|
| Gate or remove mock banner | C-001, C-048 | Remove static warning from `home.html` or show only when `dashboard.js` detects mock/fallback |
| Personalization | C-002 | `dashboard-home.js`: `userName` from auth or omit |
| Fictional deal names | C-003 | Wire KPIs/activity to real API **or** generic labels (“Deal A”, “Resort project”) until live |
| Stub CTAs | C-035, C-036, C-037, C-019 | `Invite Partner` → hide; intel/signals `href="#"` → `/market-alerts` or hide rows |
| Route alignment | (Step 2) | `ROUTE_ALIASES` for outreach paths; New Deal → `/new-deal-setup` |

**Exit criteria:** `/app#/home` shows no “Mock Data Display” in a live-data demo; no “Joan”; no dead `#` CTAs in header/ticker.

### Phase 0B-2 — Primary owner workflows

| Task | IDs | Work |
|------|-----|------|
| Market Alerts | C-004 | User-facing banner → neutral |
| LOI Hub | C-007, C-008 | Remove mock banner / fix search placeholder |
| The Radar | C-013 | Loading string |
| My Deals | C-015, C-016, C-011 | Readiness + error strings; optional sidebar stub cleanup (C-038) |
| Brand Explorer | C-006, C-010, C-011, C-012 | Hero meta + empty states |
| Outreach | C-014, C-020 | Inbox hint |

**Exit criteria:** No “Airtable” in default happy-path UI on My Deals, Explorer, Alerts, Radar, LOI.

### Phase 0B-3 — Brand & operators

| Task | IDs | Work |
|------|-----|------|
| Brand Development | C-012, C-013, C-017, C-018 | Modal hints + owner-voice error |
| Deal Room | C-021, C-022, C-023 | Labels and empty states |
| Partner Directory | C-024, C-033 | Demo stats note + Connect coming soon |
| Operators | C-025 | Error copy |
| Signup pilot | C-026, C-027 | Success message + optional hero asset |

### Phase 0B-4 — Defer / external

| Task | IDs | Work |
|------|-----|------|
| Webflow | C-051, C-052 | Doc: `/archieve/…`, `/hotel-owner/…` QA on Webflow only |
| Archive / admin | C-039, C-045, C-046, C-047, C-054 | No change unless in demo script |
| Marketing | C-040, C-053, C-055 | Sign-in, GA, signup legal links |

---

## D. Files likely affected

| Area | Files |
|------|--------|
| Command Center | `public/app/home.html`, `public/app/dashboard.js`, `public/app/dashboard-adapter.js`, `api/dashboard-home.js` |
| App shell | `public/app.js` (placeholder routes, admin labels) |
| Brand Explorer | `public/js/brand-explorer-gold-detail.js`, `public/js/brand-explorer-atelier-from-api.js`, `public/brand-explorer-combined.html` |
| Deals | `public/my-deals.html`, `public/new-deal-setup.html`, `public/deal-setup.html` |
| Brand workspace | `public/brand-development-dashboard.html`, `public/brand-development-dashboard.js`, `public/deal-summary.html` |
| Outreach | `public/outreach-inbox.html`, `public/outreach-plan-wizard.html`, `public/outreach-analytics.html` |
| Intelligence | `public/market-alerts.html`, `public/loi-database-dashboard.html`, `public/deal-capture-radar-with-ranked-list.html` |
| Deal Room | `public/deal-room-brand.html`, `public/deal-room-owner.html` |
| Directory / operators | `public/partner-directory.js`, `public/partner-directory.html`, `public/my-third-party-operators-new.html` |
| Settings / signup | `public/company-settings.html`, `public/signup-pilot.html`, `public/signup.html` |
| Marketing (defer) | `public/index.html`, `public/landing.html` |

---

## E. Manual QA checklist

### Command Center (`/app#/home`)

- [ ] No yellow “Mock Data Display” banner on live demo path.
- [ ] No hardcoded personal name (“Joan”) in header.
- [ ] “New Deal” opens new-deal flow (after Step 2 route fix).
- [ ] Toolbox links open correct shell routes (Outreach, Inbox, Activity Log).
- [ ] “Invite Partner” hidden or goes to Partner Directory.
- [ ] “Open responsiveness” hidden or navigates somewhere real.
- [ ] Market intel ticker items do not use `href="#"` dead clicks.

### Owner core

- [ ] **My Deals:** no `recommended-fit-list` 404 (Step 2); readiness status has no “Airtable”; error toasts have no `npm run`.
- [ ] **Deal setup / New deal:** readiness message is user-friendly.
- [ ] **Market Alerts:** no “Connect Airtable” banner in production demo mode.
- [ ] **LOI Hub:** no “Mock Data Display” / “sample deals” in UI.
- [ ] **The Radar:** loading text does not mention Airtable.
- [ ] **Outreach Inbox:** helper text is product language only.

### Brand

- [ ] **Brand Explorer (live brand):** hero does not say “Live Airtable / Brand Setup data”.
- [ ] **Brand Explorer (demo brand):** mock banner acceptable for internal demo only — confirm demo script.
- [ ] **Brand Development:** modal hints and owner-voice errors have no Airtable instructions.
- [ ] **Deal summary** `from=bwp` back link works (Step 2).

### Partner / operators / signup

- [ ] **Partner Directory:** no “Sample figures for demo” on cards intended as live; Connect not a dead “coming soon” if shown in demo.
- [ ] **My Operators:** error state is generic.
- [ ] **Signup pilot:** success message has no Memberstack/Airtable jargon.

### Regression

- [ ] Admin-only items (“Mock Up”, Webflow Preview) still available under dev/admin workspace.
- [ ] Internal comments / `partner-directory.js` exclusions unchanged.
- [ ] No new 404s from copy-only edits.

### Webflow (external checklist)

- [ ] No links to `/archieve/company-settings`.
- [ ] `/hotel-owner/*` member pages load on Webflow staging/prod.
- [ ] Sign-in on marketing site goes to real auth (not `#`).

---

## Related artifacts

| Report | Path |
|--------|------|
| Step 1 route/link audit (JSON) | `reports/frontend-links-audit.json` |
| Step 2 remediation plan | (conversation / team doc — route aliases, broken targets) |
| This report | `reports/dealality-step-3-visible-copy-cleanup-audit.md` |

---

*Generated from repository search 2026-05-20. Re-run grep after copy changes to refresh IDs.*
