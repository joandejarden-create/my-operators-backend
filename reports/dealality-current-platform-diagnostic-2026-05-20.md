# Dealality Current Platform Diagnostic

**Date:** 2026-05-20  
**Branch audited:** `app-shell-left-nav` (latest embed polish: `eb54175`; not merged to `main` as of this audit)  
**Staging probed:** `https://my-operators-backend-staging.up.railway.app` (HTTP headers + static route 200 checks)  
**Constraints:** Read-only — no code, schema, auth, or Webflow publish changes in this pass  

**Prior audits used for comparison:** `reports/dealality-step-3-visible-copy-cleanup-audit.md`, `reports/dealality-step-4-data-loading-validation.md`, `reports/dealality-step-5-role-switcher-validation.md`, and the pre-demo stabilization work (routes, Command Center, loading copy, `frame-ancestors`).

---

## A. Executive summary

### Materially improved since the original “Improvement Opportunities” diagnostic

| Area | Status |
|------|--------|
| **App-shell routes & legacy redirects** | `public/app.js` `ROUTES` + `ROUTE_ALIASES` and `server.js` redirects cover most retired paths; audited hash routes resolve to existing HTML on staging (HTTP 200). |
| **Webflow iframe security** | Staging returns `Content-Security-Policy: frame-ancestors …` (Dealality + Webflow hosts) on `/app`, `/app/home.html?embed=1`, `/my-deals.html?embed=1` — no blocking `X-Frame-Options: SAMEORIGIN` on those paths. |
| **Command Center credibility** | “Preview as” + helper text; internal QA banner **hidden by default** (`hidden` + `isInternalQaMode()`); embed mode hides banner, role switcher, and preview tags; API no longer uses `userName: 'Joan'` or “Invite Partner”; signals/actions labeled “Sample …”. |
| **Loading copy (partial)** | **My Deals**, **Market Alerts**, **Deal Compare**, **Radar** (primary file) use neutral loading strings; **My Deals** no longer shows “10–30 seconds” in HTML. |
| **Market Alerts demo banner** | Active `market-alerts.html` no longer contains “Showing demo data / Connect Airtable” (legacy `market-alerts-back.html` still does). |

### Still demo-critical (fix or script around before external demos)

1. **Webflow `/hotel-owner/dashboard` embed URL** — If still `…/app#/home`, users see full shell chrome unless Webflow is updated to `…/app/home.html?embed=1&appShell=1` **and** Railway deploys branch with chromeless/embed CSS (`eb54175` is on `app-shell-left-nav` only, **not** `main`).  
2. **Command Center remains sample-first** — `contentMode: 'sample'` and placeholder KPI counts; fine as **vision preview** if narrated, risky if presented as live portfolio data.  
3. **“Preview as” vs shell nav** — Dashboard role toggle still does **not** change sidebar; production shell defaults to **owner** nav (`getBaseRole()` → `owner` without `/api/auth/me`).  
4. **LOI Market Hub** — In-page `DEAL_POOL` mock + “Search sample deals…” + dead `href="#"` chart links + misleading “Live database” chip.  
5. **Brand Development Dashboard** — Still shows **“Estimated time: 10–30 seconds”** on load.  
6. **Operator Explorer** — List is API-backed; **detail panel is static mock** (`operator-explorer-gold-mock.html`).  
7. **Visible “Airtable” / backend language** on several live paths (Brand Explorer meta, Outreach Inbox, My Deals errors, deal setup).  
8. **Profile Settings** — Required **SSN/EIN last-4 / Gov ID** field visible in demo flows.  

### Intentional preview / mockup (not bugs — manage narrative)

- Command Center sample metrics and illustrative signals (API explicitly labels “Sample scenario”).  
- LOI Hub as **benchmark layout** with synthetic deal pool.  
- Demo brand records in Brand Explorer (Mock Data Display banner).  
- Operator Explorer gold detail mock, admin-only “Brand Explorer (Mock Up)”, `/route-map` dev route.  
- Partner Directory **sample stats** footnote on some cards.  

### Webflow vs Railway

| Layer | Owns |
|-------|------|
| **Webflow** | Marketing pages, `/hotel-owner/*` member URLs, iframe `src`, Memberstack widgets, static nav that may still point at old embeds or `/hotel-owner/my-deals-new`. |
| **Railway** | `/app`, `/app#/*`, static `*.html`, APIs, CSP/`frame-ancestors`, embed pages `?embed=1`. |
| **Both** | End-to-end demo: Webflow must embed correct Railway URL; Railway must deploy the branch Webflow expects. |

### What not to overbuild yet

Full RBAC, per-role dashboards, LOI warehouse integration, Operator detail API, Platform Resources module, Message Center, and migrating every legacy standalone HTML sidebar — defer until traction; polish **credibility** and **routing** first.

---

## B. Top 10 current risks (demo credibility)

| Rank | Risk | Why it hurts demos | Owner |
|------|------|-------------------|--------|
| 1 | Webflow dashboard iframe uses shell URL without `embed=1` / wrong deploy branch | Full left nav + QA chrome (screenshot issue); fix split across Webflow publish + Railway branch | Webflow + Railway |
| 2 | Command Center presented as “live” while `contentMode: 'sample'` | Numbers and deals look real but are placeholders | Product + Railway |
| 3 | Preview-as vs owner nav mismatch | User switches to Brand on home, sidebar still owner — looks broken | Product decision |
| 4 | LOI Hub “Live database” + sample search on mock `DEAL_POOL` | Contradictory trust signals | Railway |
| 5 | Brand Development “10–30 seconds” loader | Trains expectation of a slow product | Railway |
| 6 | Operator Explorer click-through reveals mock detail | Directory looks production-grade; detail is not | Railway + demo script |
| 7 | “Live Airtable / Brand Setup data” on Brand Explorer | Backend jargon on a flagship module | Railway |
| 8 | Cold API failure on Radar / My Deals / Market Alerts | Empty map or blank table after loader | Railway ops |
| 9 | Sensitive fields on Profile Settings without compliance story | Trust concern for hotel owners | Product + legal |
| 10 | Dead `href="#"` on LOI charts / legacy library sidebars | Click does nothing — feels unfinished | Railway (defer legacy pages) |

---

## C. Confirmed issue table

| ID | Category | Issue | Current status | Evidence | User-visible impact | Classification | Priority | Recommended fix | Effort | Risk | Owner |
|----|----------|-------|----------------|----------|---------------------|----------------|----------|-----------------|--------|------|--------|
| D-001 | Webflow embed | Dashboard iframe may still target `/app#/home` | Partially fixed in repo; Webflow + deploy dependent | `home.html` comment; `app.js` chromeless + `embed-mode`; `eb54175` not on `main` | Full shell + preview chrome in Webflow | Webflow embed + publish/cache | **P0** | Webflow: `…/app/home.html?embed=1&appShell=1`; Railway: deploy `app-shell-left-nav` or cherry-pick `eb54175` | S | High | Webflow + Railway |
| D-002 | Dashboard | Sample KPIs/signals without live data path | Open | `api/dashboard-home.js` `contentMode: 'sample'`, mock counts | Looks like real pipeline | Intentional preview | **P1** | Narrate as preview; optional hide KPI strip for strict demos | M | Med | Product |
| D-003 | Role architecture | Preview-as does not change shell nav | Open | `reports/dealality-step-5-role-switcher-validation.md`; `getBaseRole()` | Confusing persona demo | Product / architecture | **P1** | Keep label; hide switcher in embed; long-term `/api/auth/me` nav | M | Med | Product |
| D-004 | LOI Hub | Mock `DEAL_POOL` + “Search sample deals…” | Open | `loi-database-dashboard.html` | Misleading if called “live LOI” | Intentional preview | **P1** | Demo script: “sample benchmarks”; replace placeholder/search; remove “Live database” or add banner | S–M | Med | Railway |
| D-005 | LOI Hub | Chart footer links `href="#"` | Open | `loi-database-dashboard.html` ~1216–1307 | Dead clicks | Confirmed repo issue | **P2** | Remove links or wire to tabs | S | Low | Railway |
| D-006 | Loading copy | Brand Development “10–30 seconds” | Open | `brand-development-dashboard.html:3605` | Slow-product signal | Confirmed repo issue | **P1** | Neutral “Loading brand deals…” | S | Low | Railway |
| D-007 | Loading copy | My Operators “5–15 seconds” | Open | `my-third-party-operators-new.html` | Same | Confirmed repo issue | **P2** | Neutral copy | S | Low | Railway |
| D-008 | Explorer | Operator detail = gold mock iframe | Open | `operator-explorer.js`, `operator-explorer-gold-mock.html` | Mock behind live list | Intentional preview | **P1** | “Preview profile” label; avoid click in demo | S | Med | Railway |
| D-009 | Copy | Brand Explorer “Live Airtable / Brand Setup data” | Open | `brand-explorer-gold-detail.js:966` | Technical jargon | Confirmed repo issue | **P1** | “Brand profile” / “From brand library” | S | Low | Railway |
| D-010 | Copy | Outreach Inbox Airtable helper | Open | `outreach-inbox.html:130` | Backend visible | Confirmed repo issue | **P2** | Product-language helper | S | Low | Railway |
| D-011 | Copy | My Deals save error mentions `npm run …` | Open | `my-deals.html:5032` | Technical on error | Confirmed repo issue | **P2** | Generic support message | S | Low | Railway |
| D-012 | Copy | New deal setup “Saving to Airtable is not configured” | Open | `new-deal-setup.html:5393` | Technical | Confirmed repo issue | **P2** | Neutral status | S | Low | Railway |
| D-013 | Copy | Deal Room brand `recXXXX` / Airtable labels | Open | `deal-room-brand.html` | Admin-ish UX | Internal / edge | **P2** | Plain labels | S | Low | Railway |
| D-014 | Partner Directory | “Sample figures for demo…” | Open | `partner-directory.js:2437` | Honest but visible | Intentional preview | **P2** | Hide footnote when stats live | S | Low | Railway |
| D-015 | Settings | Company Settings “Record ID” toast | Open | `company-settings.html` | Technical on save | Confirmed repo issue | **P2** | Hide for non-admin | S | Low | Railway |
| D-016 | Settings | Profile SSN/EIN/Gov ID required | Open | `profile-settings.html:259` | Trust-sensitive | Product decision | **P1** | Hide from demo route or add compliance copy | M | High | Product |
| D-017 | Routes | `/support`, `/route-map` shell placeholders | Open | `app.js` `placeholder: true` | “Coming soon” iframe | Intentional placeholder | **P2** | Hide Support from demo nav or custom copy | S | Low | Railway |
| D-018 | Routes | Reports nav admin-only | By design | `app.js` NAV | Hidden for owner demo | Intentional | **P3** | None for owner demos | — | — | Product |
| D-019 | CSP | Non-embed pages still `X-Frame-Options: SAMEORIGIN` | By design | `server.js:339-342` | Blocks accidental embed | Not a bug | **P3** | None | — | — | Railway |
| D-020 | Webflow paths | `/hotel-owner/*` not served on Railway | By design | `dealality-webflow-nav.js` | 404 if hit on Railway | Webflow-only | **P3** | Keep Webflow routing doc | — | — | Webflow |
| D-021 | Performance | Radar `limit=100000` payload | Open | Step 4 audit; `brand-presence-mapping.js` | Slow first paint | Enhancement | **P2** | Warm cache; bbox API later | L | Med | Railway |
| D-022 | Performance | My Deals lazy tab counts | Open | Step 4 audit | Tabs show 0 until opened | Enhancement | **P2** | Pre-open tabs in demo | S | Low | Demo script |
| D-023 | Archive | Legacy pages with `href="#"` sidebars | Open | `brand-library.html`, `clause-library-clause-enhanced.html` | Only if linked | Legacy/archive | **P3** | Do not link in shell | — | — | Railway |
| D-024 | Command Center | Resolved: Joan, Invite Partner, always-on mock banner | **Fixed** | `api/dashboard-home.js`, `home.html`, `dashboard.js` | — | Already fixed | — | — | — | — | Railway |
| D-025 | Loading | Resolved: My Deals 10–30s, Market Alerts demo banner | **Fixed** | `my-deals.html`, `market-alerts.html` | — | Already fixed | — | — | — | — | Railway |
| D-026 | Security | Resolved: Webflow `frame-ancestors` | **Fixed on staging** | `curl -sI` staging `/app`, `/app/home.html?embed=1` | iframe loads | Already fixed | — | — | — | — | Railway |
| D-027 | Routes | Resolved: broad alias + redirect set | **Mostly fixed** | `app.js`, `server.js` | Fewer 404s | Already fixed | — | Monitor new links | — | — | Railway |

---

## D. Live vs Preview vs Coming Soon matrix

| Module | Route(s) | Data source | Demo state | Recommended demo handling |
|--------|----------|-------------|------------|-------------------------|
| **Command Center** | `/app#/home`, `/app/home.html?embed=1` | API sample VM + optional live LOI chart from Airtable | **Preview** | Use embed URL; say “illustrative layout”; optional hide Preview-as |
| **My Deals** | `/app#/my-deals` | `GET /api/my-deals` (Airtable) | **Live** (env-dependent) | Warm staging; owner account; pre-open tabs |
| **Brand Explorer** | `/app#/brand-explorer-combined` | `GET /api/brand-library/*` | **Live** (+ demo brands) | Use non-demo brands; fix Airtable meta line |
| **Operator Explorer** | `/app#/operator-explorer` | List API; detail mock | **Hybrid** | List only in demo; explain mock detail |
| **The Radar** | `/app#/opportunity-radar` | `GET /api/brand-presence` | **Live** (env-dependent) | Warm cache; operator role → alternate HTML |
| **Market Alerts** | `/app#/market-alerts` | `GET /api/market-alerts` | **Live** (needs rows) | Seed 7d alerts or use “All” window |
| **LOI Market Hub** | `/app#/loi-database-dashboard` | In-page `DEAL_POOL` mock | **Preview** | “Sample LOI benchmarks”; do not claim live |
| **Deal Compare** | `/app#/deal-compare`, My Deals tab | `GET /api/deal-compare/proposals` | **Live** (deal-dependent) | Open from deal with submissions |
| **Brand Development** | `/app#/brand-development-dashboard` | Brand deals API | **Live** (brand role) | Fix 10–30s copy; brand demo user |
| **My Operators** | `/app#/my-operators` | `GET /api/third-party-operators` | **Live** | Soften 5–15s copy |
| **Partner Directory** | `/app#/partner-directory` | `GET /api/partner-directory` | **Live** + sample stats | Acknowledge sample stats if shown |
| **Outreach Plans** | `/app#/outreach` | Outreach APIs | **Live** (partial) | Smoke-test plan list |
| **Outreach Inbox** | `/app#/outreach/inbox` | Messages API | **Live** (partial) | Fix Airtable copy |
| **Activity Log** | `/app#/activity-log` | Activity API | **Live** (partial) | Smoke-test |
| **Deal Room (Owner/Brand)** | `/app#/deal-room-*` | Deal room APIs | **MVP / workflow** | Internal or scripted deal id |
| **Outreach Analytics** | `/app#/outreach/analytics` | Partial | **Coming soon** | Mention “metrics expanding” |
| **Reports** | `/app#/reports` | `reports-dashboard.html` | **Internal / admin** | Hide from external demo |
| **Support** | `/app#/support` | Placeholder iframe | **Coming soon** | Do not click in strict demo |
| **Route Map** | `/app#/route-map` | Dev placeholder | **Internal** | Dev only |
| **Financial/Clause libraries** | Toolbox routes | APIs + heavy UI | **Live** (niche) | Optional deep-dive |
| **Franchise Fee Estimator** | `/app#/franchise-fee-estimator` | Client calc | **Live tool** | Safe calculator demo |
| **Signup / pilot** | `/signup.html`, `/signup-pilot.html` | Memberstack + API | **Live** | Not main app demo path |
| **Webflow hotel-owner** | `/hotel-owner/dashboard` | iframe → Railway | **Integration** | See D-001 |

---

## E. Webflow vs Railway tracker

| ID | Issue | Layer | Action |
|----|-------|-------|--------|
| W-001 | `/hotel-owner/dashboard` iframe `src` | Webflow | Set to `https://<staging>/app/home.html?embed=1&appShell=1` (not bare `/app#/home` unless chromeless deploy confirmed) |
| W-002 | Memberstack JWT on Command Center iframe | Webflow + Railway | Ensure `dealality-webflow-embed-parent.js` loads; `eb54175` extends `msToken` to `/app/home.html` on Railway |
| W-003 | Links to `/hotel-owner/my-deals-new` | Webflow | Map to shell `/app#/my-deals` or embed `my-deals.html?embed=1` per `dealality-webflow-nav.js` |
| W-004 | Stale Webflow static dashboard copies | Webflow | Retire duplicate static Command Center if any; single iframe source of truth |
| W-005 | Publish/cache after iframe change | Webflow | Hard refresh / republish |
| R-001 | Deploy branch for demo | Railway | Confirm staging tracks `app-shell-left-nav` (has `eb54175`) or merge to `main` |
| R-002 | `frame-ancestors` | Railway | **Verified** on staging for `/app` and embed HTML |
| R-003 | Do not add `/hotel-owner/*` to Express | Railway | Correct — Webflow-only unless product mandates reverse proxy |

---

## F. Sprint action plan

### Sprint 0F — Final pre-demo polish (days)

- Webflow iframe URL + republish (W-001, W-005).  
- Railway deploy: merge/cherry-pick `eb54175` to staging branch (R-001).  
- Copy: Brand Development loader (D-006); LOI search placeholder + “Live database” (D-004); Brand Explorer meta (D-009).  
- Demo script: Command Center = preview; LOI = sample; Operator = no detail click.  
- Optional: hide Preview-as via embed CSS (already in `dashboard.css` embed-mode).  
- Profile: hide or document SSN field for external demos (D-016).

### Sprint 1 — MVP functional gaps

- LOI Hub: optional read-only API or honest “Preview” chrome.  
- Operator Explorer: real detail endpoint or persistent preview label.  
- Wire dead LOI chart links (D-005).  
- Outreach/Deal Room: neutral copy pass (D-010–D-013).  
- Shell: `/api/auth/me` → `getBaseRole()` for correct default nav (role architecture).

### Sprint 2 — Performance / data architecture

- Radar bbox/limit pagination (D-021).  
- My Deals prefetch tab counts (D-022).  
- Brand list pagination/virtualization when >200 brands.

### Sprint 3 — Onboarding / activation

- Signup copy cleanup (pilot Airtable messages).  
- Profile/company settings trust copy and field gating.  
- Terms/Privacy links on signup.

### Sprint 4 — Design system polish

- CTA/badge consistency on demo surfaces only (not full DS).  
- Remove Dashdark asset URLs on signup when branded art ready.

### Sprint 5 — Role-based architecture

- Permission-based nav from resolved user.  
- Per-role Command Center metrics from real APIs.  
- Remove or internalize Dev Workspace vs Preview-as duplication.

---

## G. Manual QA checklist

**Staging base:** `https://my-operators-backend-staging.up.railway.app`  
**Webflow:** `https://www.dealality.com` or `mvp-deal-capture.webflow.io` — `/hotel-owner/dashboard`

### Headers / embed

- [ ] `curl -sI …/app` → `frame-ancestors` includes dealality.com and `*.webflow.io`; no `X-Frame-Options: SAMEORIGIN`.  
- [ ] `curl -sI …/app/home.html?embed=1` → same.  
- [ ] Webflow page: iframe shows **content only** (no left nav, no internal QA banner) after W-001 + deploy.

### App shell hash routes (logged-in demo user)

- [ ] `…/app#/home` — Command Center loads; preview tags hidden in embed; CTAs go to real routes.  
- [ ] `…/app#/my-deals` — table populates; no “10–30 seconds” on main loader.  
- [ ] `…/app#/outreach` — Outreach Plans loads (200: `outreach-plans.html`).  
- [ ] `…/app#/outreach/inbox` — Inbox loads.  
- [ ] `…/app#/activity-log` — Activity log loads.  
- [ ] `…/app#/market-alerts` — feed or friendly empty; no demo Airtable banner.  
- [ ] `…/app#/loi-database-dashboard` — mock data table; note sample positioning.  
- [ ] `…/app#/brand-development-dashboard` — loads; note 10–30s copy flag.  
- [ ] `…/app#/brand-explorer` — list + detail; check hero meta line.  
- [ ] `…/app#/my-operators` — list loads.  
- [ ] `…/app#/opportunity-radar` — map markers after load.  
- [ ] `…/app#/support` — “Coming soon” placeholder only if clicked.

### Role / preview

- [ ] Toggle Preview as Brand → KPI labels change; **sidebar unchanged** (documented).  
- [ ] `?showSampleBanner=1` on home (localhost QA) → internal banner appears.

### API smoke (authenticated)

- [ ] `GET /api/dashboard/home?role=owner` → `contentMode: "sample"`.  
- [ ] `GET /api/my-deals?view=initial` → 200 + deals.  
- [ ] `GET /api/brand-library/brands` → 200.  
- [ ] `GET /api/brand-presence?limit=100000` → success + hotels (or document failure).

---

## H. What not to do yet

- Build full **Message Center**, **Platform Resources**, or **Notifications** modules in Railway.  
- Implement complete **role-based permissions** across all APIs.  
- Replace every **mock/preview** module with live data in one sprint.  
- Add Express routes for all **Webflow `/hotel-owner/*`** paths without product sign-off.  
- Large **Airtable schema** changes for demo polish.  
- **Design system** rewrite across 50+ legacy HTML files.  
- Migrate **Operator Explorer** mock to full CRM without prioritization.  

---

## 1. Route integrity / 404s

### App-shell hash routes (audited)

All primary `ROUTES` in `public/app.js` map to files present under `public/`. Staging returned **HTTP 200** for: `app/home.html`, `outreach-plans.html`, `outreach-inbox.html`, `outreach-deal-activity-log.html`, `market-alerts.html`, `loi-database-dashboard.html`, `brand-development-dashboard.html`, `brand-explorer-combined.html`, `operator-explorer.html`, `my-third-party-operators-new.html`.

| Route | File | Status |
|-------|------|--------|
| `/home` | `/app/home.html` | OK |
| `/my-deals` | `/my-deals.html` | OK |
| `/outreach` | `/outreach-plans.html` | OK |
| `/outreach/inbox` | `/outreach-inbox.html` | OK |
| `/activity-log` | `/outreach-deal-activity-log.html` | OK |
| `/market-alerts` | `/market-alerts.html` | OK |
| `/loi-database-dashboard` | `/loi-database-dashboard.html` | OK |
| `/brand-development-dashboard` | `/brand-development-dashboard.html` | OK |
| `/brand-explorer` | `/brand-explorer-combined.html` | OK |
| `/my-operators` | `/my-third-party-operators-new.html` | OK |
| `/opportunity-radar` | `/deal-capture-radar-with-ranked-list.html` (owner); operator may swap file in `routeToEmbedUrl` | OK |
| `/support`, `/route-map` | Placeholder iframe (`Coming soon`) | Intentional |

### Express / static

- `server.js`: redirects for `deal-setup`, `new-deal-setup`, `brand-workspace-pipeline`, `recommended-fit-list`, `deal-compare-select-winner`, `brand-library` → `brand-explorer-combined`, `/app` → `app.html`.  
- Retired `management-operator-radar` **removed** from active shell (comment in `app.js`).

### Webflow-only

- `/hotel-owner/dashboard`, `/hotel-owner/my-deals-new`, etc. — **not** on Railway; must stay on Webflow or iframe to Railway.

### Legacy / archive

- Standalone pages (`brand-library.html`, old radar variants) retain `href="#"` nav — **not** in shell nav; classify as archive unless re-linked.

---

## 2. Command Center / Dashboard credibility

| Check | Result |
|-------|--------|
| Mock Data Display banner | **Hidden** by default; shown only `isInternalQaMode()` (localhost, `devNav`, `showSampleBanner`, `dc_internal_qa`) |
| Preview as label | **Yes** — “Preview as:” + “Sample layout only…” |
| Hardcoded Joan | **Removed** from API |
| Fictional city names in API | **Replaced** with “Sample deal / Sample scenario” language |
| Dead CTAs | **Removed** Invite Partner; header CTAs use real paths (`/new-deal-setup`, `/outreach`, `/outreach/inbox`) |
| Responsiveness footer | **Wired** to `/outreach/inbox` via `toShellHref` |
| Role switcher | Changes **dashboard API labels/counts only** — not nav (see Step 5) |
| Embed presentation | `embed-mode` hides banner, switcher, preview tags when `embed=1` / iframe |
| Live vs sample communication | Header description mentions “Illustrative sample metrics”; API `contentMode: 'sample'` |

**Verdict:** Credible as a **vision preview** when narrated; **not** credible as live owner data without backend wiring.

---

## 3. Visible technical / template language

| Term / pattern | Still present? | Where | Demo visible? | Action |
|----------------|----------------|-------|---------------|--------|
| Mock Data Display | Demo brands only | `brand-explorer-gold-detail.js`, static education HTML | Demo brands | Intentional preview |
| Live Airtable / Brand Setup | Yes | `brand-explorer-gold-detail.js` | Yes | Fix before demo (P1) |
| Airtable (errors/helpers) | Yes | `my-deals.html`, `new-deal-setup.html`, `outreach-inbox.html`, deal room | On error/path | P2 replace |
| Record ID | Yes | `company-settings.html` toast | On save | P2 hide |
| Dashdark template SVG | Yes | `signup*.html` CDN URL | Signup only | P2 defer |
| Search sample deals | Yes | LOI search placeholder | Yes | P1 |
| npm run … | Yes | `my-deals.html` error | On error | P2 |
| href="#" (LOI charts) | Yes | `loi-database-dashboard.html` | Yes | P2 |
| href="#" (legacy sidebars) | Yes | Archive HTML | If linked | P3 defer |

**Resolved vs Step 3:** Command Center banner (C-001), Joan (C-002), Market Alerts demo banner (C-005), fictional names softened in API (C-003 partial), Invite/`#` intel (C-019/036 partial).

---

## 4. Data loading and empty states

| Page | Source | Loading copy (current) | Demo broken risk | State |
|------|--------|----------------------|------------------|-------|
| My Deals | API | “Loading your deals…” (no 10–30s in HTML) | Medium if API/auth fail | **Live** |
| Brand Explorer | API | 1–2s / 2–6s estimates remain | Low warm | **Live** |
| Operator Explorer | API + mock detail | 2–6s on popup | High on detail click | **Hybrid** |
| Partner Directory | API | Neutral loader | Low | **Live** + sample stats |
| Market Alerts | API | “Loading market signals…” | Low if empty feed | **Live** |
| Radar | API | “Loading market signals…” (`brand-presence-mapping.js`) | High if API down | **Live** |
| LOI Hub | Mock pool | No long estimate | Low empty; **misleading “live”** | **Preview** |
| Deal Compare | API | “Loading comparison data…” | Medium without dealId | **Live** |
| Brand Development | API | **10–30 seconds** | Medium | **Live** + copy debt |
| Deal Room | API | Workflow-specific | Edge cases | **MVP** |
| Reports / Support | Placeholder / charts | N/A | N/A | **Internal / soon** |

---

## 5. Webflow vs Railway integration

**Staging (2026-05-20):**

```
/app, /app/home.html?embed=1, /my-deals.html?embed=1
→ HTTP 200
→ Content-Security-Policy: frame-ancestors 'self' https://www.dealality.com … https://*.webflow.io …
→ (no X-Frame-Options on these paths)
```

**Repo embed contract:**

- Shell child iframes: `routeToEmbedUrl` → `?embed=1&appShell=1`.  
- Recommended Webflow dashboard: `/app/home.html?embed=1&appShell=1`.  
- Chromeless parent shell when `app.html` is iframed: `app-chromeless-embed` (`eb54175`, **not on `main`**).

**Risks:** Webflow publish lag; wrong iframe URL; staging on `main` without `eb54175`.

---

## 6. Role architecture

| Question | Answer |
|----------|--------|
| Changes dashboard data? | **Yes** — `GET /api/dashboard/home?role=` |
| Changes route? | **No** |
| Changes nav? | **No** (production); **Dev Workspace** does (localhost/`devNav`) |
| Changes permissions? | **No** |
| Persists? | **Yes** — `localStorage` `dc_dashboard_role_view` |
| Clearly labeled preview? | **Improved** — “Preview as” + hint; hidden in embed mode |

**Recommendations:** Keep **Preview as** for vision demos; hide in strict Webflow embed; Sprint 5 for real RBAC + `/api/auth/me` nav.

---

## 7. Incomplete modules (summary)

See matrix §D. **Broken** = only when API/env fails or dead links clicked. **Preview** = LOI, Command Center sample, operator detail, demo brands.

---

## 8. Security / trust-sensitive fields

| Field | Location | Recommendation |
|-------|----------|----------------|
| US SSN last 4 / EIN / SIN / Gov ID | `profile-settings.html` | **Product decision:** keep for compliance flow later; **hide from external demo** or add plain-language security copy |
| Deal Room uploads / NDA | `deal-room-*.html` | OK for workflow demo with test deals |
| Signup PII | `signup.html` | Standard; fix Terms `href="#"` separately |

---

## 9. Visual / design consistency

- Command Center: consistent dark dashboard CSS; preview tags hidden in embed.  
- LOI: mixed signals (“Live database” on mock data).  
- Legacy library pages: old emoji sidebars — not in shell primary nav.  
- Signup: Dashdark Webflow template background image — defer rebrand.  
- **No full design system build recommended** for pre-demo.

---

## 10. Prior diagnostic comparison

| Original issue theme | Status |
|---------------------|--------|
| Broken routes / 404s | **Partially resolved** — shell + server aliases; Webflow paths separate |
| Mock/demo language | **Partially resolved** — home, market alerts, my deals; LOI/brand explorer/operator remain |
| Airtable visible to users | **Partially resolved** — radar loading fixed; explorer/inbox/errors remain |
| Loading 10–30 seconds | **Partially resolved** — my deals fixed; brand development + my operators remain |
| Incomplete empty modules | **Still valid** for edge APIs; many empties are OK with copy |
| Role-switcher confusion | **Partially resolved** — labeling + embed hide; nav mismatch **still valid** |
| Webflow vs Railway routing | **Partially resolved** — CSP fixed; iframe URL + deploy branch **still valid** |
| Dashboard credibility | **Partially resolved** — major home cleanup; still sample `contentMode` |
| Full left nav in Webflow embed | **Partially resolved in repo** (`eb54175`); **Webflow + deploy dependent** |

---

## References

- `public/app.js` — routes, nav, embed URLs, placeholders  
- `server.js` — redirects, `isEmbeddableShellRequest`, CSP  
- `public/app/home.html`, `dashboard.js`, `dashboard.css`, `api/dashboard-home.js`  
- `public/js/dealality-webflow-embed-parent.js`  
- `reports/dealality-step-3-visible-copy-cleanup-audit.md`  
- `reports/dealality-step-4-data-loading-validation.md`  
- `reports/dealality-step-5-role-switcher-validation.md`  

---

*End of diagnostic. No repository files were modified in this pass.*
