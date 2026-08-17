# Dealality — Consolidated Sprint 0B / 0C / 0D Action Plan

**Date:** 2026-05-20  
**Inputs:**  
- `reports/dealality-step-3-visible-copy-cleanup-audit.md`  
- `reports/dealality-step-4-data-loading-validation.md`  
- `reports/dealality-step-5-role-switcher-validation.md`  

**Status:** Planning only — **no code, schema, auth, or route changes in this document.**

**Out of scope for these sprints (explicit):** Airtable schema changes; Memberstack/auth permission changes; real role-based navigation; new modules; Railway routes for `/hotel-owner/*`; performance refactors (pagination, bbox Radar) unless listed as **defer**.

**Related (not 0B–0D):** Sprint **0A** — route aliases and broken links from Step 1/2 (`ROUTE_ALIASES`, `recommended-fit-list.html`, `brand-workspace-pipeline`, outreach paths). Run **0A before or parallel with 0B** for demo nav integrity.

---

## A. Executive summary

| Sprint | Theme | Item count (actionable) | Demo goal |
|--------|--------|-------------------------|-----------|
| **0B** | Visible credibility cleanup | 22 | No mock banners, Airtable jargon, fictional names, or dead `#` CTAs on primary demo paths |
| **0C** | Data / loading / empty-state cleanup | 14 | No scary time estimates; credible loaders/empties; env prep; honest LOI/Operator positioning |
| **0D** | Role switcher demo-safe handling | 5 | “Preview as” — never implies RBAC or nav change |

**Recommended sequencing:** **0A (routes)** → **0B Phase 1** (Command Center) → **0C Phase 1** (loading copy + env smoke) → **0D** → **0B Phase 2–3** (owner workflows) → **0C Phase 2** (positioning / prefetch) → deferred P2/P3.

**First implementation batch (see §B):** ~12 items, ~1–2 dev days — Command Center copy, role switcher labels, My Deals/Radar/Deal Compare loading strings, Radar Airtable loading line, stub CTAs on home API.

---

## B. Recommended first implementation batch

Implement in this order (copy-only where possible):

| Order | ID | Sprint | Summary |
|------:|-----|--------|---------|
| 1 | 0B-001 | 0B | Remove or gate Command Center “Mock Data Display” banner |
| 2 | 0B-002 | 0B | Replace hardcoded `userName: 'Joan'` in dashboard home API |
| 3 | 0D-001 | 0D | Relabel “View as” → “Preview as” + helper copy |
| 4 | 0B-019 | 0B | Fix home stub CTAs (`Invite Partner`, `href="#"`, responsiveness link) |
| 5 | 0C-001 | 0C | Remove/replace My Deals “10–30 seconds” loading estimate |
| 6 | 0C-002 | 0C | Replace Radar “Loading hotel data from Airtable…” (+ status seconds in `brand-presence-mapping.js`) |
| 7 | 0C-003 | 0C | Remove/replace Deal Compare “5–15 seconds” estimate |
| 8 | 0B-005 | 0B | LOI Hub mock banner + “Search sample deals…” placeholder |
| 9 | 0B-004 | 0B | Market Alerts demo banner copy (if ever shown) |
| 10 | 0C-004 | 0C | **Ops:** Verify Radar `/api/brand-presence` + My Deals `view=initial` in demo env (no code) |
| 11 | 0B-006 | 0B | Brand Explorer “Live Airtable / Brand Setup data” hero meta |
| 12 | 0B-003 | 0B | Genericize or hide fictional deal names in `dashboard-home.js` (can follow batch 1–2) |

**Exit for batch 1:** `/app#/home` and `/my-deals`, `/opportunity-radar`, `/loi-database-dashboard` pass a 10-minute smoke without mock banner, Joan, Airtable loading text, or 10–30s messaging.

---

## C. What not to touch

| Area | Reason |
|------|--------|
| **Airtable schema / field IDs** | Constraint; copy-only fixes |
| **`middleware/requireMyDealsAccess.js`**, **`resolve-user.js`** role gates | No permission changes in 0B–0D |
| **`public/app.js` `NAV_SECTIONS` role wiring from `/api/auth/me`** | Deferred to post-demo (Step 5 §F-E) |
| **`GET /api/brand-presence?limit=100000`** payload shape | Performance — defer (0C long-term) |
| **LOI `DEAL_POOL` → live API** | New integration — defer |
| **Operator gold mock → real API** | New module — defer (0C label only now) |
| **Railway `/hotel-owner/*` routes** | Webflow-only |
| **`public/archive/*`, legacy `brand-setup.html` nav stubs** | P3 unless linked from demo |
| **`partner-directory.js` `excludedFieldNames`** | Internal filter, not visible |
| **Admin-only:** Brand Explorer (Mock Up), Webflow Preview, `operator-explorer-mockup` | Keep for internal |

---

## D. Webflow-only cleanup tracker

| ID | Issue | Owner | QA |
|----|--------|-------|-----|
| WF-001 | `/archieve/company-settings` (typo) — not in repo | Webflow | No footer/nav links to `archieve` |
| WF-002 | `/hotel-owner/*` member pages (e.g. `my-deals-new`) | Webflow + embed | Staging/prod loads; MS token handoff |
| WF-003 | Marketing **Sign In** `href="#"` | Webflow `index` / landing | Real Memberstack login URL |
| WF-004 | Signup **Terms / Privacy** `href="#"` | Webflow signup | `/terms.html`, `/privacy.html` |
| WF-005 | Inject `window.__DEALALITY_APP_ROLE` from member type (optional) | Webflow + app shell doc | Only if product wants shell nav ≠ owner default |

---

## E. Rollback notes

| Change type | Rollback |
|-------------|----------|
| **HTML/CSS copy** | Revert file; no migration |
| **`api/dashboard-home.js` strings** | Revert commit; API contract unchanged |
| **`localStorage` keys** | `dc_dashboard_role_view` — safe to clear in browser; no server state |
| **Hide switcher / banner (CSS `hidden` or feature flag)** | Toggle query param e.g. `?demo=1` if used — document in deploy notes |
| **Env smoke (0C-004)** | N/A — verification only |

Keep changes in **small commits per sprint** (0B / 0C / 0D) for easy cherry-pick revert before external demo.

---

## F. Action items (full register)

### Sprint 0B — Visible credibility cleanup

| ID | Source | Issue | File(s) likely affected | Current user-visible problem | Recommended fix | Priority | Risk | Effort | Now / defer | Manual QA |
|----|--------|-------|-------------------------|------------------------------|-----------------|----------|------|--------|-------------|-----------|
| **0B-001** | Step 3 C-001, C-048 | Command Center mock banner always on | `public/app/home.html`, optionally `public/app/dashboard.js` | Yellow “Mock Data Display” on every `/app#/home` visit | Remove static banner **or** show only when API fallback/mock (`dashboard.js`) | P0 | Low | S | **Now** | Home: no mock banner on live demo path |
| **0B-002** | Step 3 C-002 | Hardcoded user name “Joan” | `api/dashboard-home.js` | Personalized header feels fake/wrong | `Welcome back` or name from auth/me when available; omit if unknown | P0 | Low | S | **Now** | Home header: no “Joan” |
| **0B-003** | Step 3 C-003 | Fictional deal names in home API | `api/dashboard-home.js` | Guadalajara, Punta Cana, Hilton in signals/activity | Generic labels (“Project A”) **or** empty sections until wired | P1 | Med | M | **Now** (batch 2) | Signals/activity: no obvious fiction |
| **0B-004** | Step 3 C-004 | Market Alerts demo banner | `public/market-alerts.html` | “Connect Airtable for real alerts” (hidden by default CSS) | Ensure stays hidden; if shown, neutral empty copy | P1 | Low | S | **Now** | Alerts: no Airtable banner |
| **0B-005** | Step 3 C-007, C-008; Step 4 S4-P1-4 | LOI Hub mock banner + sample search | `public/loi-database-dashboard.html` | Mock banner + “Search sample deals…” | Reposition: “Sample LOI benchmarks” **or** soften banner; placeholder `Search deals…` | P1 | Low | S | **Now** | LOI: honest sample positioning |
| **0B-006** | Step 3 C-006, C-010 | Brand Explorer Airtable hero meta | `public/js/brand-explorer-gold-detail.js` | “Live Airtable / Brand Setup data” on live brands | “Brand library” / “Updated from brand profile” | P1 | Low | S | **Now** | Open live brand: no Airtable in hero |
| **0B-007** | Step 3 C-007, C-011; Step 4 S4-P1-6 | Brand Explorer demo brand mock banner | `public/js/brand-explorer-gold-detail.js` | Mock banner on Atelier North, Voco, etc. | Keep for named demo brands only; “Sample profile” wording | P1 | Low | S | **Defer** (script) | Demo brand OK; prod brand clean |
| **0B-008** | Step 3 C-012 | Case studies empty Airtable instruction | `public/js/brand-explorer-atelier-from-api.js` | “Add rows in Airtable with slot…” | “No case studies published for this brand yet.” | P1 | Low | S | **Now** | Empty tab: user-facing only |
| **0B-009** | Step 3 C-015 | Deal readiness Airtable message | `public/my-deals.html`, `public/new-deal-setup.html` | “Saving to Airtable is not configured yet” | “Review saved. Workspace sync pending.” | P1 | Low | S | **Now** | AI readiness: no Airtable |
| **0B-010** | Step 3 C-016 | My Deals npm error toast | `public/my-deals.html` | `npm run add-brand-deal-requests-fields` on error | “Contact support if this continues.” | P1 | Low | S | **Now** | Trigger error path: no npm text |
| **0B-011** | Step 3 C-017 | Brand Development Airtable hint | `public/brand-development-dashboard.html` | Modal references Airtable field | “Internal team notes” | P1 | Low | S | **Now** (if in demo) | Modal hint clean |
| **0B-012** | Step 3 C-018 | Owner-voice Airtable error | `public/brand-development-dashboard.js` | “Strategic Intent record linked in Airtable” | Plain-language owner setup message | P1 | Low | S | **Defer** unless BDD in demo | Owner panel error readable |
| **0B-013** | Step 3 C-014 | Outreach Inbox Airtable hint | `public/outreach-inbox.html` | “Messages table in Airtable” | “Logged in outreach history” | P1 | Low | S | **Now** | Inbox compose helper clean |
| **0B-014** | Step 3 C-021–C-023 | Deal Room brand Airtable labels | `public/deal-room-brand.html` | `recXXXX`, “matches Airtable…”, grant copy | Product labels + “Ask owner to grant access” | P1 | Med | M | **Defer** unless deal room in demo | Brand deal room labels |
| **0B-015** | Step 3 C-024; Step 4 §7 | Partner Directory demo stats note | `public/partner-directory.js` | “Sample figures for demo…” on cards | Hide note or hide stat row when live | P1 | Low | S | **Defer** | Cards: no demo footnote if live |
| **0B-016** | Step 3 C-025 | My Operators Airtable error | `public/my-third-party-operators-new.html` | “check Airtable configuration” | Generic load error | P1 | Low | S | **Defer** | Error state generic |
| **0B-017** | Step 3 C-026 | Signup pilot success jargon | `public/signup-pilot.html` | Memberstack/Airtable Unique ID message | “Account created. Check email.” | P1 | Low | S | **Defer** (pilot only) | Pilot success clean |
| **0B-018** | Step 3 C-035, C-020 | Home responsiveness `href="#"` | `public/app/home.html`, `api/dashboard-home.js` | Dead “Open responsiveness” link | Link `/outreach/inbox` or hide | P1 | Low | S | **Now** | Click: navigates or hidden |
| **0B-019** | Step 3 C-036, C-037, C-019 | Home stub CTAs | `api/dashboard-home.js`, `public/app/dashboard-adapter.js` | Invite Partner + intel `href="#"` | Hide Invite **or** `/partner-directory`; wire intel to `/market-alerts` | P1 | Low | S | **Now** | No dead header/ticker clicks |
| **0B-020** | Step 3 C-013; Step 4 S4-P1-2 | Radar loading mentions Airtable (copy) | `public/deal-capture-radar-with-ranked-list.html` | “Loading hotel data from Airtable…” | “Loading market data…” — **also tracked as 0C-002** | P1 | Low | S | **Now** | Radar loader text |
| **0B-021** | Step 3 C-030 | Shell “Coming soon” routes | `public/app.js` | Support / route-map placeholder | Hide from nav **or** softer copy | P2 | Low | S | **Defer** | Click Support: acceptable |
| **0B-022** | Step 3 C-033 | Partner Connect coming soon | `public/partner-directory.html` | Alert “Connect functionality coming soon!” | Hide Connect until live | P2 | Low | S | **Defer** | No coming-soon alert in demo |

---

### Sprint 0C — Data / loading / empty-state cleanup

| ID | Source | Issue | File(s) likely affected | Current user-visible problem | Recommended fix | Priority | Risk | Effort | Now / defer | Manual QA |
|----|--------|-------|-------------------------|------------------------------|-----------------|----------|------|--------|-------------|-----------|
| **0C-001** | Step 4 S4-P1-1 | My Deals 10–30s estimate | `public/my-deals.html` (3 loaders) | “Estimated time: 10–30 seconds” trains slowness | “Loading deals…” only; keep wave/progress | P1 | Low | S | **Now** | My Deals: no 10–30s text |
| **0C-002** | Step 4 S4-P1-1, S4-P1-2; Step 3 C-013 | Radar Airtable + 3–5s status | `public/deal-capture-radar-with-ranked-list.html`, `public/brand-presence-mapping.js` | Airtable string + “3–5 seconds” status | Neutral “Loading census…”; remove/g soften second estimates | P1 | Low | S | **Now** | Radar: no Airtable, no 3–5s |
| **0C-003** | Step 4 S4-P1-1 | Deal Compare 5–15s estimate | `public/deal-compare.html`, `public/my-deals.html` (tab) | “Estimated time: 5–15 seconds” | “Loading proposals…” only | P1 | Low | S | **Now** | Compare: no 5–15s |
| **0C-004** | Step 4 S4-P0-1 | Radar API failure / empty map | `api/brand-presence.js`, env | Empty map looks broken | **Ops:** verify `AIRTABLE_BASE_ID_ALT` + key; warm cache before demo | P0 | High | S (ops) | **Now** (pre-demo) | Map shows markers |
| **0C-005** | Step 4 S4-P0-2 | My Deals blank after load | `api/my-deals.js`, auth | Empty table after spinner | **Ops:** Memberstack + owner user + `view=initial` 200 | P0 | High | S (ops) | **Now** (pre-demo) | Table populates |
| **0C-006** | Step 4 S4-P0-3 | Market Alerts empty feed | `api/market-alerts.js`, Airtable data | Empty feed without context | Seed alerts / use **All** time window in demo | P2 | Med | S (ops) | **Now** (pre-demo) | 7d feed has cards or clear empty |
| **0C-007** | Step 4 S4-P1-3 | Operator Explorer mock detail | `public/js/operator-explorer.js`, `operator-explorer-gold-mock.html` | Live list → mock popup on click | Banner “Preview profile (sample layout)” in popup; demo script: don’t click | P1 | Med | S | **Now** | Click operator: label visible |
| **0C-008** | Step 4 S4-P1-5 | My Deals lazy tab counts | `public/my-deals.html` | Matched/Compare tabs show 0 until opened | Prefetch after initial load **or** presenter opens tabs once | P1 | Low | M | **Defer** (script) | Tab badges update after visit |
| **0C-009** | Step 4 S4-P1-1 | Brand Explorer time estimates | `public/brand-explorer-combined.html` | 1–2s, 2–6s, “Waiting for live…” | Neutral loading only | P1 | Low | S | **Now** | Explorer: no second estimates |
| **0C-010** | Step 4 S4-P1-1 | Operator popup 2–6s estimate | `public/js/operator-explorer.js` | “Estimated time: 2-6 seconds” | “Loading operator profile…” | P1 | Low | S | **Now** | Popup: no 2–6s |
| **0C-011** | Step 4 §6 LOI | LOI mock data positioning | `public/loi-database-dashboard.html` | Looks like live LOI DB | Narrative + 0B-005 copy; don’t claim “live LOI” | P1 | Low | S | **Now** | Presenter script aligned |
| **0C-012** | Step 4 §1 My Deals | API failure empty state | `public/my-deals.html` | Blank table on error | Ensure error toast/banner (copy-only if missing) | P1 | Med | M | **Defer** | Simulate 500: message shown |
| **0C-013** | Step 4 §8 Deal Compare | Empty / no proposals | `public/deal-compare.html` | Empty without context | Keep existing copy; demo with deal that has submitted proposals | P2 | Low | — | **Defer** (script) | Open with `dealId` + proposals |
| **0C-014** | Step 4 §D long-term | Radar 100k payload / perf | `api/brand-presence.js`, `brand-presence-mapping.js` | Slow first paint | Bbox/pagination — **post-demo** | P3 | Med | L | **Defer** | N/A |

---

### Sprint 0D — Role switcher demo-safe handling

| ID | Source | Issue | File(s) likely affected | Current user-visible problem | Recommended fix | Priority | Risk | Effort | Now / defer | Manual QA |
|----|--------|-------|-------------------------|------------------------------|-----------------|----------|------|--------|-------------|-----------|
| **0D-001** | Step 5 §E | “View as” implies real role | `public/app/home.html`, `public/app/dashboard.css` | Users think RBAC changed | Label **“Preview as:”** + muted helper under toggle | P1 | Low | S | **Now** | Label + helper visible |
| **0D-002** | Step 5 §E | Button tooltips | `public/app/home.html` | Owner/Brand/Operator ambiguous | `title="Owner command center preview"` etc. | P2 | Low | S | **Now** | Hover tooltips |
| **0D-003** | Step 5 §D-A | Hide switcher option | `public/app/home.html` | External demo RBAC questions | `hidden` or `?hideRolePreview=1` for strict demos | P2 | Low | S | **Defer** | Optional investor build |
| **0D-004** | Step 5 §F | Document long-term nav/RBAC | `docs/` (new note) | Team rebuilds preview as permissions | Doc: E nav from auth/me, D role dashboards — **no code** | P3 | Low | S | **Defer** | Doc exists in backlog |
| **0D-005** | Step 5 §G | Demo script / presenter note | Runbook (this report §H) | Nav contradicts Brand preview | Script: use Dev Workspace locally **or** navigate to My Brand Deals | P1 | Low | — | **Now** (process) | Presenter rehearsed |

**0D — Do not implement now:** Wire shell nav to `dc_dashboard_role_view`; sync Dev Workspace with preview toggle; role-specific CTAs in `dashboard-home.js`.

---

## G. Sprint phases (implementation order)

### Sprint 0B phases

| Phase | IDs | Focus |
|-------|-----|--------|
| **0B-1** | 0B-001, 0B-002, 0B-003, 0B-018, 0B-019 | Command Center + home API |
| **0B-2** | 0B-004, 0B-005, 0B-020, 0B-006, 0B-008, 0B-009, 0B-010, 0B-013 | Owner intel + deals + explorer |
| **0B-3** | 0B-011, 0B-012, 0B-014, 0B-015, 0B-016, 0B-017, 0B-021, 0B-022 | Brand room, directory, deferrables |

### Sprint 0C phases

| Phase | IDs | Focus |
|-------|-----|--------|
| **0C-1** | 0C-001, 0C-002, 0C-003, 0C-009, 0C-010, 0C-004, 0C-005, 0C-006 | Copy + env smoke |
| **0C-2** | 0C-007, 0C-011, 0C-008, 0C-012, 0C-013 | Positioning + prefetch/script |

### Sprint 0D

| Phase | IDs |
|-------|-----|
| **0D-1** | 0D-001, 0D-002, 0D-005 |
| **0D-2 (optional)** | 0D-003 hide switcher |

---

## H. Final pre-demo QA checklist

### Environment (0C — before screen share)

- [ ] `GET /api/my-deals?view=initial` → 200, deals array (logged-in demo user)
- [ ] `GET /api/brand-presence?limit=100000` → `success: true`, hotels &gt; 0
- [ ] `GET /api/brand-library/brands` → non-empty
- [ ] `GET /api/market-alerts?timeWindow=7d&limit=100` → items or acceptable empty + copy
- [ ] Pre-warm: open **Home**, **My Deals**, **Radar**, **Brand Explorer** once

### Command Center (0B + 0D)

- [ ] No “Mock Data Display” (or gated intentionally)
- [ ] No “Joan”; no fictional city/deal names in ticker
- [ ] **Preview as:** label + helper; not “View as”
- [ ] No dead `#` on Invite / responsiveness / intel
- [ ] Switch Brand → KPI labels change; **sidebar unchanged** (prod) — explain in script
- [ ] New Deal / Outreach toolbox routes work (**0A**)

### Owner core (0B + 0C)

- [ ] **My Deals:** no 10–30s; table loads; no Airtable/npm in happy path
- [ ] **Radar:** map markers; no “Airtable” loader; no 3–5s status
- [ ] **Market Alerts:** no demo banner; feed or friendly empty
- [ ] **LOI Hub:** sample positioning clear; search placeholder OK
- [ ] **Deal Compare:** no 5–15s; meaningful empty if no proposals
- [ ] **Brand Explorer (live brand):** no Airtable hero meta
- [ ] **Outreach Inbox:** product-language helper

### Operator / partner (0C + 0B defer)

- [ ] **Operator Explorer:** avoid detail click **or** preview label visible
- [ ] **Partner Directory:** no “coming soon” Connect if shown

### Role / auth sanity (0D)

- [ ] Dashboard “Preview as Brand” ≠ brand nav in production
- [ ] Real brand user: My Deals 403 expected — don’t demo as full RBAC
- [ ] Localhost: Dev Workspace changes nav — don’t conflate with Preview as

### Webflow (§D)

- [ ] No `/archieve/` links; `/hotel-owner/` pages load
- [ ] Marketing sign-in works

### Regression

- [ ] No new 404s from copy-only edits
- [ ] Admin/dev tools still available under `devNav=1`

---

## I. Longer-term backlog (post-demo, not 0B–0D)

| Track | Source | Work |
|-------|--------|------|
| **0A** | Step 1/2 | `ROUTE_ALIASES`, broken `recommended-fit-list`, `brand-workspace-pipeline`, outreach paths |
| **Nav/RBAC** | Step 5 §F-E | `/api/auth/me` → shell `currentBaseRole`; remove preview toggle when real |
| **Role dashboards** | Step 5 §F-D | Real KPIs/pipeline/CTAs per role in `dashboard-home.js` |
| **Performance** | Step 4 §D | Radar bbox, My Deals prefetch, pagination |
| **LOI live API** | Step 4 | Replace `DEAL_POOL` |
| **Operator detail API** | Step 4 | Replace gold mock iframe |

---

## J. ID cross-reference

| Consolidated ID | Step 3 ID | Step 4 ID | Step 5 |
|-----------------|-----------|-----------|--------|
| 0B-001 | C-001, C-048 | — | — |
| 0B-002 | C-002 | — | — |
| 0B-003 | C-003 | — | — |
| 0B-020 / 0C-002 | C-013 | S4-P1-2 | — |
| 0C-001 | — | S4-P1-1 | — |
| 0C-004–006 | — | S4-P0-1–3 | — |
| 0D-001 | — | — | §E primary |

---

*Planning artifact only. Update item status in issue tracker when implementation starts.*
