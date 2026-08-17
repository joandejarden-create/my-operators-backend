# Dealality Step 5 — Dashboard Role Switcher Validation

**Audit date:** 2026-05-20  
**Scope:** Command Center “View as: Owner / Brand / Operator” switcher and its relationship to app-shell navigation, APIs, and real permissions.  
**Constraints:** Read-only audit — no file, API, or schema changes.

---

## A. Executive summary

The **Owner / Brand / Operator** control on the Command Center (`public/app/home.html`) is a **partially functional preview switcher**, not a platform role system.

| What it does | What it does **not** do |
|--------------|-------------------------|
| Refetches `GET /api/dashboard/home?role=…` | Change app-shell route or hash |
| Updates KPI **labels** and **mock counts** by role | Change sidebar navigation (production) |
| Updates pipeline **stage labels** by role | Change header CTAs, signals, next actions, or toolbox links |
| Persists choice in `localStorage` (`dc_dashboard_role_view`) | Change Memberstack session or `req.dealalityUser` |
| Re-renders dashboard widgets in the iframe | Enforce API permissions (e.g. My Deals still owner-gated) |

**Classification:** **Partially functional** — credible for “how the Command Center might look per persona,” but **easy to misread as full RBAC** during a demo.

**Separate control:** On **localhost** / `?devNav=1`, the app shell exposes **Dev Workspace** (`owner` / `brand` / `operator` / `admin` / `all`), which **does** change side nav, default home route, LOI tab visibility, and operator Radar embed. That switcher is **hidden in non-dev production** and uses a **different** storage key (`DEALALITY_DEV_WORKSPACE`). The Command Center switcher **does not sync** with Dev Workspace.

**Real role differentiation** elsewhere is driven by **Airtable Users** (`resolveDealalityUser` → `req.dealalityUser`) and **static nav `roles` arrays** in `public/app.js`, not by the dashboard toggle.

**Recommended short-term demo posture:** **B — Keep switcher but label it clearly as “Preview as”** (not “your role”), alongside the existing mock-data banner. For external demos where persona confusion is unacceptable, **A — hide the switcher** is also valid.

**Longer term:** **E** — permission-based navigation from `/api/auth/me` + **D** — role-specific dashboard data wired to real metrics, not mock placeholders.

---

## B. Implementation findings

### 1. Where the role switcher is implemented

| Layer | Location | Notes |
|-------|----------|--------|
| **UI** | `public/app/home.html` | `dc-role-switch` / `#dc-role-toggle` — three buttons `data-role="owner|brand|operator"`, label **“View as:”** |
| **Client logic** | `public/app/dashboard.js` | `getStoredRole` / `setStoredRole`, `updateRoleToggle`, `fetchAndRender(role)`, click handler on `#dc-role-toggle` |
| **Storage** | `localStorage` key `dc_dashboard_role_view` | Same key name documented in `api/dashboard-home.js` (`STORAGE_KEY`) but **only written client-side** |
| **API** | `api/dashboard-home.js` → `GET /api/dashboard/home?role=` | `buildHomeDashboardViewModel({ role })` |
| **Label config** | `lib/dashboardRoleConfig.js` | KPI and pipeline **labels** per role |
| **Shell route** | `public/app.js` `ROUTES['/home']` → `/app/home.html` | Command Center loads **inside iframe** only |

**Not the same as:** App shell **Dev Workspace** (`public/app.html` + `public/app.js` `initDevWorkspaceSwitcher`) — visible when `isDevMode` (localhost or `devNav=1`).

### 2. What changes when a role button is clicked

| Dimension | Changes? | Detail |
|-----------|----------|--------|
| **Route** | **No** | Stays on `/app#/home` (or `/app/home.html` in iframe). No hash or parent-shell navigation. |
| **Dashboard data** | **Partially** | API called with `?role=owner|brand|operator`. **KPI values** and **KPI/pipeline labels** vary by role. **Pipeline counts**, **signals**, **nextActions**, **recentActivity**, **header CTAs**, **toolboxLinks** are **identical** across roles in `buildHomeDashboardViewModel`. LOI volume chart and map overlay are **role-agnostic** (live Airtable when configured, else mock). |
| **Visible modules (dashboard)** | **Partially** | Same sections always render; only metric **labels/numbers** and pipeline **labels** shift. |
| **Side navigation** | **No** (production) | Shell `renderNav(currentRole)` uses `getEffectiveRole()` → `currentBaseRole` (defaults **`owner`**) or dev workspace on localhost. **Dashboard role is ignored.** |
| **CTA labels** | **No** | Header always: New Deal, Start Outreach, Messages, Invite Partner (same `href`s including legacy `/outreach-plans`, `/outreach-inbox`). |
| **Permissions** | **No** | No server check of `dc_dashboard_role_view`. APIs use **Memberstack JWT** + `requireDealalityUser` (`middleware/requireDealalityUser.js`). |
| **API calls (other pages)** | **No** | Only `/api/dashboard/home?role=` changes. Embedded pages (My Deals, Brand Explorer, etc.) unchanged. |
| **User session / state** | **Client-only** | `localStorage.setItem('dc_dashboard_role_view', role)`. Does not update Memberstack, JWT claims, or `window.__DEALALITY_APP_ROLE` (never set in repo). |

### 3. Persistence

| Scenario | Persists? |
|----------|-----------|
| Refresh Command Center / revisit Home | **Yes** — `dc_dashboard_role_view` restored in `init()` → `fetchAndRender(getStoredRole())` |
| Navigate away (e.g. My Deals) and back to Home | **Yes** — storage unchanged; home reloads with stored role |
| Full app refresh (`/app`) | **Yes** for dashboard iframe; **shell nav role unchanged** |
| Different browser / cleared storage | Resets to **owner** (default in `getStoredRole`) |
| Logout / login | **Not tied to auth** — storage may still show last preview role |

### 4. Real role differentiation elsewhere in the app

| Mechanism | Where | Owner / Brand / Operator impact |
|-----------|--------|--------------------------------|
| **Airtable user resolution** | `lib/dealality/resolve-user.js`, `middleware/requireDealalityUser.js` | Sets `isOwner`, `isBrand`, `isOperator`, `isAdmin` from Platform Role / User Type |
| **API gates** | e.g. `middleware/requireMyDealsAccess.js` | **My Deals: admin + owner only** — brand/operator get **403** |
| **Nav `roles` arrays** | `public/app.js` `NAV_SECTIONS` | Hides/shows items (My Deals vs My Brand Deals, Deal Rooms, Partner Directory, LOI Hub, Brand Explorer, etc.) |
| **Default home route** | `getDefaultRoute(role)` | **Brand** → `/brand-development-dashboard`; others → `/home` |
| **Radar embed** | `routeToEmbedUrl` | **Operator** → `operator-intelligence-radar-with-list.html` |
| **LOI Hub tabs** | `injectShellCss` in `app.js` | Database/benchmark tabs **admin + dev “all”** only |
| **Page-specific UX** | `brand-development-dashboard.html`, deal rooms, operator setup | Separate surfaces per persona (not driven by Command Center switcher) |

**Gap:** Shell does **not** call `GET /api/auth/me` to set `currentBaseRole` from the logged-in user. `getBaseRole()` uses `window.__DEALALITY_APP_ROLE` if set externally, else **`owner`** (and **always `owner` on localhost**).

### 5. Switcher classification

**Partially functional** (preview / prototype):

- Works for Command Center metric **presentation** swap.
- Does **not** implement product RBAC.
- Coexists with **mock** dashboard body (banner on home) and **placeholder** KPI/pipeline counts in API.
- **Confusing** when demo flow is: switch to **Brand** on home → sidebar still shows **owner** nav (production) → open **My Deals** → still owner API data or **403** for true brand users.

**Not cosmetic-only** (it does fetch and re-render), but **not production-role switching**.

### 6. Second switcher (internal / dev only)

| Control | Visibility | Storage | Effect |
|---------|------------|---------|--------|
| **Dev Workspace** select | `localhost` or `?devNav=1` | `DEALALITY_DEV_WORKSPACE` | Full nav filter, default route, LOI CSS, operator radar file |
| **View as** (Command Center) | Always on home page | `dc_dashboard_role_view` | Dashboard API + labels only |

These are **independent** — changing one does not update the other.

---

## C. Demo risk assessment

| Risk | Severity | Scenario |
|------|----------|----------|
| **Switcher implies RBAC is done** | **High** | Presenter selects Brand; audience assumes nav and deals are brand-scoped |
| **Nav contradicts dashboard** | **High** (production) | Brand KPI labels + owner sidebar (My Deals, Add New Deal) |
| **My Deals 403 for brand login** | **High** | Real brand user opens My Deals while dashboard showed “brand view” |
| **Mock metrics look live** | **Medium** | Role changes numbers (12 vs 18 vs 14) — looks like real data switched |
| **CTAs wrong for persona** | **Medium** | Brand preview still shows “New Deal” / owner outreach links |
| **Dev Workspace hidden in prod** | **Low** | Internal testers forget nav only changes on localhost |
| **Persisted preview role** | **Low** | Return visit opens home as Brand while user is owner account |

**Could mislead demo users?** **Yes** — unless narrated as **layout preview** only.

---

## D. Recommendation

**Primary (short-term): B — Keep switcher, label as preview**

- Rename label from **“View as:”** to **“Preview as:”** (or **“Demo view:”**).
- Add one-line helper text: *“Preview only — does not change your account or navigation.”*
- Keep existing **Mock Data Display** banner; mention preview in demo script.

**When to use A (hide for demo):** External investor/customer demo where **any** role toggle creates RBAC questions; single-owner narrative only.

**When to use C (admin/internal only):** If product team still needs persona screenshots — gate toggle behind `devNav=1` or admin role (requires implementation later).

**Do not rely on D or E before demo** — those are post-demo architecture (see section F).

**Do not use dashboard switcher to demonstrate permission boundaries** — use **Dev Workspace on localhost** for nav differentiation, or walk to **My Brand Deals** / **Brand Deal Room** pages explicitly.

---

## E. Suggested short-term copy (if switcher remains visible)

**Switcher group label (replace “View as:”):**

> **Preview as:**

**Optional helper (below toggle, muted):**

> Sample layout only. Your account, menu, and deal data do not change.

**Tooltip on each button (title attribute):**

- Owner — “Owner command center preview”
- Brand — “Brand command center preview”
- Operator — “Operator command center preview”

**Presenter one-liner (demo script):**

> “This toggle previews how the home dashboard labels and metrics could look for each participant type. Navigation and permissions follow your actual login.”

**If hiding (option A):** Remove or `hidden` the `dc-role-switch` block in `home.html` for demo builds only (implementation later).

---

## F. Longer-term product architecture recommendation

1. **E — Permission-based navigation (first)**  
   - On shell boot, `GET /api/auth/me` → set `currentBaseRole` from `dealalityUser.role` (not hardcoded `owner`).  
   - Remove duplicate “preview” switcher from customer-facing UI once nav matches auth.

2. **D — Separate role dashboards (data layer)**  
   - `buildHomeDashboardViewModel` should query role-specific datasets (owner deals, brand inbound, operator pipeline).  
   - Role-specific CTAs, signals, and toolbox links in API contract.  
   - Drop static mock `kpiValues` / shared `pipelineCounts`.

3. **Unify preview vs real (dev)**  
   - Single **“Impersonate / preview”** control for internal users that sets both nav **and** dashboard API role, with banner “Preview mode”.  
   - Never persist preview in `localStorage` for production users, or clear on logout.

4. **API alignment**  
   - Extend brand/operator access where product allows (e.g. brand dashboard APIs) with `requireBrandAccess` mirrors — already partially reflected in nav, not in My Deals.

5. **Wire `__DEALALITY_APP_ROLE` or remove**  
   - Either inject role from server/Webflow after login or delete dead hook in `getBaseRole()`.

---

## G. Manual QA checklist

### Command Center switcher (any environment)

- [ ] Open `/app#/home` — switcher shows Owner / Brand / Operator.
- [ ] Click **Brand** — KPI labels change (e.g. “Inbound Opportunities” vs “Active Opportunities”).
- [ ] KPI **numeric values** change (e.g. 12 → 18 on first KPI).
- [ ] Pipeline **stage labels** change (e.g. “Submitted” → “New Inbound”).
- [ ] Pipeline **counts** unchanged across roles (same numbers — document if presenting).
- [ ] Header CTAs **unchanged** (New Deal, Start Outreach, etc.).
- [ ] Signals / Next Actions / Recent Activity **unchanged** text.
- [ ] Refresh page — last selected role **still active** on home.
- [ ] Mock data banner **still visible**.

### Shell interaction (production / staging, not localhost)

- [ ] With dashboard set to **Brand**, sidebar still shows **owner-oriented** items (My Deals, Add New Deal) unless user is truly owner.
- [ ] Navigate to **My Deals** — data loads per **logged-in user**, not preview role.
- [ ] Log in as **brand** user (if available) — My Deals returns **403** or error; dashboard preview does not fix this.

### Dev Workspace (localhost or `?devNav=1`)

- [ ] Dev Workspace select **visible** in sidebar footer area.
- [ ] Select **Brand** — nav hides owner-only items; default route may go to brand development dashboard.
- [ ] Select **Operator** — open **The Radar** — loads operator intelligence HTML variant.
- [ ] Change Dev Workspace — Command Center **“Preview as”** does **not** auto-update (independent).

### Auth / API

- [ ] `GET /api/dashboard/home?role=brand` returns `"role":"brand"` and different KPI values.
- [ ] `GET /api/auth/me` with JWT returns real `dealalityUser.role` — compare to dashboard preview (expect mismatch).

### Demo script safeguards

- [ ] Presenter explains **preview vs account role**.
- [ ] Do not claim “switching roles changes what I can access” without using Dev Workspace or a brand-specific page.
- [ ] For brand story, navigate to **My Brand Deals** / **Brand Deal Room**, not only dashboard toggle.

---

## References

| File | Role |
|------|------|
| `public/app/home.html` | Switcher markup |
| `public/app/dashboard.js` | Toggle behavior, storage, `fetchAndRender` |
| `api/dashboard-home.js` | `GET /api/dashboard/home`, mock VM by role |
| `lib/dashboardRoleConfig.js` | KPI/pipeline labels |
| `public/app.js` | Shell nav roles, Dev Workspace, `getEffectiveRole` |
| `middleware/requireDealalityUser.js` | Real user flags |
| `middleware/requireMyDealsAccess.js` | Owner/admin gate |
| `lib/dealality/resolve-user.js` | Airtable role resolution |

**Related audits:** `reports/dealality-step-3-visible-copy-cleanup-audit.md`, `reports/dealality-step-4-data-loading-validation.md`.
