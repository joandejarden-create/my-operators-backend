# Owner-Operator Implementation Audit

**Audit date:** 2026-06-04  
**Repo:** `deal-capture-proxy` (Dealality)  
**Scope:** Phase 0 — read-only audit before schema/code changes for Owner-Operator hybrid model  
**Stack:** Custom JS/HTML, Airtable, Memberstack, Railway (no Wized)

---

## Executive summary

Dealality today treats **company classification**, **ecosystem role**, and **user permission** as overlapping but **not formally separated**. A vertically integrated owner-operator (e.g. GHL, Posadas, regional family groups) is forced into buckets such as **Hotel Owner** or **Hotel Management Company**, with **one primary `role` string** driving app shell navigation and several API gates.

**Operator marketplace visibility** is governed almost entirely by **Operator Setup - Master** (`submission_status === Active` when `activeOnly=1`), not by Company Profile ownership fields or third-party management intent.

**Recommended minimal safe path:**

1. Add Airtable fields on **Company Profile** (`Workspace Access`, tags, operating model, third-party availability, profile statuses) — schema plan before production edits.
2. Add **`lib/company-workspace-access.js`** (and browser mirror) as the single eligibility/access helper layer.
3. Extend **`classifyRole` / `roleInfoFromUserFieldsAsync`** to return **multi-flags** plus **`workspaceAccess[]`**, with **`primaryRole`** for legacy UI — do not rely on one `role` for gates.
4. Extend **`/api/me`** and **`requireDealalityUser`** to pass workspace access and eligibility flags.
5. Filter **Operator Explorer list API** using eligibility helpers (after migration window with documented legacy fallback).
6. Add **real workspace switcher** (`dealality_active_workspace`) wired to `/api/me` — keep **Command Center** `dc_dashboard_role_view` as **preview only** (rename copy).
7. **Backfill report** before changing production Company Type values.

**Highest risk if Airtable adds `Owner-Operator` before code:** Partner Directory maps any string containing `"OPERATOR"` to **3rd Party Operator**; app shell rejects unknown `dealality.role` values; `classifyRole` sets `role = "owner"` while flags may be mixed — inconsistent UX.

---

## 1. Files reviewed

| # | Area | Paths |
|---|------|--------|
| 1 | Company Type normalization | `lib/company-type-normalize.js`, `public/js/company-type-options.js` |
| 2 | Ecosystem role | `lib/company-role-normalize.js`, `public/js/company-role-options.js` |
| 3 | Company Profile API | `api/company-profile.js` |
| 4 | User role resolution | `lib/dealality/resolve-user.js`, `middleware/requireDealalityUser.js` |
| 5 | `/api/me` | `api/me.js` |
| 6 | Memberstack | `lib/memberstack/sync-member-to-airtable.js`, `lib/memberstack/memberstack-custom-fields.js`, `lib/memberstack/verify-token.js`, `public/js/dealality-memberstack-auth.js` |
| 7 | Dashboard preview | `reports/dealality-step-5-role-switcher-validation.md`, `public/app/dashboard.js`, `lib/dashboardRoleConfig.js`, `api/dashboard-home.js` (referenced in prior report) |
| 8 | App shell | `public/app.js` |
| 9 | Operator Explorer | `api/third-party-operators-list.js`, `api/operator-explorer.js`, `public/operator-explorer.html`, `public/js/operator-explorer.js` (grep + list handler) |
| 10 | Operator setup | `public/third-party-operator-setup-new-two.html`, `api/third-party-operator-intake.js`, `api/lib/operator-setup-new-base-read.js` |
| 11 | Operator deals | `public/operator-development-dashboard.html`, `public/operator-development-dashboard.js`, `middleware/requireOperatorDealsAccess.js`, `docs/operator-deal-requests-phase-2-scoping.md` |
| 12 | Operator scope | `lib/dealality/resolve-operator-scope.js` |
| 13 | Alignment snapshot | `api/operator-alignment-snapshot.js`, `api/my-deals.js` (`scoreOperatorMatchForDeal`), `docs/operator-alignment-snapshot-audit.md` |
| 14 | Matching / deal scope | `api/deal-readiness-context.js`, `lib/operator-capability-inputs.js`, `public/js/operator-capability-intake.js` |
| 15 | Access middleware | `middleware/requireMyDealsAccess.js` |
| 16 | Users / company docs | `docs/users-table-consolidation.md`, `docs/users-table-field-order.md`, `docs/dealality-demo-login-troubleshooting.md` |
| 17 | Partner Directory | `api/partner-directory.js`, `public/partner-directory.js` |
| 18 | Signup | `public/signup.html` |
| 19 | Company Settings UI | `public/company-settings.html` (ecosystem role + readonly company type) |
| 20 | Prior validation | `reports/dealality-step-5-role-switcher-validation.md` |

---

## 2. Current data model (two parallel taxonomies)

### 2.1 Company Type (`Company Profile` → **Company Type**)

**Canonical field:** `COMPANY_TYPE_AIRTABLE_FIELD = "Company Type"` (`lib/company-type-normalize.js`).

**Known Airtable values (code-mapped today):**

| Airtable value | Partner Directory filter key | UI label (directory) |
|----------------|------------------------------|----------------------|
| Hotel Owner | `HOTEL OWNERS` | Hotel Owners |
| Hotel Management Company | `HOTEL MGMT. COMPANY` | 3rd Party Operator |
| Hotel Brands (Franchise) | `HOTEL BRANDS (FRANCHISE)` | Hotel Brands (Franchise) |
| Hospitality Consultants | `HOSPITALITY CONSULTANTS` | Advisor / Consultant |
| Other | `OTHER` | Other |

**Company Profile API** (`api/company-profile.js` `COMPANY_TYPE_FORM_TO_AIRTABLE`):

- Form keys: `Brand`, `Operator`, `Owner`, `Advisor`, `Lender`, `Other` → Airtable strings above (Advisor/Lender → Hospitality Consultants).

**Signup** (`public/signup.html`): user-selectable `Hotel Owner`, `Hotel Management Company`, etc. — written to Memberstack `companyType` custom field and Airtable via signup upsert.

**Company Settings:** `#companyType` is **readonly** (“Set from your account; contact support to change”).

### 2.2 Ecosystem role (`Company's role in the hotel ecosystem`)

**Canonical field:** `COMPANY_ROLE_AIRTABLE_FIELD = "Company's role in the hotel ecosystem"` (`lib/company-role-normalize.js`).

**Form / Airtable options today:**

| Form key | Meaning in product copy |
|----------|-------------------------|
| Brand | Franchise / licensing platform |
| Operator | Operates under third-party brands (**operator only**) |
| **Both** | **Brand + operator** (not owner + operator) |
| Owner | Owner, developer, or investor |
| Advisor | Broker, consultant, service provider |
| Lender | Lender / legal-advisory |

**Answer: “Both” = Brand + Operator, not Owner + Operator.** There is **no** Owner-Operator ecosystem option today.

Company Settings (`public/company-settings.html` line ~715) exposes the same six options; ecosystem role is editable, company type is not.

### 2.3 Users table role fields

**Table:** Users `tbl6shiyz2wdUqE5F` (`docs/users-table-consolidation.md`).

**Resolution order** (`roleInfoFromUserFieldsAsync`):

1. **Platform Role**, **User Type**, or **Role** on Users (`ROLE_FIELD_CANDIDATES`).
2. If unknown → linked **Company Profile** → **Company Type** (`roleSource: "company"`).

**Link:** Users → **Company Profile** (`AIRTABLE_USERS_COMPANY_LINK_FIELD`, default `"Company Profile"`).

**Operator scope link (separate):** Users → **Operator Setup - Master** (`MAP_OPERATOR_SCOPE.usersOperatorSetupLink`) — used for deal requests, not for `classifyRole`.

---

## 3. `classifyRole()` — single primary role vs multi-flags

**File:** `lib/dealality/resolve-user.js`

### 3.1 Behavior today

```93:106:lib/dealality/resolve-user.js
export function classifyRole(roleRaw) {
  const r = normalizeRoleToken(roleRaw);
  // ...
  const isAdmin = ADMIN_ROLE_TOKENS.some((t) => r.includes(t));
  const isOwner = OWNER_ROLE_TOKENS.some((t) => r.includes(t));
  const isBrand = BRAND_ROLE_TOKENS.some((t) => r.includes(t));
  const isOperator = OPERATOR_ROLE_TOKENS.some((t) => r.includes(t));
  let role = roleRaw || "unknown";
  if (isAdmin) role = "admin";
  else if (isOwner) role = "owner";
  else if (isBrand) role = "brand";
  else if (isOperator) role = "operator";
  return { role, isAdmin, isOwner, isBrand, isOperator };
}
```

- **Flags** (`isOwner`, `isOperator`, etc.) use **substring** matching on env token lists.
- **`role` string** is **mutually exclusive** via `if / else if` priority: **admin > owner > brand > operator**.

### 3.2 Implications

| Input | isOwner | isOperator | `role` returned |
|-------|---------|------------|-----------------|
| `Hotel Owner` | true | false | `owner` |
| `Hotel Management Company` | false | true (`management`) | `operator` |
| Hypothetical `Owner-Operator` | true (`owner`) | true (`operator`) | **`owner` only** |
| Ecosystem: `Both - We both represent a brand...` | false | true (`both` contains `operator`? no - "both" doesn't match operator tokens) | likely `unknown` or partial |

**Note:** `"both"` does not match `OPERATOR_ROLE_TOKENS`; **Both** ecosystem value does **not** set `isOperator` via classifyRole on the full Airtable string unless it contains “operator” in the description (title case choice includes “Operate Hotels” → may match `operator` token).

**Gap:** Even when both `isOwner` and `isOperator` could be true, **middleware and app shell use `role` and individual flags inconsistently**.

### 3.3 Company Type as permission surrogate

When Users role fields are empty, **`Hotel Owner`** → `isOwner: true`, `roleSource: "company"`, warning **`role_from_company_type`** on `/api/me`.

A user at an owner-operator company typed as **Hotel Owner** gets **owner** access only from company type, **even if** they have Operator Setup and need operator workspace.

Typing the same company as **Hotel Management Company** yields **`operator`** only — **My Deals** blocked (`requireMyDealsAccess`).

**This is the core Owner-Operator blocker.**

---

## 4. `/api/me` response shape

**File:** `api/me.js`

**`dealality` block today:**

```352:362:api/me.js
    dealality: {
      role: dealalityRole.role,
      roleRaw: dealalityRole.roleRaw,
      roleSource: dealalityRole.roleSource || null,
      userRoleRaw: dealalityRole.userRoleRaw || null,
      companyTypeRaw: dealalityRole.companyTypeRaw || null,
      isOwner: dealalityRole.isOwner,
      isBrand: dealalityRole.isBrand,
      isOperator: dealalityRole.isOperator,
      isAdmin: dealalityRole.isAdmin,
    },
```

**Missing today:** `workspaceAccess`, `isOwnerOperator`, `activeWorkspace`, `operatorExplorerEligible`, company-level operating model / third-party availability.

**Operator scope** loaded when `isOperator || isAdmin` only — owner-only users with Operator Setup link do **not** get `allowedOperatingCompanyNames` on `/api/me` even if they operate hotels.

**`requireDealalityUser`** copies the same flags to `req.dealalityUser` — no workspace array.

---

## 5. Memberstack

### 5.1 Auth flow

- Browser: `public/js/dealality-memberstack-auth.js` — JWT for API Bearer.
- Server: `api/me.js` — `@memberstack/admin` `verifyToken`.
- Alternate: `lib/memberstack/verify-token.js` (JWKS path).

### 5.2 Sync to Airtable

**`lib/memberstack/sync-member-to-airtable.js`:**

- Webhook / member update → `upsertSignupUserRecord`.
- Custom fields via `readLogicalCustomFields` — includes **`companyType`** (`MS_CF.companyType`).
- Plan approval can set Users **Status** Active; **no workspace access** or Owner-Operator logic.

### 5.3 Permission model today

**Memberstack does not gate Owner vs Operator workspaces** in app code reviewed. Gates are **Airtable-derived** `req.dealalityUser` flags + middleware.

**No evidence** of separate Memberstack plans per workspace in code — only `MEMBERSTACK_APPROVED_PLAN_IDS` for signup status.

**Recommendation:** Store optional mirror metadata (`companyType`, `workspaceAccess`) on Memberstack custom fields for support/debug, but **canonical permissions = Airtable Company Profile + Users**.

---

## 6. Dashboard role preview (not real access)

**Documented in:** `reports/dealality-step-5-role-switcher-validation.md`

| Control | Storage | Effect |
|---------|---------|--------|
| Command Center “View as:” | `dc_dashboard_role_view` | KPI/pipeline **labels** via `GET /api/dashboard/home?role=` |
| Dev Workspace (localhost / `?devNav=1`) | `DEALALITY_DEV_WORKSPACE` | Full nav preview |

**Neither** updates Memberstack, `/api/me`, or `req.dealalityUser`.

**Classification:** Preview-only; **misleading** if presented as RBAC (High demo risk — already reported).

---

## 7. App shell navigation (`public/app.js`)

### 7.1 Role source

- `ALLOWED_ROLES = ['owner', 'brand', 'operator', 'admin']` — **no** `owner-operator`.
- `applyRoleFromMe`: sets `authenticatedRole` from `dealality.role` only if in `ALLOWED_ROLES` — **`owner-operator` would fail** and leave default **`owner`** on localhost or unset production behavior.
- `getBaseRole()`: defaults **`owner`**; on localhost **always `owner`**.
- `getEffectiveRole()`: dev workspace override OR `authenticatedRole || currentBaseRole`.

### 7.2 Nav mutual exclusivity (UI assumption)

`NAV_SECTIONS` entries use **one** `currentRole` string:

- **My Deals:** `roles: ['owner', 'operator', 'admin']` — nav can show for operator.
- **My Operator Deals:** `roles: ['operator', 'admin']` only.
- **Add New Deal:** `roles: ['owner', 'admin']` only.
- **Operator Setup:** `roles: ['operator', 'admin']` only.

An Owner-Operator with `role: "owner"` sees **My Deals** in nav but **not** My Operator Deals or Operator Setup — even if they have Operator Setup in Airtable.

### 7.3 API vs nav mismatch

| Surface | Operator access | Owner deal access |
|---------|-----------------|-------------------|
| Nav “My Deals” | Listed for `operator` | — |
| `requireMyDealsAccess` | **403** for operator | owner + admin only |
| Nav “My Operator Deals” | operator + admin | — |
| `requireOperatorDealsAccess` | operator + admin | **403** for owner |

Owner-Operator needs **both** gates to pass when acting in each workspace — today requires **both flags true**, but nav uses **single role**.

---

## 8. Operator Explorer visibility

**API:** `api/third-party-operators-list.js` (also `GET /api/third-party-operators`, `GET /api/operator-explorer/operators`)

### 8.1 Current gates

1. Loads **Operator Setup - Master** + Profile + Platform + children (new-base tables).
2. **`activeOnly=1`** (or `explorer=1`): filter `submission_status` / `dealStatus` === **`Active`** (case-insensitive).
3. Optional env `OPERATOR_EXPLORER_HIDE_TEST_RECORDS=1` — name heuristics for test rows.

### 8.2 Not gated today

- Company Profile **Company Type** (Hotel Owner vs Management Company).
- Third-party management availability.
- Owner-operator vs pure third-party operator.
- Company visibility / Platform Visibility.
- `Workspace Access` (field does not exist).
- Conflict flags.

### 8.3 List row shape

Built by `buildNewBaseListRow` in `api/lib/operator-setup-new-base-read.js` — includes `companyName`, `primaryServiceModel`, `dealStatus`, etc. **No Owner-Operator badge fields.**

**Partner Directory** uses Company Profile separately (`api/partner-directory.js`) — companies can appear as **Hotel Owners** while same entity has Active Operator Setup (data inconsistency risk).

---

## 9. Operator Deal Requests

**Gate:** `middleware/requireOperatorDealsAccess.js` — `isAdmin || isOperator` (plus demo bypass).

**Scope:** `lib/dealality/resolve-operator-scope.js` — Users → **Operator Setup - Master** → `company_name`; match **Operating Company Name** on Operator Deal Requests.

**Not gated:**

- Third-party management availability.
- Owner-operator “own portfolio only” model.
- Company Type.

**`/api/me`:** operator permissions only when `dealalityRole.isOperator || isAdmin` — owner-classified users with operator master link **may not receive** operator scope on me.

**UI:** `public/operator-development-dashboard.js` — loads `/api/me` first for allow-list (per phase-2 doc).

---

## 10. Owner workspace / My Deals

**Gate:** `middleware/requireMyDealsAccess.js` — **`isAdmin || isOwner`** only.

Operators (including Hotel Management Company classification) get **403** `forbidden_role`.

**Implication:** Company classified as **Hotel Management Company** cannot use My Deals APIs even if they also own assets — must use **Hotel Owner** (or user Platform Role owner) for deal sponsor flows.

**No related-party flag** found in middleware when sponsor invites their own operating company (future Phase 9).

---

## 11. Operator Alignment Snapshot

**Routes:** `api/operator-alignment-snapshot.js` — profile-level and company-level GET handlers.

**Scoring:** `api/my-deals.js` → `scoreOperatorMatchForDeal` with operator prefill from Operator Setup.

**No Owner-Operator-specific eligibility** or disclaimer in code paths reviewed.

**Docs:** `docs/operator-alignment-snapshot-audit.md` — product naming and phased rollout; recommends profile-level snapshot before Explorer-wide.

**Eligibility today:** Driven by deal context + operator prefill presence, not Company Profile hybrid classification.

---

## 12. Matching / deal readiness

**`api/deal-readiness-context.js`:** `isOperatorInScope`, franchise-only contexts, owner-operated paths — deal-field driven.

**`lib/operator-capability-inputs.js` / `public/js/operator-capability-intake.js`:** operator-in-scope from deal form values.

**Gap:** No check for sponsor’s **third-party management availability** when suggesting operators from Explorer/list.

---

## 13. Can one Company Profile link to Operator Setup while sponsoring deals?

**Yes, architecturally:**

- **Users** row links **Company Profile** (company identity, Company Type).
- Same Users row can link **Operator Setup - Master** (operator scope for deal requests).
- **Deals** are tied to owner/user flows separately (My Deals APIs) — not blocked by dual links in code; blocked by **role gates** if user resolves to `operator` only.

**CALA pattern (HE/CALA, Arbor):** Operator Setup new-base records + company narrative “owner-operator” in copy — likely **one company**, **one operator master**, deals as sponsor — **requires dual workspace**, not duplicate company records.

---

## 14. Audit questions — answers

| Question | Answer |
|----------|--------|
| Where is Company Type used as user permission? | Fallback in `roleInfoFromUserFieldsAsync` → `classifyRole(companyTypeRaw)` sets `isOwner` / `isOperator` flags and primary `role`. |
| Where is Company Type used as role? | Same path; `/api/me` exposes `role` + `companyTypeRaw`; app shell `applyRoleFromMe` uses `dealality.role`. |
| Where does `classifyRole()` force one role? | `if/else` chain for `role` string; app shell accepts only one of four roles. |
| What breaks if Airtable adds Owner-Operator before code? | `normalizeCompanyTypeToFilterKey` maps to **HOTEL MGMT. COMPANY** (substring `OPERATOR`); Partner Directory mislabels; `applyRoleFromMe` may ignore role; inconsistent badges. |
| One Company Profile + Operator Master + owner deals? | Possible in data model; **API gates** prevent full UX unless user has both owner and operator resolution. |
| Operator Explorer gates? | **Active** `submission_status` (+ optional test-name hide). |
| Operator Deal Request gates? | Auth operator/admin + company name scope on ODR rows. |
| My Deals gates? | **owner** or **admin** only. |
| UI assumes mutually exclusive roles? | **Yes** — single `currentRole` for nav; ecosystem **Both** is brand+operator, not owner+operator. |
| Smallest safe path? | Schema + workspace helpers + multi-flag resolve + `/api/me` + list filter + real workspace switcher (see Executive summary). |
| Production data risks? | Mis-typed Company Type; Explorer showing own-portfolio-only groups; owners locked out of operator workspace and vice versa. |

---

## 15. Recommended minimal safe path (phased)

### Phase A — Documentation & schema (no production mass edit)

1. `owner-operator-airtable-schema-plan.md` — confirm field names in Airtable UI.
2. `owner-operator-memberstack-permission-plan.md` — Airtable-canonical, MS metadata optional.
3. `owner-operator-backfill-candidates.md` — report only.

### Phase B — Code foundation (safe without new Airtable options)

1. `lib/company-workspace-access.js` + browser mirror.
2. `resolve-user.js` — multi-flag + `workspaceAccess` from new fields **or** derived rules during migration.
3. `api/me.js` + `requireDealalityUser` — extended payload.
4. Env docs: `DEALITY_OWNER_ROLES` include `owner-operator`.

### Phase C — Airtable + mappings (coordinated deploy)

1. Add **Owner-Operator** to Company Type + new fields.
2. `api/company-profile.js`, `company-type-normalize.js`, signup, company settings derivation UI.
3. **Fix** `normalizeCompanyTypeToFilterKey` for `Owner-Operator` **before** enabling option in Airtable.

### Phase D — Marketplace & workspaces

1. `third-party-operators-list.js` eligibility filter + `reviewBeforeOutreach`.
2. Explorer card badges.
3. `public/app.js` workspace switcher + nav based on `workspaceAccess`.
4. Rename Command Center preview label.
5. `requireMyDealsAccess` / `requireOperatorDealsAccess` → workspace-aware helpers.
6. Alignment snapshot note + eligibility check.

### Phase E — QA

`owner-operator-qa-results.md` — six scenarios from prompt.

---

## 16. Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Airtable Owner-Operator option before code deploy | **High** | Deploy code mappings first or use private option |
| `role` stays `owner` while user needs operator nav | **High** | `workspaceAccess` drives nav, not `role` alone |
| Explorer hides/shows wrong companies | **High** | Explicit third-party availability + profile status |
| Dual records created manually | Medium | Backfill report; governance |
| Memberstack/custom field drift | Low | Airtable source of truth |
| Command Center preview confusion | Medium | Rename “Preview dashboard as” |
| Legacy `Both` ecosystem confusion | Medium | Document; add Owner-Operator ecosystem option |

---

## 17. Files likely requiring changes (Phase 1+)

| Priority | File |
|----------|------|
| P0 | `reports/owner-operator-airtable-schema-plan.md` (new) |
| P0 | `reports/owner-operator-memberstack-permission-plan.md` (new) |
| P1 | `lib/company-workspace-access.js` (new) |
| P1 | `lib/dealality/resolve-user.js` |
| P1 | `api/me.js`, `middleware/requireDealalityUser.js` |
| P1 | `lib/company-type-normalize.js`, `api/company-profile.js` |
| P1 | `lib/company-role-normalize.js`, `public/js/company-role-options.js` |
| P2 | `api/third-party-operators-list.js`, `public/js/operator-explorer.js` |
| P2 | `middleware/requireMyDealsAccess.js`, `middleware/requireOperatorDealsAccess.js` |
| P2 | `public/app.js`, `public/company-settings.html`, `public/signup.html` |
| P2 | `public/js/company-type-options.js`, `public/js/company-workspace-access.js` (new) |
| P3 | `api/operator-alignment-snapshot.js`, `lib/operator-alignment-*.js` |
| P3 | `api/deal-readiness-context.js`, `public/app/dashboard.js` (preview label) |
| P3 | `public/partner-directory.js`, `api/partner-directory.js` |

---

## 18. TODOs requiring Airtable confirmation

- [ ] Exact **Company Type** select options currently live in base (vs code assumptions).
- [ ] Whether **Workspace Access**, **Company Type Tags**, **Operating Model**, **Third-Party Management Availability** fields exist or must be created.
- [ ] Formula vs code-computed eligibility fields.
- [ ] Canonical source: Company Profile vs Operator Setup - Master for operating model / third-party availability.
- [ ] Users-level **Workspace Access** override field — needed or company-level only.

---

## 19. Definition of done for Phase 0

- [x] Audit report created at `/reports/owner-operator-implementation-audit.md`
- [x] No application code or Airtable schema changed in Phase 0
- [ ] Stakeholder review of audit before Phase 1 schema plan
- [ ] Phase 1–13 per implementation prompt (subsequent work)

---

*End of Phase 0 audit.*
