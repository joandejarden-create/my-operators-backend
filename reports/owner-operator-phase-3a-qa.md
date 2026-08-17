# Owner-Operator Phase 3A — QA Results

**Date:** 2026-06-04  
**Scope:** Shared workspace helpers, company type/role normalization, `resolve-user`, `/api/me`, minimal app shell compatibility  
**Out of scope (not tested here):** Airtable schema, Explorer filter, workspace switcher, middleware gate changes beyond `req.dealalityUser` passthrough

---

## Automated tests

| Script | Result |
|--------|--------|
| `node scripts/test-company-type-normalize.mjs` | **Pass** |
| `node scripts/test-company-workspace-access.mjs` | **Pass** |

**New assertions:**

- `Owner-Operator` → filter key `OWNER_OPERATOR` (not `HOTEL MGMT. COMPANY`)
- Hotel Owner / Management Company unchanged
- Owner-Operator workspace flags: Owner + Operator, `primaryRole` `owner-operator`, `legacyRole` `owner`
- Ecosystem **Both** → Brand + Operator only (no Owner)
- Third-party availability Yes/No/Unknown

---

## Scenario matrix (unit-level)

| # | Scenario | isOwner | isOperator | isOwnerOperator | workspaceAccess | primaryRole | legacyRole (`role`) |
|---|----------|---------|------------|-----------------|-----------------|-------------|---------------------|
| 1 | Hotel Owner | true | false | false | Owner | owner | owner |
| 2 | Hotel Management Company | false | true | false | Operator | operator | operator |
| 3 | Owner-Operator (type) | true | true | true | Owner, Operator | owner-operator | owner |
| 4 | Hotel Brands (Franchise) | false | false* | false | Brand | brand | brand |
| 5 | Empty / unknown | false | false | false | [] | unknown | unknown |

\*Brand flag true for scenario 4 (`isBrand: true`).

---

## `/api/me` response (expected shape)

**Additive fields under `dealality`:**

- `primaryRole`, `legacyRole`, `workspaceAccess`, `flags`
- `companyType`, `isOwnerOperator`
- `canAccessOwnerWorkspace`, `canAccessOperatorWorkspace`, `canAccessBrandWorkspace`
- `thirdPartyManagementAvailable`, `activeWorkspace`

**Preserved:**

- `role` (legacy — same as `legacyRole` for Owner-Operator → `owner`)
- `isOwner`, `isOperator`, `isBrand`, `isAdmin`
- `permissions.allowedOperatingCompanyNames` when operator workspace allowed

**Operator scope:** Resolved when `canAccessOperatorWorkspace` **or** legacy `isOperator` **or** admin — Owner-Operator with Operator access receives operator scope on `/api/me`.

**Warnings:** Still emits `role_from_company_type` when role inferred from company; adds `owner_operator_legacy_role_mapped_to_owner` when applicable.

---

## App shell (`public/app.js`)

| Check | Result |
|-------|--------|
| `owner-operator` / `owner_operator` in allowed list | **Yes** (tolerated) |
| `applyRoleFromMe` maps to `owner` for nav | **Yes** — uses `legacyRole` first |
| Command Center `dc_dashboard_role_view` | **Unchanged** (preview-only) |
| Full workspace switcher | **Not in this PR** |

---

## Files changed

| File | Change |
|------|--------|
| `lib/company-workspace-access.js` | **New** — access helpers |
| `lib/company-type-normalize.js` | `OWNER_OPERATOR` filter key + safe matching |
| `lib/company-role-normalize.js` | `OwnerOperator` ecosystem option |
| `lib/dealality/resolve-user.js` | Multi-flag resolution + company field fetch |
| `api/me.js` | Extended `dealality` payload; operator scope fix |
| `middleware/requireDealalityUser.js` | Passthrough new fields to `req.dealalityUser` |
| `public/app.js` | Legacy role normalization for shell |
| `public/js/company-type-options.js` | Browser mirror |
| `public/js/company-role-options.js` | Browser mirror |
| `scripts/test-company-workspace-access.mjs` | **New** |
| `scripts/test-company-type-normalize.mjs` | Owner-Operator cases |

---

## Manual QA checklist (staging / local)

- [ ] Sign in as **Hotel Owner** user — `/api/me` → `workspaceAccess: ["Owner"]`, `role: "owner"`
- [ ] Sign in as **Hotel Management Company** — `workspaceAccess: ["Operator"]`, operator permissions block populated if Master linked
- [ ] Mock merged fields with `Company Type: Owner-Operator` on linked company — `primaryRole: owner-operator`, `role: owner`, both flags true
- [ ] App shell loads nav without error when `primaryRole` is `owner-operator`
- [ ] Partner Directory filter (if used) shows **Owner-Operator** label for `OWNER_OPERATOR` key when type present in data
- [ ] **My Deals** still owner-gated (middleware unchanged — owner-operator gets owner via `isOwner` true)
- [ ] **My Operator Deals** still operator-gated — owner-operator gets `isOperator` true but API middleware not changed in 3A; verify `isOperator` on `req.dealalityUser`

---

## Known limitations (Phase 3B+)

| Item | Status |
|------|--------|
| Airtable `Workspace Access` field | Not created — inference only |
| Operator Explorer eligibility filter | Not implemented |
| Real workspace switcher (`dealality_active_workspace`) | Not implemented |
| `requireMyDealsAccess` / `requireOperatorDealsAccess` workspace-aware gates | Not implemented |
| `dealality.role` still `owner` for Owner-Operator | Intentional for legacy nav |
| Company Settings onboarding multi-select | Not implemented |

---

## Regression risks

| Risk | Mitigation |
|------|------------|
| Users with only Platform Role set bypass company inference | Unchanged — user fields merged last |
| `Both` ecosystem gains Operator without Owner | Documented; same as product rule |
| Owner-Operator string in unrelated company names | Checked `OWNER-OPERATOR` / `OWNER OPERATOR` before generic OPERATOR |
| `/api/me` payload size | Additive only |

---

*Phase 3A QA — automated pass; complete manual checklist before production deploy.*
