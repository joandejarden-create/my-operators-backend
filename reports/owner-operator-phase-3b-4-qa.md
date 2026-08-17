# Owner-Operator Phase 3B / 4 — QA Results

**Date:** 2026-06-04  
**Scope:** Middleware workspace gates, Operator Explorer eligibility, API payload fields, Explorer UI badges, deal-request eligibility foundation  
**Prerequisite:** Phase 3A + Airtable value `Hotel Owner - Operator`

---

## Automated tests

| Script | Result |
|--------|--------|
| `scripts/test-company-workspace-access.mjs` | Pass |
| `scripts/test-operator-marketplace-eligibility.mjs` | Pass |
| `scripts/test-user-workspace-gates.mjs` | Pass |

---

## Part 1 — Owner workspace gate (`requireMyDealsAccess`)

**File:** `middleware/requireMyDealsAccess.js`  
**Helper:** `lib/dealality/user-workspace-gates.js` → `userCanAccessOwnerWorkspace`

| Persona | Expected | Unit test |
|---------|----------|-----------|
| Hotel Owner | My Deals allowed | Pass |
| Hotel Owner - Operator | My Deals allowed | Pass |
| Hotel Management Company only | Denied | Pass |
| Brand only | Denied | Pass |
| Admin | Allowed | Pass |

Uses `canAccessOwnerWorkspace`, `flags.isOwner`, `workspaceAccess` includes `Owner` — not `role === "owner"` alone.

---

## Part 2 — Operator workspace gate (`requireOperatorDealsAccess`)

**File:** `middleware/requireOperatorDealsAccess.js`  
**Helper:** `userCanAccessOperatorWorkspace`

| Persona | Expected | Unit test |
|---------|----------|-----------|
| Hotel Management Company | Operator Deals allowed | Pass |
| Hotel Owner - Operator | Operator Deals allowed | Pass |
| Hotel Owner only | Denied | Pass |
| Brand only | Denied | Pass |
| Admin | Allowed | Pass |

---

## Part 3 — Operator Explorer eligibility

**Files:** `lib/company-workspace-access.js` (`evaluateOperatorMarketplaceEligibility`, `enrichOperatorListRowWithEligibility`), `api/third-party-operators-list.js`

### Rules (when `activeOnly=1`)

1. `submission_status` === Active (unchanged)
2. `operatorExplorerEligible === true` from helper
3. Operator workspace (or legacy active unmatched setup / Hotel Management Company)
4. Third-Party Management Availability:
   - **No** → excluded
   - **Yes** → included, `reviewBeforeOutreach: false`
   - **Selectively** / **Case-by-case** → included, `reviewBeforeOutreach: true`
   - **Blank / Unknown** → included (migration), `eligibilitySource` legacy / owner-operator-legacy-migration
5. Hidden/anonymous Company Platform Visibility → excluded

### Company Profile join

- Loads **Company Profile** table by name match to `company_name`
- Unmatched names: legacy active Operator Setup still eligible (`legacy-active-operator-setup`)

### Scenarios (unit-level)

| Scenario | Visible in Explorer (`activeOnly`) |
|----------|-----------------------------------|
| Active Hotel Management Company | Yes |
| Active Hotel Owner - Operator, third-party blank | Yes (migration) |
| Active Hotel Owner - Operator, third-party Yes | Yes, no review flag |
| Active Hotel Owner - Operator, Selectively | Yes, `reviewBeforeOutreach` |
| Active Hotel Owner - Operator, No | **No** |
| Inactive setup | No |
| Hotel Owner only (no operator workspace) + active setup | No |

**Important:** Company Type `Hotel Owner - Operator` alone does not bypass third-party **No** or missing operator workspace.

---

## Part 4 — API list response fields

Each operator row from `GET /api/third-party-operators?activeOnly=1` may include:

| Field | Present |
|-------|---------|
| `isOwnerOperator` | Yes |
| `companyType` | Yes |
| `normalizedCompanyType` | Yes |
| `workspaceAccess` | Yes |
| `operatorExplorerEligible` | Yes |
| `operatorDealRequestEligible` | Yes |
| `thirdPartyManagementAvailability` | Yes |
| `thirdPartyManagementAvailabilityStatus` | Yes |
| `reviewBeforeOutreach` | Yes |
| `eligibilitySource` | Yes |
| `companyDisplayBadges` | Yes |

---

## Part 5 — Operator Explorer UI

**Files:** `public/js/operator-explorer.js`, `public/operator-explorer.html`

- Type line: **Hotel Owner - Operator** when `isOwnerOperator` / `OWNER_OPERATOR` (not generic 3rd Party Operator only)
- Compact badges from `companyDisplayBadges` or fallback
- **Review availability before outreach** when `reviewBeforeOutreach`
- Tooltip on badge row (product-neutral copy)
- Safe when badge fields missing

---

## Part 6 — `/api/me` + Operator Deal Requests foundation

**`api/me.js` / `resolve-user.js`:**

- `dealality.operatorDealRequestEligible`
- `dealality.reviewBeforeOutreach`
- `dealality.thirdPartyManagementAvailabilityStatus`
- `dealality.eligibilitySource`

**`public/operator-development-dashboard.js`:** Comment documents reuse of marketplace helper for future owner-side operator outreach lists.

**Deferred:** No separate “eligible operators for outreach” picker API yet — ODR list remains scoped by operating company name on existing rows.

---

## Part 7 — Related-party placeholder

**File:** `lib/dealality/operator-related-party-note.js`  
Constant message only — not wired to deal invite UI (deferred until sponsor/operator company comparison is safe).

---

## Regression checklist (manual)

- [ ] `/api/me` for Hotel Owner, Hotel Management Company, Hotel Owner - Operator
- [ ] My Deals loads for owner + owner-operator users
- [ ] My Operator Deals loads for operator + owner-operator users
- [ ] Operator Explorer loads; legacy mgmt companies still listed
- [ ] Hotel Owner - Operator cards show correct type + badges
- [ ] Partner Directory: `Hotel Owner - Operator` → `OWNER_OPERATOR`, not 3rd Party Operator
- [ ] `public/app.js` nav still defaults owner-operator to owner legacy role
- [ ] Command Center preview switcher unchanged

---

## Files changed (summary)

| File | Change |
|------|--------|
| `lib/company-workspace-access.js` | Marketplace eligibility + hidden profile |
| `lib/dealality/user-workspace-gates.js` | **New** |
| `lib/dealality/operator-related-party-note.js` | **New** placeholder |
| `middleware/requireMyDealsAccess.js` | Workspace gate |
| `middleware/requireOperatorDealsAccess.js` | Workspace gate |
| `middleware/requireDealalityUser.js` | Passthrough eligibility |
| `api/third-party-operators-list.js` | Company Profile join + filter |
| `lib/dealality/resolve-user.js` | User company marketplace fields |
| `api/me.js` | Extended dealality payload |
| `public/js/operator-explorer.js` | Badges + type label |
| `public/operator-explorer.html` | Badge CSS |
| `public/operator-development-dashboard.js` | Foundation comment |
| `scripts/test-operator-marketplace-eligibility.mjs` | **New** |
| `scripts/test-user-workspace-gates.mjs` | **New** |

---

## Not in this PR

- Workspace switcher (`dealality_active_workspace`)
- Remaining Airtable schema fields (written when present)
- Operator Alignment Snapshot
- Full related-party conflict enforcement
- Production data backfill

---

*Phase 3B/4 — automated tests pass; complete manual checklist before release.*
