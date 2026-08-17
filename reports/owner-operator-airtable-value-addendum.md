# Owner-Operator Airtable Value Addendum

**Date:** 2026-06-04  
**Airtable field:** Company Profile → **Company Type**  
**Canonical Airtable single-select value:** `Hotel Owner - Operator`

---

## Normalization model

| Layer | Value |
|-------|--------|
| Airtable (canonical) | `Hotel Owner - Operator` |
| Partner Directory filter key | `OWNER_OPERATOR` |
| Product / API `primaryRole` | `owner-operator` |
| Legacy nav `role` / `legacyRole` | `owner` |
| Company Profile form key | `owner_operator` |

**Aliases** (all → `OWNER_OPERATOR`, not a second company type):

- `Hotel Owner - Operator` (canonical)
- `Owner-Operator`
- `Owner Operator`
- `Hotel Owner Operator`
- `owner_operator` (form)

---

## Bug fixed

Before this addendum, `Hotel Owner - Operator` could fall through substring rules to **`HOTEL OWNERS`** because the string contains `OWNER` but not the hyphenated token `OWNER-OPERATOR`.

`isOwnerOperatorCompanyTypeString()` now runs **before** generic `OWNER` / `OPERATOR` matching.

---

## Files updated

| File | Change |
|------|--------|
| `lib/company-type-normalize.js` | Canonical Airtable value; `isOwnerOperatorCompanyTypeString()`; alias map |
| `lib/company-workspace-access.js` | Uses shared type detector; badge text |
| `api/company-profile.js` | Form ↔ `Hotel Owner - Operator` mappings |
| `public/js/company-type-options.js` | Browser mirror |
| `scripts/test-company-type-normalize.mjs` | Canonical value tests |
| `scripts/test-company-workspace-access.mjs` | Full flag/role tests for canonical value |

**Unchanged (by design):**

- `lib/company-role-normalize.js` — ecosystem role remains `OwnerOperator` (separate from Company Type)
- Operator Explorer list filtering (Phase 3B+)
- Airtable production data

---

## Test results

```text
node scripts/test-company-type-normalize.mjs  → ok
node scripts/test-company-workspace-access.mjs → ok
```

### `Company Type: Hotel Owner - Operator`

| Check | Expected | Result |
|-------|----------|--------|
| `normalizeCompanyTypeToFilterKey` | `OWNER_OPERATOR` | Pass |
| `isOwnerOperatorCompany` | `true` | Pass |
| `workspaceAccess` | Owner, Operator | Pass |
| `primaryRole` | `owner-operator` | Pass |
| `legacyRole` / `role` | `owner` | Pass |
| `isOwner` / `isOperator` | `true` | Pass |
| Not `HOTEL MGMT. COMPANY` | — | Pass |
| Not `HOTEL OWNERS` | — | Pass |

---

## `/api/me` / `resolve-user` (inferred)

User linked to Company Profile with `Company Type = Hotel Owner - Operator` resolves via `roleInfoFromUserFieldsAsync` → `buildDealalityAccessContext`:

- `workspaceAccess: ["Owner", "Operator"]`
- `primaryRole: "owner-operator"`
- `legacyRole: "owner"`
- `flags.isOwnerOperator: true`
- `canAccessOwnerWorkspace` / `canAccessOperatorWorkspace`: `true`
- Operator scope on `/api/me` when operator workspace allowed (unchanged from Phase 3A)

---

## Operator Explorer

`Hotel Owner - Operator` alone does **not** add Explorer visibility. Explorer still requires (future/current):

- Operator workspace access
- Active Operator Setup (`submission_status`)
- Third-party management eligibility when enforced
- No change in this addendum to `api/third-party-operators-list.js`

---

## Legacy app shell

`public/app.js` continues to map `primaryRole` / `legacyRole` to nav role **`owner`** for Owner-Operator users so existing nav does not break.

---

*Addendum complete — use `Hotel Owner - Operator` as the Airtable write target for new Owner-Operator companies.*
