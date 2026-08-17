# Owner-Operator Memberstack Permission Plan

**Date:** 2026-06-04  
**Prerequisite:** `reports/owner-operator-implementation-audit.md`  
**Principle:** **Airtable is canonical** for workspace access; Memberstack handles authentication and optional metadata mirror.

---

## 1. Current Memberstack integration

| Component | Path | Behavior |
|-----------|------|----------|
| Token verify (me) | `api/me.js` | `@memberstack/admin` `verifyToken` → member id |
| Token verify (alt) | `lib/memberstack/verify-token.js` | JWKS / admin API |
| Browser JWT | `public/js/dealality-memberstack-auth.js` | Bearer for API calls |
| Webhook sync | `lib/memberstack/sync-member-to-airtable.js` | Member → `upsertSignupUserRecord` |
| Custom field IDs | `lib/memberstack/memberstack-custom-fields.js` | `MS_CF.companyType`, `companyName`, etc. |
| Plan gate | `memberHasApprovedPlan` | Sets Users status Active on approved plan IDs |

**No Memberstack plan** in code distinguishes Owner vs Operator workspace today.

---

## 2. Target permission model

### 2.1 Do not create

- Separate Memberstack **accounts** per workspace hat  
- New Memberstack **plans** per Owner-Operator (unless billing/product later requires it)

### 2.2 Do create / use

| Layer | Stores | Example |
|-------|--------|---------|
| **Company Profile (Airtable)** | `Company Type`, `Workspace Access`, eligibility fields | `Owner-Operator` + `[Owner, Operator]` |
| **Users (Airtable)** | Optional `Workspace Access` override, Platform Role | Override empty → inherit company |
| **Memberstack custom fields (optional mirror)** | `companyType`, future `workspaceAccess` string | Debug/support only |
| **App session** | `dealality_active_workspace` in localStorage | `Owner` or `Operator` |
| **API** | `/api/me` → `workspaceAccess`, flags, `primaryRole` | Client + middleware |

### 2.3 Access helpers (server + browser)

Implement in `lib/company-workspace-access.js`:

```text
canAccessOwnerWorkspace(entity)
canAccessOperatorWorkspace(entity)
canAccessBrandWorkspace(entity)
isAdmin(entity)
isOwnerOperatorCompany(companyFields)
isThirdPartyManagementAvailable(companyFields)
isOperatorExplorerEligible(companyFields, operatorMasterFields)
```

**Middleware migration:**

| Middleware | Today | Target |
|------------|-------|--------|
| `requireMyDealsAccess` | `isAdmin \|\| isOwner` | `isAdmin \|\| canAccessOwnerWorkspace(req.dealalityUser)` |
| `requireOperatorDealsAccess` | `isAdmin \|\| isOperator` | `isAdmin \|\| canAccessOperatorWorkspace(...)` |

`req.dealalityUser` should include resolved `workspaceAccess: string[]` from company + user override.

---

## 3. Memberstack custom fields

### 3.1 Existing

| Logical | MS_CF key (env override) | Set on signup? |
|---------|--------------------------|----------------|
| companyType | `MEMBERSTACK_CF_COMPANY_TYPE` | Yes (`signup.html`) |
| companyName | `MEMBERSTACK_CF_COMPANY_NAME` | Yes |
| companyProfileId | `MEMBERSTACK_CF_COMPANY_PROFILE_ID` | After company create |
| airtableUserId | `MEMBERSTACK_CF_AIRTABLE_USER_ID` | After user upsert |

### 3.2 Proposed (optional mirror)

| Logical | Suggested MS field label | Format |
|---------|-------------------------|--------|
| workspaceAccess | `Workspace Access` | Comma-separated: `Owner,Operator` |
| primaryWorkspace | `Primary Workspace` | Single: `Owner` |
| companyTypeTags | `Company Type Tags` | Comma-separated |

**Write path:** After Company Profile PATCH or onboarding complete, optional Admin API update to Memberstack member — **non-blocking**; failure logs warning only.

**Read path:** Do **not** use Memberstack as primary for gates; always resolve from Airtable on `/api/me`.

---

## 4. `/api/me` response extension (proposed)

```json
{
  "dealality": {
    "role": "owner-operator",
    "primaryRole": "owner-operator",
    "roleRaw": "Owner-Operator",
    "roleSource": "company",
    "workspaceAccess": ["Owner", "Operator"],
    "activeWorkspace": "Owner",
    "flags": {
      "isOwner": true,
      "isOperator": true,
      "isBrand": false,
      "isAdmin": false,
      "isOwnerOperator": true
    },
    "companyTypeRaw": "Owner-Operator",
    "operatorExplorerEligible": false,
    "operatorDealRequestEligible": false,
    "thirdPartyManagementAvailability": "No"
  }
}
```

**Backward compatibility:** Keep `role`, `isOwner`, `isOperator` for existing clients. `activeWorkspace` from request header or cookie optional later; Phase 1 use client localStorage only.

---

## 5. `resolve-user.js` changes

### 5.1 `classifyRole`

- Keep token-based **flags** (multiple may be true).
- Set `primaryRole`:
  - If `isOwnerOperator` (type or tags) → `owner-operator`
  - Else existing priority admin > owner > brand > operator
- Set `role` = `primaryRole` for legacy (document breaking change if any client expects only four values).

### 5.2 Load company fields

When Company Profile linked, fetch (batch):

- Company Type, Workspace Access, Operating Model, Third-Party Management Availability, profile statuses

Merge into `resolveDealalityUser` return object.

### 5.3 Env tokens

Update documentation:

```bash
DEALITY_OWNER_ROLES=owner,hotel owner,hotel owners,owner-operator,owner operator,owner_operator
DEALITY_OPERATOR_ROLES=operator,management,mgmt,hotel management,owner-operator,owner operator,owner_operator
```

Substring match on `owner-operator` sets **both** flags today — ensure `primaryRole` handles explicitly.

---

## 6. App shell (`public/app.js`)

### 6.1 `applyRoleFromMe`

- Accept `primaryRole` `owner-operator` OR derive nav from `workspaceAccess` array.
- Add `ALLOWED_ROLES` entry or stop using single role for nav — **prefer `hasWorkspaceAccess` per nav item**.

### 6.2 Real workspace switcher

| Key | `dealality_active_workspace` |
| Values | `Owner`, `Operator`, `Brand` (subset of `/api/me` workspaceAccess) |
| UI label | `Workspace: Owner-side / Operator-side` |

### 6.3 Separate from preview

| Control | Key | Label |
|---------|-----|-------|
| Real | `dealality_active_workspace` | Workspace / Acting as |
| Preview | `dc_dashboard_role_view` | **Preview dashboard as** |

---

## 7. Operator scope (`resolve-operator-scope.js`)

**Today:** Runs for `/api/me` when `isOperator || isAdmin`.

**Change:** Run when `canAccessOperatorWorkspace(effectiveUser)` even if `primaryRole === 'owner-operator'` and `isOwner` true.

Ensures Owner-Operator users get `allowedOperatingCompanyNames` for My Operator Deals.

---

## 8. Signup / webhook flow

1. Signup collects capabilities (phase 5 UI) → writes Company Profile tags + type.
2. `syncMemberstackMemberToAirtable` — unchanged path; optional mirror `workspaceAccess` to MS custom field.
3. No second Memberstack member on “complete operator profile.”

---

## 9. Security notes

- Never trust client-only `dealality_active_workspace` for API authorization — middleware uses Airtable-resolved `workspaceAccess`.
- Tampering `activeWorkspace` in localStorage only affects **nav** until APIs re-check.
- Operator deal-request scope remains **Operating Company Name** allow-list.

---

## 10. Rollback

1. Remove Workspace Access values on companies → app falls back to `classifyRole` on Company Type only.
2. Revert middleware to `isOwner` / `isOperator` checks.
3. Memberstack custom fields optional — can leave stale without breaking auth.

---

## 11. QA checklist (Memberstack + auth)

- [ ] Owner-Operator user: one Memberstack login, `/api/me` shows both workspace flags
- [ ] JWT valid; operator APIs 403 without Operator workspace access
- [ ] Owner APIs 403 without Owner workspace access
- [ ] Email fallback match still syncs Memberstack id to Users
- [ ] Webhook signup does not strip Workspace Access on update

---

*Permission plan — implement with Phase 3–4 code changes after schema fields exist or migration defaults are coded.*
