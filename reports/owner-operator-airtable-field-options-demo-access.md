# Owner-Operator Airtable Field Options + Demo Workspace Access

**Date:** 2026-06-04  
**Change impact:** **High** (Company Profile writes, `/api/me`, workspace gates, marketplace eligibility)

---

## Exact Airtable labels confirmed

### Company Type Tags (multiple select)

Owns Hotels, Develops Hotels, Operates Own Portfolio, Operates Affiliated-Owned Hotels, Operates Third-Party Hotels, Brand / Franchisor, Capital Provider, Asset Manager, Broker, Consultant / Advisor, Service Provider, Lender

### Workspace Access (multiple select)

Owner, Operator, Brand, **Demo**, Admin

### Operating Model (single select)

Own-and-Operate Only, Affiliated-Owned Hotels Only, Third-Party Management, Mixed Owner/Operator Model, Asset-Light Management Platform, Franchisee/Operator Model, Unknown / To Confirm

### Third-Party Management Availability (single select)

Yes, No, Selectively, **Case-by-Case**, Unknown / To Confirm

### Profile Status (single select)

Not Started, In Progress, Complete, Needs Review, Not Applicable

### Company Type (canonical Owner-Operator)

**Hotel Owner - Operator** (internal key: `owner_operator` / `OWNER_OPERATOR`)

---

## Files updated

| File | Change |
|------|--------|
| `lib/company-profile-owner-operator-fields.js` | Exact select labels; `toAirtableOperatingModel`, `toAirtableThirdPartyManagement`; write-time normalization |
| `lib/company-workspace-access.js` | `WORKSPACE_DEMO`, `isDemo`, `canAccessDemoWorkspace`, `demoPreviewWorkspaces`, third-party eligibility keys |
| `lib/dealality/user-workspace-gates.js` | Demo documented; `userCanAccessDemoWorkspace`; Demo ≠ Admin |
| `lib/dealality/resolve-user.js` | Pass through Demo fields |
| `middleware/requireDealalityUser.js` | `isDemo`, `canAccessDemoWorkspace`, `demoPreviewWorkspaces` on `req.dealalityUser` |
| `middleware/requireMyDealsAccess.js` | Comment: Demo alone does not pass |
| `middleware/requireOperatorDealsAccess.js` | Comment: Demo alone does not pass |
| `api/me.js` | `isDemo`, `canAccessDemoWorkspace`, `demoPreviewWorkspaces`, `flags.isDemo` |
| `public/js/company-profile-capabilities.js` | UI option labels aligned with Airtable |

---

## Demo workspace behavior

| Rule | Behavior |
|------|----------|
| Demo ≠ Admin | `isAdmin` only from Admin workspace or admin role tokens |
| Demo preview | When `Workspace Access` includes **Demo**, `/api/me` returns `demoPreviewWorkspaces: ["Owner","Operator","Brand"]` |
| Production writes | Demo **does not** satisfy `userCanAccessOwnerWorkspace` / `userCanAccessOperatorWorkspace` unless Owner/Operator also present |
| Capability derivation | **Does not** add Demo — assign Demo manually in Airtable |
| Middleware | My Deals / Operator Deals gates unchanged logic; Demo-only users get 403 |

---

## Operator marketplace (exact third-party labels)

| Airtable value | Explorer / deal-request |
|----------------|-------------------------|
| Yes | Eligible |
| Selectively | Eligible + `reviewBeforeOutreach` |
| Case-by-Case | Eligible + `reviewBeforeOutreach` |
| No | Not eligible |
| Unknown / To Confirm | Not eligible |

Input aliases (e.g. `case-by-case`) accepted; **writes** use **Case-by-Case**.

---

## Tests run

```bash
node scripts/test-company-profile-owner-operator.mjs
node scripts/test-company-workspace-access.mjs
node scripts/test-user-workspace-gates.mjs
node scripts/test-operator-marketplace-eligibility.mjs
```

All pass.

---

## Confirmations

- [x] Airtable writes use canonical labels (Operating Model, Third-Party, Workspace Access)
- [x] Demo is not Admin
- [x] Demo does not bypass production write gates
- [x] Demo + Owner / Demo + Operator combine correctly
- [x] Hotel Owner - Operator unchanged as canonical Company Type

---

## Deferred

- Phase 6 workspace switcher consuming `demoPreviewWorkspaces`
- Production Airtable backfill of old option spellings
- Memberstack plan changes

---

## Manual QA

1. Company Settings → save capabilities → verify Airtable **Operating Model** = `Mixed Owner/Operator Model` (not lowercase legacy).
2. Verify **Third-Party Management Availability** = `Case-by-Case` when applicable (not `Case-by-case`).
3. Set **Workspace Access** = `Demo` on a test company → `/api/me` shows `isDemo: true`, `demoPreviewWorkspaces` populated.
4. Demo-only user → My Deals and Operator Deals return 403 (unless demo-safe bypass env is on for operator).
5. Demo + Owner → My Deals allowed via Owner access.

---

## Rollback

Revert `lib/company-workspace-access.js` Demo exports and `lib/company-profile-owner-operator-fields.js` label constants; redeploy server.
