# Memberstack + Owner-Operator + Demo — QA Report

**Date:** 2026-06-04  
**Change impact:** **High** (auth/sync path; no production Airtable writes in this PR)

---

## Files changed

| File | Change |
|------|--------|
| `lib/memberstack/upsert-member-identity-to-airtable.js` | **New** — identity-only upsert; protected Workspace Access / Company Profile |
| `lib/memberstack/memberstack-role-hint.js` | **New** — hint vs Airtable conflict warnings |
| `lib/memberstack/sync-member-to-airtable.js` | Uses identity upsert; plan = status only |
| `lib/memberstack/verify-token.js` | Identity-only documentation |
| `lib/dealality/resolve-user.js` | Company-wins merge (exported); MS hint warnings |
| `api/me.js` | `memberstackId`, `companyProfileId`, `companyName` on response |
| `public/js/dealality-memberstack-auth.js` | `/api/me` authority comments |
| `scripts/test-memberstack-airtable-source-of-truth.mjs` | **New** unit tests |
| `scripts/test-company-workspace-access.mjs` | Owner-Operator cases |
| `reports/memberstack-owner-operator-demo-audit.md` | Audit |
| `reports/memberstack-owner-operator-demo-qa.md` | This report |

---

## Source of truth decision

| Layer | Responsibility |
|-------|----------------|
| **Memberstack** | Authentication: member id, email, session JWT |
| **Airtable Users** | User record, optional role **hint**, link to Company Profile |
| **Airtable Company Profile** | **Company Type**, **Workspace Access**, Demo, Owner-Operator classification |

If Memberstack custom field says “Operator” but Company **Workspace Access** is `Owner` only → **Airtable wins**; `/api/me` meta may include `memberstack_role_hint_conflicts_with_airtable_workspace`.

---

## Current Memberstack fields (reference)

See `lib/memberstack/memberstack-custom-fields.js` (`MS_CF.*`). Env overrides: `MEMBERSTACK_CF_*`, `AIRTABLE_USERS_AUTH_ROLE_HINT_FIELDS`.

---

## Sync behavior changes

| Action | Before | After |
|--------|--------|-------|
| Webhook update | `upsertSignupUserRecord` → overwrite **User Type** | Identity fields only; WS / company link preserved |
| Workspace Access from MS | Not written (good) | Still **never** written |
| Company Type on company | Not written | Still **never** written |
| Role hint | Overwrote User Type | **Auth Role Hint** (or User Type if blank) |
| Approved plan | Status → Active | Unchanged |

---

## `/api/me` example shapes (illustrative)

### Hotel Owner - Operator

```json
{
  "memberstackId": "mem_sb_…",
  "airtable": {
    "airtableUserId": "rec…",
    "companyProfileId": "reccQJUKO2RAY9zhE",
    "companyName": "Dovetail + Co"
  },
  "dealality": {
    "workspaceAccess": ["Owner", "Operator"],
    "primaryRole": "owner-operator",
    "legacyRole": "owner",
    "flags": { "isOwner": true, "isOperator": true, "isOwnerOperator": true, "isDemo": false },
    "canAccessOwnerWorkspace": true,
    "canAccessOperatorWorkspace": true,
    "canAccessDemoWorkspace": false,
    "demoPreviewWorkspaces": []
  }
}
```

### Demo only

```json
{
  "dealality": {
    "workspaceAccess": ["Demo"],
    "primaryRole": "unknown",
    "isDemo": true,
    "isAdmin": false,
    "canAccessOwnerWorkspace": false,
    "canAccessOperatorWorkspace": false,
    "canAccessDemoWorkspace": true,
    "demoPreviewWorkspaces": ["Owner", "Operator", "Brand"]
  }
}
```

### Demo + Owner

```json
{
  "dealality": {
    "workspaceAccess": ["Demo", "Owner"],
    "isDemo": true,
    "canAccessOwnerWorkspace": true,
    "canAccessOperatorWorkspace": false,
    "demoPreviewWorkspaces": ["Owner", "Operator", "Brand"]
  }
}
```

---

## Automated test results

```bash
node scripts/test-memberstack-airtable-source-of-truth.mjs
node scripts/test-company-workspace-access.mjs
```

| Case | Result |
|------|--------|
| Owner Airtable | Pass |
| Operator Airtable | Pass |
| Owner-Operator WS + type | Pass |
| Demo only | Pass |
| Demo + Owner / Operator | Pass |
| MS hint vs Airtable conflict | Pass |
| Sync patch preserves WS + company link | Pass |
| Fill-if-blank User Type | Pass |

---

## Conflict behavior (Memberstack vs Airtable)

| Memberstack hint | Airtable Workspace Access | Effective | Warning |
|------------------|---------------------------|-----------|---------|
| Operator | Owner | Owner | `memberstack_role_hint_conflicts_with_airtable_workspace` |
| Owner-Operator | Owner, Operator | Both | None |

---

## Manual QA checklist

1. **Owner user** — Log in; `GET /api/me` → `workspaceAccess: ["Owner"]`; shell shows Owner-Side nav (single workspace = no switcher unless Demo).
2. **Operator user** — `workspaceAccess: ["Operator"]`; Operator nav / dashboards.
3. **Hotel Owner - Operator** — `workspaceAccess: ["Owner","Operator"]`; workspace switcher shows Owner-Side + Operator-Side; My Deals + My Operator Deals routes allowed per workspace.
4. **Demo-only** — Company WS = `Demo`; `isDemo: true`; switcher shows three preview sides; production writes still blocked by gates.
5. **Demo + Owner** — Owner writes work; Operator/Brand preview only in switcher.
6. **Memberstack webhook** — Trigger member update; confirm Users **Workspace Access** and **Company Profile** unchanged in Airtable.
7. **Collapsed sidebar** — Workspace control still visible (compact) after prior CSS fix.

---

## Regression risks

- Webhook sync no longer refreshes **User Type** on every event — intentional; onboarding must set Company Profile WS.
- Missing **Auth Role Hint** field — sync falls back to fill-if-blank **User Type** or strips unknown columns.
- `/api/me` adds fields — backward compatible.

**Pages to retest:** App shell (`/app`), My Deals, Operator dashboards, Company Settings (read-only WS), Memberstack login on Webflow.

**Airtable fields touched by sync (when webhook runs):** Email, names, Unique Webflow ID, optional Auth Role Hint — **not** Workspace Access.

---

## Deferred items

- Optional Memberstack custom-field **mirror** of Workspace Access (write-back from Airtable).
- Separate Memberstack plans per workspace (not recommended).
- Production backfill of Auth Role Hint column (schema optional).
- Wized — out of scope.

---

## Rollback

Revert `sync-member-to-airtable.js` to call `upsertSignupUserRecord` only if emergency (restores old User Type overwrite behavior). Prefer forward fix + Airtable WS on Company Profile.
