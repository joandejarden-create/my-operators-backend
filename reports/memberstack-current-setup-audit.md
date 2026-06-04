# Memberstack Current Setup Audit — Identity vs Authorization

**Date:** 2026-06-04  
**Scope:** How Memberstack is configured and used in the Deal Capture / Dealality codebase **today** (read-only audit; no code or Memberstack dashboard changes in this step).

**Target architecture:**

| Layer | Question |
|-------|----------|
| **Memberstack** | Can this person log in? |
| **Airtable** | What company / workspace access do they have? |

---

## Executive summary

The **runtime permission path** (JWT → `/api/me` → middleware gates → app shell workspace switcher) is **already Airtable-driven**. Memberstack is used for **login identity** (JWT, member id, email) and for **optional onboarding hints** and **account status** on webhook sync.

**You do not need** separate Memberstack plans for Owner, Operator, Brand, Demo, Owner-Operator, or Admin for app permissions. A **single login plan** (e.g. “Dealality Access”) plus **Workspace Access** on the linked **Company Profile** is the correct model.

Recent code (`upsert-member-identity-to-airtable.js`, updated `sync-member-to-airtable.js`) aligns the **Memberstack webhook** with identity-only sync. Some **signup** paths still write **User Type** on form submit (onboarding, not workspace gates).

---

## Part 1 — Answers to audit questions

### 1. What does Memberstack currently provide to the app?

| Data | Used for | Where |
|------|----------|--------|
| **Member ID** (`mem_sb_…` / `mem_…`) | Match Users row (`Unique Webflow ID`, `Slug`) | `verify-token.js`, `memberstackAuth`, `/api/me` |
| **Email** | Fallback user match; display | JWT payload, sync, `/api/me` |
| **JWT (eyJ…)** | Bearer auth on API routes | `dealality-memberstack-auth.js`, middleware |
| **Name** (custom fields) | Airtable Users first/last on sync | `memberstack-custom-fields.js` → sync |
| **Plans** | (a) Approved plan → Users **Status** = Active; (b) non-authoritative **role hint** string | `sync-member-to-airtable.js` only |
| **Custom fields** | Identity + onboarding hints (company name, phone, company type label, Airtable/company IDs) | Webhook sync, signup mirror to MS |
| **Metadata** | Same as custom fields (Memberstack API shape) | Webhook / GET member |
| **Role hints** | Stored in **Auth Role Hint** (if column exists) or fill-if-blank **User Type**; **not** used as workspace gates | Sync only; warnings if hint ≠ Airtable WS |

Memberstack does **not** supply `workspaceAccess`, `isOwnerOperator`, or Demo preview flags to gates directly.

---

### 2. Where does the code read Memberstack plans?

| Location | Purpose | Permission impact? |
|----------|---------|-------------------|
| `lib/memberstack/sync-member-to-airtable.js` → `memberHasApprovedPlan()` | If member’s plan ID is in `MEMBERSTACK_APPROVED_PLAN_IDS` / `MEMBERSTACK_SIGNUP_FREE_PLAN_ID`, set Users **Status** to Active | **No** — account activation only |
| `lib/memberstack/signup-member.js` | Signup may assign `MEMBERSTACK_SIGNUP_PENDING_PLAN_ID` on **create** | **No** — gates login/product access in Memberstack, not Owner/Operator/Brand in app |
| `memberstackRoleHintFromPayload()` | Joins plan labels into a **hint string** (logged path: `memberstack_plans_non_authoritative`) | **No** — not passed to `buildDealalityAccessContext` as workspace |

**Not read for gates:** `/api/me`, `resolve-user.js`, `requireDealalityUser.js`, `user-workspace-gates.js`, `public/app.js`.

---

### 3. Where does the code read Memberstack custom fields or metadata?

| Location | Fields read (logical) |
|----------|------------------------|
| `lib/memberstack/memberstack-custom-fields.js` | First/last name, company name, phone, **company type** (onboarding), reason to join, how did you hear, Airtable user id, company profile id |
| `sync-member-to-airtable.js` | Via `readLogicalCustomFields()` on webhook payload |
| `lib/memberstack/signup-member.js` | Writes custom fields **to** Memberstack after signup (mirror) |

**Permission resolution does not read** Memberstack custom fields at request time — only Airtable after user is found.

---

### 4. Does any code treat Memberstack **plan** as the user’s real permission?

**No** for Owner / Operator / Brand / Demo / Owner-Operator workspace gates.

**Yes (limited)** for:

- **Users Status** (Active vs Pending) when approved plan IDs match env — operational onboarding, not route gates.
- **Memberstack product access** (can open gated Webflow pages / login) — outside this repo’s workspace model.

---

### 5. Does any code treat Memberstack **role** as the user’s real permission?

**No** on the main auth path. `/api/me` and middleware use `roleInfoFromUserFieldsAsync` → `buildDealalityAccessContext()` from **Airtable** (merged user + company).

**Indirect / legacy:**

- `dealality.role` / `legacyRole` in API are **derived from Airtable** workspace flags, not from Memberstack JWT.
- Sync may write **User Type** or **Auth Role Hint** from Memberstack **company type** custom field — if those columns are mistaken for “the role,” admins could be confused; **gates still use Workspace Access** when set on company.

---

### 6. Does Memberstack webhook sync overwrite User Type, Platform Role, Workspace Access, or Company Profile?

**Current webhook path** (`syncMemberstackMemberToAirtable` → `upsertMemberIdentityToAirtable`):

| Field | On update (existing user) | On create |
|-------|---------------------------|-----------|
| **Workspace Access** | **Never written** (`MS_SYNC_NEVER_WRITE`) | Never |
| **Company Type** (company) | Never | Never |
| **Company Profile** link | **Preserved** if already linked | Set only if empty + valid `companyProfileId` in MS custom field |
| **Platform Role / User Type** | **Preserved** if already set; fill-if-blank only | May set from hint |
| **Auth Role Hint** | Fill-if-blank (if columns exist) | May set |

**Legacy path removed from webhook:** `upsertSignupUserRecord()` no longer called from webhook (that path overwrote **User Type** every time).

**Still overwrites User Type on signup form POST:** `api/signup.js`, `api/signup-pilot.js` (user re-submits signup) — see fix plan.

---

### 7. Is Workspace Access read from Company Profile, Users, or both?

**Both, with precedence:**

1. Load **Users** row by Memberstack id / email.
2. Load linked **Company Profile** fields.
3. **Merge:** `{ ...company, ...user }` then **company wins** for `Workspace Access`, `Company Type`, `Company Type Tags` when company has values (`mergeUserAndCompanyFields` in `resolve-user.js`).
4. `normalizeWorkspaceAccess()` in `company-workspace-access.js`: explicit **Workspace Access** on merged fields first; else infer from company type / tags / ecosystem role.

**Authoritative for Owner-Operator:** Company Profile **Company Type** = `Hotel Owner - Operator` and **Workspace Access** = `Owner`, `Operator` (recommended).

---

### 8. Is linked Company Profile preserved during Memberstack sync?

**Yes**, on webhook update: if Users already has **Company Profile** linked, sync **skips** changing that field.

**Exception:** New user with empty link + `Company Profile ID` in Memberstack custom field → link is set once (supporting onboarding automation).

---

### 9. Dependency on separate Memberstack plans for Owner, Operator, Brand, Demo, Admin?

**No** in application permission code.

| Plan usage in repo | Purpose |
|------------------|---------|
| `MEMBERSTACK_APPROVED_PLAN_IDS` | Flip Airtable Users to **Active** |
| `MEMBERSTACK_SIGNUP_PENDING_PLAN_ID` | Assign pending plan on **new** Memberstack member at signup |

Neither maps plan name → Owner/Operator/Brand/Demo workspace in `api/me` or middleware.

---

### 10. If Memberstack plan/custom field says one thing and Airtable Workspace Access says another, which wins today?

**Airtable wins** for all app permissions and `/api/me` payload.

- Company **Workspace Access** overrides conflicting values on Users when company field is populated.
- Memberstack hint vs Airtable: `memberstack_role_hint_conflicts_with_airtable_workspace` may appear in `/api/me` `meta.warnings` (no change to effective access).

Memberstack **plan** does not override workspace list.

---

### 11. What would break if we removed role/plan-based authorization from Memberstack?

| Removed | Impact |
|---------|--------|
| Per-workspace Memberstack plans | **None** on gates (already unused) |
| `MEMBERSTACK_APPROVED_PLAN_IDS` → Status Active | Users might stay **Pending** in Airtable until manual Active; app may still 403 if status checked elsewhere |
| Pending plan on signup create | New members might get full Memberstack CMS access earlier/later — product decision |
| Custom field **company type** on sync | Losing **hint** columns only; permissions unchanged if Company Profile WS set |
| Memberstack login entirely | **Everything** — no JWT, no `/api/me` |

**Would not break:** Owner-Operator dual workspace, Demo preview switcher, My Deals / Operator dashboards — as long as Airtable **Workspace Access** is correct.

---

### 12. Smallest safe change to make Memberstack identity-only?

**Status:** **Mostly already done** in repo for webhook + `/api/me` + gates.

| Item | Status |
|------|--------|
| Webhook identity-only sync | **Done** |
| Company-wins merge | **Done** |
| Gates use `workspaceAccess` / `canAccess*` | **Done** |
| `verify-token.js` identity-only | **Done** (documented) |
| Single MS plan for login | **Dashboard config** (not code) |
| Signup routes not overwriting User Type on update | **Optional residual** |
| Optional **Auth Role Hint** column in Airtable | **Schema** (recommended, not required) |

See `reports/memberstack-identity-only-fix-plan.md` for residual follow-ups only.

---

## Part 2 — Recommended Memberstack setup

### Plans

| Recommendation | Detail |
|----------------|--------|
| **Keep** | **One** plan meaning “allowed to log into Dealality” (e.g. **Dealality Access**, or existing free/approved plan used only for login + Status Active) |
| **Stop using for permissions** | Any plan per Owner / Operator / Brand / Demo / Admin / Owner-Operator |
| **Owner-Operator plan** | **Not needed** |
| **Demo plan** | **Not needed** — Demo = `Workspace Access: Demo` in Airtable |
| **Admin plan** | **Not needed** for app workspace — use `Workspace Access: Admin` in Airtable (platform admin); preserve any existing secure admin tooling outside Memberstack if already in use |

`MEMBERSTACK_APPROVED_PLAN_IDS` can remain as “this member completed signup / was approved” → Users **Status** only.

### Custom fields (optional hints)

| Field | Role |
|-------|------|
| First Name, Last Name, Company Name, Phone | Identity / CRM |
| Company Profile ID, AirTable User ID | Linking automation |
| **Company Type** (MS) | **Auth Role Hint only** — mirror onboarding selection; **not** workspace source |
| **Auth Role Hint** (Airtable Users) | Preferred store for MS onboarding label |
| Invite Source, Onboarding Type | Optional future hints |

Do **not** add a Memberstack custom field “Workspace Access” and expect the app to honor it unless you later build an explicit non-authoritative mirror (not recommended).

### Airtable (source of truth)

| Field | Values (canonical) |
|-------|-------------------|
| **Company Type** | `Hotel Owner - Operator`, etc. |
| **Workspace Access** | `Owner`, `Operator`, `Brand`, `Demo`, `Admin` (multi-select) |
| **Company Profile** link on Users | Required for company-driven access |

**Demo:** `Workspace Access` includes `Demo` only → preview switcher, not production writes.  
**Demo + Owner:** `Demo`, `Owner` → Owner gates pass; other workspaces preview.  
**Owner-Operator:** `Company Type` = `Hotel Owner - Operator`, `Workspace Access` = `Owner`, `Operator`.

---

## Part 3 — Code path map (current)

```
Login (Memberstack JWT)
  → memberstackAuth / verify-token.js     [identity only]
  → resolveDealalityUser / api/me         [Airtable Users + Company Profile]
  → buildDealalityAccessContext           [Workspace Access, flags]
  → middleware (requireMyDealsAccess, etc.) [canAccess* / workspaceAccess]
  → app shell                             [switchableWorkspaces from /api/me]

Webhook (Memberstack event)
  → sync-member-to-airtable.js
  → upsert-member-identity-to-airtable.js [identity; preserve WS + company link]
```

---

## Part 4 — Manual QA: inspect `/api/me`

No new code required — use browser DevTools.

### Steps

1. Log in on the **published** site (or local app shell with Memberstack).
2. Open **DevTools → Network**.
3. Filter by `me` or reload after login.
4. Open **`GET /api/me`** (or `POST`) → **Response** tab.
5. Expand `dealality` and `meta.warnings`.

### App shell shortcut

1. Open `http://localhost:3000/app` (or deployed app).
2. Log in via shell gate.
3. Network → `/api/me` after `dealality-shell-auth-ready`.

### Webflow shortcut

Load `dealality-webflow-me-bootstrap.js` after `dealality-memberstack-auth.js`; listen for `dealality-me-ready` or inspect the same Network request.

### Console (after successful `/api/me`)

```javascript
// If bootstrap stored response on event:
// Or re-fetch:
const h = await DealalityMemberstackAuth.getAuthHeaders();
const r = await fetch((window.DEALALITY_API_BASE || location.origin) + '/api/me', { headers: h.headers });
const j = await r.json();
console.log(j.dealality);
```

### Expected shapes

**Hotel Owner - Operator**

```json
"workspaceAccess": ["Owner", "Operator"],
"primaryRole": "owner-operator",
"legacyRole": "owner",
"flags": { "isOwner": true, "isOperator": true, "isOwnerOperator": true },
"isDemo": false,
"isAdmin": false
```

**Demo only**

```json
"workspaceAccess": ["Demo"],
"isDemo": true,
"isAdmin": false,
"canAccessOwnerWorkspace": false,
"demoPreviewWorkspaces": ["Owner", "Operator", "Brand"]
```

**Owner only**

```json
"workspaceAccess": ["Owner"],
"isOwner": true,
"isOperator": false
```

**Operator only**

```json
"workspaceAccess": ["Operator"],
"isOperator": true,
"isOwner": false
```

---

## Part 5 — What Joan Should Do in Memberstack

### Plain-English answer

**You do not need a new “Owner-Operator” plan in Memberstack.**  
**You do not need separate Demo, Owner, Operator, or Brand plans for the app to work.**

Use Memberstack for **login only**: one plan that means “this person is allowed to sign in to Dealality.”

Put real permissions in **Airtable**:

1. Open the user’s **Company Profile** (linked from **Users**).
2. Set **Company Type** (e.g. **Hotel Owner - Operator**).
3. Set **Workspace Access** (e.g. **Owner** and **Operator** together; or **Demo** for sandbox-only).
4. Keep **Users** linked to that company; put Memberstack member id in **Unique Webflow ID** / **Slug**.

**For Owner-Operator companies:**  
Company Type = **Hotel Owner - Operator**  
Workspace Access = **Owner**, **Operator**  
One Memberstack account per person — no second login for “operator mode.”

**For Demo sandbox:**  
Workspace Access = **Demo** (Demo is **not** Admin).  
They can preview other workspaces in the app switcher but should not get production write access unless you also add Owner/Operator/Brand on that company.

**For platform admin:**  
Use **Workspace Access = Admin** in Airtable if you use that value in your base — not a special Memberstack Admin plan for app permissions.

**In Memberstack dashboard:**  
- Keep or rename a **single access plan** (e.g. “Dealality Access”).  
- Optional custom fields for name/company are fine; they are hints only.  
- Do **not** maintain parallel plan per workspace type.

---

## Related reports

| Report | Notes |
|--------|--------|
| `reports/memberstack-owner-operator-demo-audit.md` | Pre-identity-only implementation audit |
| `reports/memberstack-owner-operator-demo-qa.md` | QA after identity sync change |
| `reports/owner-operator-memberstack-permission-plan.md` | Original target architecture (still valid) |
| `docs/dealality-demo-login-troubleshooting.md` | Demo user + `/api/me` troubleshooting |
| `docs/users-table-consolidation.md` | Users + Company Profile link |

---

## Change impact classification

**This audit:** Read-only — **no** code, Airtable, or Memberstack changes.

**If implementing residual fix plan only:** **Low–Medium** (signup path alignment).
