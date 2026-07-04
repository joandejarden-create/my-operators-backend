# Memberstack + Owner-Operator + Demo — Audit

**Date:** 2026-06-04  
**Scope:** Identity (Memberstack) vs authorization (Airtable) for workspace switching and gates.

---

## 1. What Memberstack fields are read today

| Source | Fields / data | Module |
|--------|----------------|--------|
| JWT verify | `id`, `sub`, `email` (payload variants) | `lib/memberstack/verify-token.js`, `api/me.js` |
| Webhook / GET member | `email`, `customFields`, `plans` | `lib/memberstack/sync-member-to-airtable.js` |
| Custom fields (logical) | First/last name, company name, phone, **company type** (onboarding), company profile id, Airtable user id | `lib/memberstack/memberstack-custom-fields.js` → `readLogicalCustomFields()` |
| Plans | Plan IDs vs `MEMBERSTACK_APPROVED_PLAN_IDS` → Users **Status** only (Active) | `sync-member-to-airtable.js` |

**Not read for permissions:** Memberstack plan display names as workspace roles (logged as non-authoritative hint only).

---

## 2. What is written into Airtable Users (before this fix)

| Path | Previous behavior | Risk |
|------|-------------------|------|
| `sync-member-to-airtable.js` → `upsertSignupUserRecord()` | On **every** webhook update: overwrote **User Type** from Memberstack `companyType` custom field | Single-role assumption; could mask Owner-Operator; did **not** set Workspace Access but confused role columns |
| Signup form | Sets User Type, identity, pending status | Intended for new users only |
| `/api/me` email match | Syncs Memberstack id to Unique Webflow ID / slug only | Safe |

**Not written from Memberstack (before):** Company Profile link, Workspace Access, Company Type on Company Profile.

---

## 3. Is Memberstack plan / role treated as permission?

| Consumer | Uses Memberstack for gates? |
|----------|----------------------------|
| `/api/me` | **No** — `roleInfoFromUserFieldsAsync` → `buildDealalityAccessContext` from Airtable |
| `middleware/requireDealalityUser.js` | **No** — `resolveDealalityUser` |
| `public/app.js` workspace switcher | **No** — `dealality.workspaceAccess` from `/api/me` |
| `sync-member-to-airtable` (old) | **Partial** — User Type from custom field `companyType` treated like role |

**Approved plan IDs** only flip Users **Status** to Active — not workspace access.

---

## 4. Does sync assume one role only?

**Yes (legacy).** `upsertSignupUserRecord` mapped one `companyType` string → **User Type** on each Memberstack update.

**After fix:** Sync uses `upsertMemberIdentityToAirtable` — role hint goes to **Auth Role Hint** (or fill-if-blank User Type / Platform Role only when empty).

---

## 5. Platform Role / User Type overwrite?

| Scenario | Before | After |
|----------|--------|-------|
| Webhook on existing Owner-Operator user | User Type could be overwritten by MS company type | **Preserved** unless blank |
| New user | User Type set from signup/sync hint | Same, hint only when blank fields |

---

## 6. Linked Company Profile preserved?

| Scenario | Before | After |
|----------|--------|-------|
| Webhook update | Not touched by signup upsert (no Company Profile in payload) | **Explicitly preserved** if link exists |
| Create with MS `companyProfileId` custom field | N/A | Link set only when Users row has no link |

---

## 7. Company Profile read after sync?

**Unchanged.** `/api/me` and `resolve-user.js` always load Company Profile after Users lookup and merge fields (**company wins** for Workspace Access, Company Type, Company Type Tags).

---

## 8. Demo without Admin?

| Workspace Access | `isDemo` | `isAdmin` | Production Owner/Operator/Brand gates |
|------------------|----------|-----------|--------------------------------------|
| `Demo` only | true | false | Blocked unless workspace listed |
| `Demo` + `Owner` | true | false | Owner allowed; Operator/Brand preview via switcher only if in list |

**Rule:** `Demo` does not imply `Admin` or other workspaces unless explicitly in **Workspace Access**.

---

## 9. Owner-Operator → Owner + Operator access?

| Company Type | Workspace Access | Result |
|--------------|------------------|--------|
| `Hotel Owner - Operator` | `Owner`, `Operator` | `primaryRole: owner-operator`, `legacyRole: owner`, switcher shows both |
| Same + empty WS | (infer) | Inferred Owner + Operator via `isOwnerOperatorCompany()` |

**Single Memberstack account** — no duplicate members.

---

## 10. Memberstack-only role usage (remaining)

| Location | Usage | Severity |
|----------|---------|----------|
| `getBaseRole()` localhost | Dev default `owner` before `/api/me` | Dev only |
| `classifyRole` in resolve-user | Deprecated; not used for gates | Low |
| Signup `User Type` | Onboarding label, not workspace | OK if not synced over |

---

## Risks (residual)

1. **Auth Role Hint** column may not exist in all bases — sync strips unknown fields automatically.
2. **Users row** with its own Workspace Access still merged under company-wins rule when company has values.
3. **Memberstack → Airtable mirror** (optional custom field write-back) still not implemented — intentional deferral.

---

## Recommended minimal safe fix (implemented)

| # | Change | File |
|---|--------|------|
| 1 | Identity-only Memberstack sync; protected fields | `lib/memberstack/upsert-member-identity-to-airtable.js` |
| 2 | Webhook uses new upsert | `lib/memberstack/sync-member-to-airtable.js` |
| 3 | Company wins on merge + hint conflict warnings | `lib/dealality/resolve-user.js` |
| 4 | `/api/me` exposes company + memberstack ids | `api/me.js` |
| 5 | Document identity-only verify | `lib/memberstack/verify-token.js` |
| 6 | Browser JWT module comments | `public/js/dealality-memberstack-auth.js` |
| 7 | Unit tests | `scripts/test-memberstack-airtable-source-of-truth.mjs` |

---

## Files requiring changes (this PR)

- `lib/memberstack/upsert-member-identity-to-airtable.js` (new)
- `lib/memberstack/memberstack-role-hint.js` (new)
- `lib/memberstack/sync-member-to-airtable.js`
- `lib/memberstack/verify-token.js` (comments)
- `lib/dealality/resolve-user.js`
- `api/me.js`
- `public/js/dealality-memberstack-auth.js` (comments)
- `scripts/test-memberstack-airtable-source-of-truth.mjs` (new)
- `scripts/test-company-workspace-access.mjs` (extended)
- `reports/memberstack-owner-operator-demo-audit.md` (this file)
- `reports/memberstack-owner-operator-demo-qa.md`

**Not changed:** Production Airtable data, Memberstack plans, app-shell switcher logic (prior collapse fix separate), backfill scripts.
