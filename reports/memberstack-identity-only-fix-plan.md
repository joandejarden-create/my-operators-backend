# Memberstack Identity-Only — Fix Plan (Residual)

**Date:** 2026-06-04  
**Prerequisite:** `reports/memberstack-current-setup-audit.md`

---

## Verdict

The **identity-only model is already implemented** for the paths that matter for day-to-day login and permissions:

| Target behavior | Current code status |
|-----------------|-------------------|
| `verify-token.js` — identity only | **Meets target** |
| Webhook `sync-member-to-airtable.js` — no WS/company overwrite | **Meets target** |
| `resolve-user.js` — Airtable access + company-wins merge | **Meets target** |
| `api/me.js` — Airtable-driven `dealality` payload | **Meets target** |
| Middleware gates — `workspaceAccess` / `canAccess*` | **Meets target** |
| Browser JWT — no plan-based gates | **Meets target** |

**Do not re-implement** the webhook/`/api/me` split unless regression is found.

This plan covers **optional residual work** and **operational** steps only.

---

## Residual gaps (optional code)

### 1. Signup form still overwrites User Type on update

| | |
|--|--|
| **Files** | `api/signup.js`, `api/signup-pilot.js`, `lib/signup-airtable-upsert.js` |
| **Current** | `POST /api/signup` updates existing user by email and sets **User Type** from form `companyType` every time |
| **Desired** | Signup writes **User Type** on **create** only; on update patch identity fields only (or fill-if-blank), same rules as `buildMemberstackSyncPatch` |
| **Risk** | **Low** — affects re-signup only; `/api/me` still uses company **Workspace Access** when set |
| **Minimal fix** | Reuse `buildMemberstackSyncPatch` from `upsert-member-identity-to-airtable.js` in signup upsert, or call shared `upsertMemberIdentityToAirtable` after Memberstack member exists |

### 2. Auth Role Hint column may be missing in Airtable

| | |
|--|--|
| **Files** | `lib/memberstack/upsert-member-identity-to-airtable.js` |
| **Current** | If `AIRTABLE_USERS_AUTH_ROLE_HINT_FIELDS` columns absent, hint falls back to fill-if-blank **User Type** |
| **Desired** | Add optional **Auth Role Hint** (single line) on Users so MS onboarding label never overwrites **User Type** after first set |
| **Risk** | **Low** — schema-only; sync strips unknown fields |
| **Minimal fix** | `node scripts/ensure-users-platform-fields.mjs` or manual column; document in admin runbook |

### 3. Documentation drift

| | |
|--|--|
| **Files** | `reports/memberstack-owner-operator-demo-audit.md` (describes **legacy** webhook → `upsertSignupUserRecord`) |
| **Desired** | Add banner: superseded by `memberstack-current-setup-audit.md` for webhook behavior |
| **Risk** | **None** |
| **Minimal fix** | One-line note at top of older reports |

---

## Not required (confirm do not build)

| Item | Reason |
|------|--------|
| Memberstack plan per workspace | Audit proves gates ignore plan names |
| Owner-Operator Memberstack plan | Not used by code |
| Demo Memberstack plan | Demo = Airtable `Workspace Access: Demo` |
| Memberstack → Airtable Workspace Access write-back | Violates source-of-truth rule |
| Second Memberstack account per user | Explicitly out of scope |

---

## Operational checklist (no code)

1. **Memberstack:** One login plan; retire any unused per-role plans from *permission* workflows (can leave plans in dashboard if used for marketing, but app ignores them).
2. **Airtable:** For each user, link **Company Profile**; set **Workspace Access** on **company** (not Memberstack).
3. **Owner-Operator:** `Hotel Owner - Operator` + `Owner`, `Operator`.
4. **Demo:** `Demo` only (or `Demo` + production workspace as needed).
5. **Verify:** `/api/me` manual QA (see audit Part 4).
6. **Env:** `MEMBERSTACK_APPROVED_PLAN_IDS` = plan id(s) that mean “account approved” (Status only).

---

## Rollback

If webhook sync causes issues, revert `sync-member-to-airtable.js` to call `upsertSignupUserRecord` (**not recommended** — restores User Type overwrite). Forward fix is Airtable **Workspace Access** on Company Profile.

---

## Regression checklist (after any residual signup change)

- [ ] Webhook sync: WS and Company Profile link unchanged on member update  
- [ ] `/api/me` Owner-Operator: `["Owner","Operator"]`  
- [ ] `/api/me` Demo-only: `isDemo` true, `isAdmin` false  
- [ ] New signup: User Type set once; re-signup does not clear company-linked access  
- [ ] `node scripts/test-memberstack-airtable-source-of-truth.mjs`  
- [ ] `node scripts/test-company-workspace-access.mjs`

---

## Definition of done (identity-only model)

- [x] Memberstack authenticates; Airtable authorizes (runtime path)  
- [x] Webhook does not overwrite Workspace Access  
- [x] Company Profile link preserved on webhook update  
- [ ] Joan confirms single MS login plan in dashboard (operational)  
- [ ] Optional: signup path aligned with identity-only patch rules  
- [ ] Optional: Auth Role Hint column in Airtable Users  

**No further code changes are required** to confirm the identity-only model for production permission behavior.
