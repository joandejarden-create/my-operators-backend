# Memberstack signup path alignment

**Date:** 2026-06-04  
**Model:** Memberstack = identity/login; Airtable = authorization/workspace access.

## Routes reviewed

| Route / function | File | Airtable write |
|----------------|------|----------------|
| `POST /api/signup` | `api/signup.js` | `upsertSignupUserRecord()` |
| `POST /api/signup-pilot` | `api/signup-pilot.js` | `upsertSignupUserRecord()` |
| Memberstack webhook sync | `lib/memberstack/sync-member-to-airtable.js` | `upsertMemberIdentityToAirtable()` (unchanged in this task) |

**Before:** `api/signup.js` duplicated inline upsert and set **`User Type`** from form `companyType` on every create/update. `lib/signup-airtable-upsert.js` did the same for pilot.

**After:** Both paths use `buildSignupUsersPatch()` — role is **Auth Role Hint** (or fill-blank **User Type** on create only when hint columns are absent). Re-signup does not overwrite authorization fields.

## Fields written (signup)

| Field | Create | Update (re-signup) |
|-------|--------|---------------------|
| Email, First/Last name, Unique Webflow ID | Yes | Yes |
| Company Name, Title, Phone, Reason, How did you hear | Yes | Fill-if-blank only |
| Auth Role Hint (and env aliases) | If form `role` / `companyType` | Yes when submitted |
| User Type | Only if blank and no Auth Role Hint columns in base | **Never** |
| Platform Role | Never from signup | **Never** |
| Workspace Access | **Never** | **Never** |
| Company Profile link | Only if empty | **Preserved** if linked |
| Company Type / Tags / Permission Level / Region / Deal Access | **Never** | **Never** |
| Status | Pending on create (if `SIGNUP_AIRTABLE_STATUS_FIELD` set) | Only if `options.statusOnWrite` |

Form aliases: `companyType`, `role` → `resolveSignupRoleHint()`.

## Authorization overwrite (before vs after)

| Scenario | Before | After |
|----------|--------|-------|
| Existing Owner, signup `companyType` = Operator | **User Type** overwritten | User Type unchanged; hint = Operator |
| Existing Demo WS, signup role Owner | User Type could change | WS unchanged; no Owner access from signup |
| New signup role Owner | User Type = Owner | Hint only; no Workspace Access |
| Unknown **Auth Role Hint** column | 422 possible | Field stripped; signup succeeds |

## Recommended fix (implemented)

1. Shared guards: `lib/airtable-users-protected-patch.js` (`USERS_PROTECTED_NEVER_WRITE`, `buildSignupUsersPatch`, `buildMemberstackSyncPatch`, `writeUsersRecordWithFieldFallback`).
2. `lib/signup-airtable-upsert.js` — identity upsert only; no **User Type** in raw payload.
3. `api/signup.js` — delegate to shared upsert; pass `memberstackId` from DOM signup when present.
4. `lib/memberstack/upsert-member-identity-to-airtable.js` — import shared patch helpers (no behavior change intended).

## Files changed

- `lib/airtable-users-protected-patch.js` (new)
- `lib/signup-airtable-upsert.js`
- `api/signup.js`
- `lib/memberstack/upsert-member-identity-to-airtable.js`
- `scripts/test-memberstack-signup-path-alignment.mjs` (new)
- `reports/memberstack-signup-path-alignment.md` (this file)

**Not modified:** `server.js` / `server.upload-ready.js` (route wiring unchanged), Memberstack plans, production Airtable data, workspace switcher, `/api/me` authorization logic.

## Tests run

```bash
node scripts/test-memberstack-signup-path-alignment.mjs
node scripts/test-memberstack-airtable-source-of-truth.mjs
```

Coverage:

1. Existing user re-signup — User Type / WS / Company Profile preserved; hint applied when configured.
2. Demo re-signup — Workspace Access not expanded from signup role.
3. New user — no Workspace Access; optional hint / fill-blank User Type.
4. Unknown Auth Role Hint — mock Airtable 422 → field stripped → create succeeds.

## Manual QA

1. Pick an existing Airtable User with **Workspace Access** and **Company Profile** set.
2. Submit signup/onboarding again (same email) with a different `companyType` / `role`.
3. Confirm **Workspace Access** unchanged in Airtable.
4. Confirm **Company Profile** link unchanged.
5. Confirm **User Type** / **Platform Role** not overwritten; **Auth Role Hint** updated if column exists.
6. Log in and call **`GET /api/me`** — `dealality.workspaceAccess` still from Airtable/company merge, not signup form.

## Data contract snapshot

| Item | Value |
|------|--------|
| Table | Users `tbl6shiyz2wdUqE5F` |
| Mapping | `USERS_SIGNUP` in `lib/signup-airtable-upsert.js` |
| Hint fields | `AIRTABLE_USERS_AUTH_ROLE_HINT_FIELDS` (default: Auth Role Hint, …) |
| Required | Email |
| Authorization source | Company Profile **Workspace Access** + `/api/me` gates |

**Change impact:** **High** (Airtable Users writes on signup) — narrow scope, preserve-on-update semantics. Rollback: revert `lib/signup-airtable-upsert.js` and `api/signup.js` to prior inline **User Type** write (not recommended).

## Regression risks

- Signup without **Auth Role Hint** column still sets **User Type** on **create** only (compatibility).
- Pending **Status** on create unchanged when env configured.
- Pilot path unchanged except shared upsert behavior.
