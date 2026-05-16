# Dealality demo login (`dealalitydemo@dealality.com`)

## Airtable (backend data)

Users row **`rec32VycDpnAp3OmP`** (Joan Dejarden, Hotel Owner) must have:

- **Email:** `dealalitydemo@dealality.com`
- **Platform Role / User Type:** Owner (e.g. `Hotel Owner`)
- **Unique Webflow ID** (`flddTfp7oLdcPwBIC`) and **Slug** (`fldEgbHu5MvfyrxgE`) must equal the **same** `mem_sb_…` as the logged-in Memberstack session (not an old id).
- Railway env: `AIRTABLE_ME_USERS_MEMBERSTACK_FIELDS=flddTfp7oLdcPwBIC,fldEgbHu5MvfyrxgE` (do **not** use lowercase `slug` for writes — Airtable field name is **Slug**).

Verify locally:

```bash
node scripts/verify-demo-user-setup.mjs
```

Manual link if needed:

```bash
node scripts/link-airtable-user-memberstack.mjs --email dealalitydemo@dealality.com --memberstack-id mem_sb_XXXXX
```

## Console: `No Member has logged in` (login:128)

This comes from **Webflow custom code**, not Railway. It means **`$memberstackDom.getCurrentMember()` returned no `data`** when your script ran.

Common causes:

1. **Webflow Designer preview** — Memberstack cookies/sessions often do not work in the designer. Use the **published** site URL.
2. **Script runs too early** — on the login page or right after redirect, before Memberstack finishes init. Use `DealalityMemberstackAuth.waitForLoggedInMember()` or load `dealality-webflow-me-bootstrap.js`.
3. **Not actually logged in** — complete the Memberstack login form on the live site; check Memberstack dashboard that `dealalitydemo@dealality.com` exists in the **same app** wired to Webflow.
4. **Wrong API host** — set before other scripts:

```html
<script>
  window.DEALALITY_API_BASE = 'https://my-operators-backend-staging.up.railway.app';
</script>
<script src="https://my-operators-backend-staging.up.railway.app/js/dealality-memberstack-auth.js"></script>
<script src="https://my-operators-backend-staging.up.railway.app/js/dealality-webflow-me-bootstrap.js"></script>
```

Replace with production Railway URL when testing production.

## Webflow login page pattern (replace early `getCurrentMember` check)

```javascript
(async function () {
  var auth = window.DealalityMemberstackAuth;
  if (!auth) return;
  var member = await auth.waitForLoggedInMember(20000);
  if (!member || !member.data) {
    console.warn('No Member has logged in');
    return; // or open login modal: await window.$memberstackDom.openModal('login')
  }
  // Now safe to call /api/me or redirect to dashboard
})();
```

## “No brands assigned” (blue Information toast)

`/api/me` returns **200** but `permissions.allowedBrandNames` is empty. That is **normal for Hotel Owner** demo accounts (no Brand Basics link on the Users row).

Webflow should **not** show “No brands assigned” when `dealality.isOwner === true` (included in `/api/me` as of role payload). Owners use **My Deals**, not brand allow-lists.

## JWT in the browser console

Webflow may load **Memberstack v1** (`v1.js`). On that build, **`getToken` is not a function** on `$memberstackDom`.

Use the shared helper (scans clients, member object, cookies, storage):

```javascript
await DealalityMemberstackAuth.inspectMemberstackAuth();
// → { hasJwt: true/false, apis: [...], jwtPreview: 'eyJ…' }

const token = await DealalityMemberstackAuth.getMemberstackJwt();
```

If `hasJwt` is false but `/api/me` still returns 200 in `loadUserContext`, your Webflow script is obtaining the JWT another way — search `dashboard` custom code for `Authorization` / `Bearer` / `eyJ` and reuse that same call for My Deals.

Do **not** use `mem_sb_…` as Bearer (API returns 401).

### Hide “No brands assigned” for owners

After `/api/me` 200, check:

```javascript
if (userContext.dealality && userContext.dealality.isOwner) {
  // skip brand-assignment toast
}
```

## Toast: “Your account isn't set up in our system yet”

Shown when **`GET /api/me`** returns **`user_not_found`** (404) or **`user_not_found`** (403 on `/api/auth/me`). After Airtable email is fixed and Memberstack session exists, `/api/me` should return **200**.

In DevTools → Network, confirm:

- Request URL is `https://my-operators-backend-staging.up.railway.app/api/me` (not `webflow.com/api/me`)
- Header `Authorization: Bearer eyJ…` (JWT, not `mem_sb_…`)

## CSP / `editor-ep` errors

`frame-ancestors webflow.com` and `editor-ep.cecb652b` errors are **Webflow Designer** noise. Ignore for auth testing; use published staging/production URLs.
