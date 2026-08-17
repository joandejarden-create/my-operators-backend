# Dealality demo login (`dealalitydemo@dealality.com`)

## Airtable (backend data)

Users row **`rec32VycDpnAp3OmP`** (Joan Dejarden, Hotel Owner) must have:

- **Email:** `dealalitydemo@dealality.com`
- **Platform Role / User Type:** Owner (e.g. `Hotel Owner`)
- **Unique Webflow ID** (`flddTfp7oLdcPwBIC`) and **Slug** (`fldEgbHu5MvfyrxgE`) must equal the **Live** Memberstack id on **`dealality.com`** (e.g. `mem_cmq0460yc28rv0slkartj6fif`). Do **not** overwrite with localhost sandbox `mem_sb_…` ids.
- **Localhost (`npm start`):** Memberstack **does not allow localhost in Application Domains** (Live or Test) — skip that step entirely. Local login always uses Test Mode (`mem_sb_…` ids). Keep the **Live** `MEMBERSTACK_APP_ID` + live `MEMBERSTACK_SECRET_KEY` (`sk_…`) for dealality.com parity; add `MEMBERSTACK_TEST_SECRET_KEY=sk_sb_…` so `/api/me` can resolve sandbox sessions by email **without** overwriting the Live id in Airtable. For live `mem_cmq…` ids on localhost, log in on **dealality.com** and open `http://localhost:8080/app?msToken=<eyJ…>`.
- Railway env: `AIRTABLE_ME_USERS_MEMBERSTACK_FIELDS=flddTfp7oLdcPwBIC,fldEgbHu5MvfyrxgE` (do **not** use lowercase `slug` for writes — Airtable field name is **Slug**).

Verify locally:

```bash
node scripts/verify-demo-user-setup.mjs
```

Manual link if needed:

```bash
node scripts/link-airtable-user-memberstack.mjs --email dealalitydemo@dealality.com --memberstack-id mem_cmq0460yc28rv0slkartj6fif
```

## Brand demo (`dealalitydemobrand@dealality.com`)

Users row **`rec0w3LKN2PI8PtPw`** must have:

- **Email:** `dealalitydemobrand@dealality.com`
- **User Type:** `Hotel Brand` (live Users select option; Platform Role is not on this base)
- **Company Profile:** Dealality Brand Demo `reciQEtqmxz6ZroVc` (Company Type `Hotel Brands (Franchise)`, Workspace Access `Brand`)
- **Unique Webflow ID** and **Slug:** Live Memberstack id (`mem_…`, never `mem_sb_…`)
- Memberstack Live plan: Hotel Brands. Production nav: Hotel Brands Sidebar → Market Intelligence → **Brand AI Visibility** → `/brand/ai-visibility` (embeds staging `/ai-visibility-brand.html?embed=1`).

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

## Basic plan / pending approval (signup waiting for admin)

New signups on **Basic** have no workspace access yet. `/api/me` includes:

```json
"accountAccess": {
  "state": "pending_approval",
  "pendingApproval": true,
  "userTitle": "Account pending approval",
  "userMessage": "Thanks for signing up…",
  "suppressBrandAssignmentToast": true
}
```

**Webflow footer:** load `dealality-webflow-account-notice.js` after `dealality-webflow-me-bootstrap.js`. It shows a banner and sets `window.__dealalitySuppressBrandToast = true`.

In existing `loadUserContext` toast logic, skip the brand toast when:

```javascript
if (window.__dealalitySuppressBrandToast || (data.accountAccess && data.accountAccess.pendingApproval)) {
  return; // pending signup — do not show "No brands assigned"
}
```

## “No brands assigned” (blue Information toast)

`/api/me` returns **200** but `permissions.allowedBrandNames` is empty. That is **normal for Hotel Owner** demo accounts (no Brand Basics link on the Users row).

Webflow should **not** show “No brands assigned” when `dealality.isOwner === true` (included in `/api/me` as of role payload). Owners use **My Deals**, not brand allow-lists.

Also skip when `dealality.isBrand === true` or `dealality.canAccessBrandWorkspace === true`. Brand entitlements live on Company Profile **Brands You Operate / Support**, which does **not** populate `permissions.allowedBrandNames` (Users → Brand Basics). An empty allow-list is normal for Hotel Brand demo accounts.

Also skip for **pending approval** signups (`accountAccess.pendingApproval === true`) — they are not brand users yet.

If the console shows `/api/me` **200** then `Request failed: … jquery.toast … toLowerCase`, that is the “No brands assigned” toast crashing — not an auth failure. Staging must serve current `dealality-webflow-account-notice.js` (preserves `$.toast.options` and sets `__dealalitySuppressBrandToast` for brand/owner/platform pages).

### `window.__dealalityUserContext.dealality` is `undefined`

The footer only skips the toast when `dealality.isOwner` / `dealality.isAdmin` exist. If `dealality` is missing:

1. **Staging/production Railway** may be running an older build (no `dealality` key in JSON). Deploy current `api/me.js` and retry.
2. In the console, run `Object.keys(window.__dealalityUserContext || {})` — you should see `dealality`, `permissions`, `airtable`, etc.

### Role lives on Company Profile, not Users

If Users has no **User Type** / **Platform Role**, `/api/me` resolves role from the linked **Company Profile → Company Type** (e.g. `Hotel Owner` → `isOwner: true`). Ensure Justin’s Users row links **Company Profile** and that company’s **Company Type** is set.

## Local app shell (`http://localhost:3000/app#/my-deals`)

The app shell loads Memberstack on the parent page and passes the session JWT into embedded pages (My Deals, Deal Setup, etc.) via `msToken` and `postMessage`.

1. Set in `.env`: `MEMBERSTACK_APP_ID`, `MEMBERSTACK_SECRET_KEY`, plus Airtable keys.
2. Restart the server (`npm start`).
3. Open `http://localhost:3000/app#/my-deals`.
4. Click **Log in** in the shell (first visit on localhost) and sign in with your Dealality Memberstack account, then **Reload after login**.

Alternatively, append a token from a published site session:

`http://localhost:3000/app?msToken=eyJ…#/my-deals`

Debug in the shell console:

```javascript
await DealalityMemberstackAuth.inspectMemberstackAuth();
await DealalityAppShellAuth.whenReady();
```

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
