# Dealality demo login (`dealalitydemo@dealality.com`)

## Airtable (backend data)

Users row **`rec32VycDpnAp3OmP`** (Joan Dejarden, Hotel Owner) must have:

- **Email:** `dealalitydemo@dealality.com`
- **Platform Role / User Type:** Owner (e.g. `Hotel Owner`)
- After first successful `/api/me`: **Unique Webflow ID** and **slug** = `mem_sb_…` (auto-sync from commit `0c53bc7`)

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

## Toast: “Your account isn't set up in our system yet”

Shown when **`GET /api/me`** returns **`user_not_found`** (404) or **`user_not_found`** (403 on `/api/auth/me`). After Airtable email is fixed and Memberstack session exists, `/api/me` should return **200**.

In DevTools → Network, confirm:

- Request URL is `https://my-operators-backend-staging.up.railway.app/api/me` (not `webflow.com/api/me`)
- Header `Authorization: Bearer eyJ…` (JWT, not `mem_sb_…`)

## CSP / `editor-ep` errors

`frame-ancestors webflow.com` and `editor-ep.cecb652b` errors are **Webflow Designer** noise. Ignore for auth testing; use published staging/production URLs.
