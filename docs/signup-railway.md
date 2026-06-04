# Signup on Railway (no Zapier)

Production signup is **`GET /signup`** → **`POST /api/signup`**. It replaces the Webflow + Zapier chain for onboarding while keeping **Memberstack** as the identity and email source of truth.

## Flow

1. User submits the form on `/signup`.
2. **Browser** calls Memberstack `signupMemberEmailPassword` (DOM) — this is what **sends the verification email** (same as Webflow signup).
3. **Railway** `POST /api/signup` with `memberstackId` from step 2:
   - **Does not** re-create the member when `memberstackId` is present.
   - Assigns **`MEMBERSTACK_SIGNUP_PENDING_PLAN_ID`** on new members (stagegate — no full platform plan until admin approves in Memberstack).
   - **Upserts** Airtable **Users** with real `mem_…` in **Unique Webflow ID**.
   - **Patches** Memberstack custom fields (**AirTable User ID**, **First Name**, etc.) using dashboard field IDs (see `MEMBERSTACK_CF_*` below).
   - Sends an **admin notification** email (SMTP) if configured.
3. **Memberstack** (dashboard templates, not Railway SMTP):
   - Email verification (“YOU'RE IN…” / confirm link).
   - Hello & welcome after verify (“under review”).
   - Access granted when admin assigns the approved plan.
4. **`POST /api/webhooks/memberstack`** (configure in Memberstack):
   - On member create/update → sync profile fields to Airtable (replaces **Zap B**).

Railway does **not** create Webflow CMS items (that was Zap A step 2). Webflow marketing pages can still link to `/signup` on your Railway host.

## Test Mode vs Production (critical)

Memberstack has two separate member databases. **Credentials must come from the same side.**

| Dashboard | Secret key | App ID |
|-----------|------------|--------|
| **Test Mode** (toggle ON) | `sk_sb_…` (sandbox) | App id shown while Test Mode is ON (DevTools / install snippet) |
| **Live / Production** (toggle OFF) | `sk_…` (not `sk_sb_`) | App id shown while in Live mode |

If `.env` has a **live** `sk_…` key but you browse **Test Mode** in the dashboard, signups still land in **Production** — you will not see the new member under Test Mode.

On `npm start`, the server logs e.g. `Memberstack Admin API: sandbox` or `live`. Fix `.env` before testing again.

**Wrong-environment test member:** delete or ignore `joandejarden@gmail.com` under **Live → Members** if it was created by mistake; re-test with sandbox keys + a new email.

## Environment variables

| Variable | Purpose |
|----------|---------|
| `MEMBERSTACK_SECRET_KEY` | `sk_sb_…` for local/Test Mode; `sk_…` for production deploy only. |
| `MEMBERSTACK_APP_ID` | Must match the same mode as the secret (Test vs Live). Required for DOM signup on `/signup` (verification emails). |
| `SIGNUP_USE_DOM_SIGNUP` | Default on — browser creates member via Memberstack DOM before `/api/signup`. Set `false` to use Admin API create only (emails may not send). |
| `SIGNUP_MEMBERSTACK_MODE` | `create` (default), `lookup`, or `off`. |
| `MEMBERSTACK_SIGNUP_PENDING_PLAN_ID` | `pln_…` assigned on signup (pending / no full access). |
| `MEMBERSTACK_SIGNUP_FREE_PLAN_ID` | **Do not** set on signup — assign in Memberstack when approving. |
| `MEMBERSTACK_APPROVED_PLAN_IDS` | Comma-separated; webhook sets Airtable **Status** when member has one of these plans. |
| `SIGNUP_AIRTABLE_STATUS_FIELD` | Exact Airtable column name for status (e.g. `Account Status`). **Omit** if your Users table has no status column. |
| `SIGNUP_AIRTABLE_PENDING_STATUS` | Value on create when status field is configured (default `Pending`). |
| `SIGNUP_AIRTABLE_SET_PENDING_STATUS` | Set `false` to skip pending status on create. |
| `SIGNUP_AIRTABLE_APPROVED_STATUS` | Default `Active` when approved plan detected on webhook. |
| `AIRTABLE_USERS_INACTIVE_STATUS_VALUES` | Add `pending` here to block `/app` until approved (optional; Memberstack plan also gates). |
| `SIGNUP_NOTIFY_EMAIL` or `SUPPORT_EMAIL` | Admin alert on new signup. |
| `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS` | Required for admin notify (and optional duplicate welcome). |
| `SIGNUP_SEND_WELCOME_EMAIL` | `true` only if you want Railway SMTP welcome **in addition to** Memberstack. Default: off. |
| `MEMBERSTACK_CF_*` | Optional overrides for Memberstack custom field **API keys** (defaults slugify dashboard names, e.g. `First Name` → `first-name`, `AirTable User ID` → `air-table-user-id`). If columns stay empty after signup, run `node scripts/inspect-memberstack-custom-fields.mjs --email <email>` and set env vars to match the keys printed on the member. |
| `MEMBERSTACK_WEBHOOK_SECRET` | Optional; verify webhook `x-memberstack-secret` / `x-webhook-secret`. |
| `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID` | Users upsert. |

## Memberstack webhook setup

1. Memberstack dashboard → Webhooks (or integrations).
2. URL: `https://<your-railway-host>/api/webhooks/memberstack`
3. Events: member created, member updated (names may vary by UI).
4. Set the same secret in `MEMBERSTACK_WEBHOOK_SECRET` if the dashboard provides one.

## Local test

```bash
# .env: MEMBERSTACK_SECRET_KEY, AIRTABLE_*, optional MEMBERSTACK_SIGNUP_PENDING_PLAN_ID
npm start
# Open http://localhost:3000/signup
```

Use a **sandbox** Memberstack key and test email. Confirm verification email in Memberstack logs; confirm Users row in Airtable with `mem_…` ID.

## Pilot path (optional QA)

- `GET /signup-pilot` → `POST /api/signup-pilot`
- Uses `SIGNUP_PILOT_MEMBERSTACK_MODE` (default `lookup`) instead of production `SIGNUP_MEMBERSTACK_MODE`.

See also: `docs/signup-railway-pilot.md`, `docs/zapier-railway-replacement-matrix.md`.

## “Back to Dealality” link

Signup and verify pages link to the public marketing site (default **[https://www.dealality.com/](https://www.dealality.com/)**), not `localhost`. Override with:

```env
DEALALITY_PUBLIC_HOME_URL=https://www.dealality.com
```

## Email verification URL (`/verify`)

Memberstack **Basic** plan redirects to slug **`verify`** after signup / verification. Railway must serve that page:

- Local: `http://localhost:8080/verify`
- Production: `https://<your-railway-host>/verify`

Set the same URL in Memberstack → **Plans → Basic → Redirects** (On Signup / On Verification Required) when testing on Railway. Use your Webflow `/verify` URL only when the live site is still on Webflow.

If verification links still 404, check **Memberstack → Settings → Site URL / domain** points at the host serving `/verify` (not an old Webflow path only).

### Blank page after “Confirm my email” (logo only)

If the URL looks like `https://dealality.com/verify?member={"verified":true}&forceRefetch=true` but you only see a centered logo and no “Your email is verified” message:

1. **Wrong host** — `dealality.com/verify` may be a **Webflow** page without `verify-page.js`. Memberstack must redirect to the **Railway** host that serves `public/verify.html` (same host as `/signup`), **or** add Webflow custom code that loads `/js/verify-page.js` from Railway.
2. **Deploy** — Ensure latest `server.js` includes `GET /verify` (not only static `verify.html`).
3. **Memberstack** — **Plans → Basic → Redirects** → On Verification / On Signup: use `https://<railway-host>/verify` while testing; update production when DNS/proxy routes `/verify` to Railway.
4. After deploy, hard-refresh the verify link; you should see **Your email is verified** and the pending-approval note.

## Disable Zapier

After Railway signup is verified in production:

1. Turn off Zap A (New Member → Webflow → Airtable) and Zap B (Member Updated → Airtable).
2. Point Webflow “Join” buttons to `https://<host>/signup`.
3. Keep Memberstack email templates and approval workflow unchanged.
