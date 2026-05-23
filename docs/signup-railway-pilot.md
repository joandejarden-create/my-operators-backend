# Railway signup pilot (optional QA)

**Production** signup (Memberstack + Airtable, no Zapier) is documented in **`docs/signup-railway.md`** (`GET /signup`, `POST /api/signup`).

Use this **pilot** only for side-by-side testing while Webflow still uses Zapier:

| URL | Purpose |
|-----|--------|
| **`GET /signup-pilot`** | Same UI as `/signup`, with a visible banner. Submits to the pilot API only. |
| **`POST /api/signup-pilot`** | Same Airtable field mapping as `/api/signup`, plus optional Memberstack handling (see below). |

Production Webflow continues to post to Zapier until you deliberately change the Webflow “Send to” / custom action to your Railway `PUBLIC_URL`.

## How to test

1. Deploy this branch to Railway (or run locally with `.env` pointing at a **test** Airtable base if you prefer).
2. Open `https://<your-railway-host>/signup-pilot`.
3. Submit a **test** email you control. Confirm in Airtable:
   - Row created/updated on the Users table.
   - **Unique Webflow ID** is either a real `mem_…` (when Memberstack lookup links) or the placeholder `signup-pilot` (when no Memberstack match).
4. When behavior matches what your Zaps did, switch Webflow to call Railway (single endpoint) and retire the duplicate Zapier hooks.

## Environment variables

| Variable | Effect |
|----------|--------|
| `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID` | Required (same as rest of app). |
| `SIGNUP_PILOT_SEND_WELCOME_EMAIL` | Set to `false` to skip the welcome email during repeated QA (default: emails send like production). |
| `MEMBERSTACK_SECRET_KEY` | Required for any Memberstack Admin call. |
| `MEMBERSTACK_BASE_URL` | Optional; default `https://admin.memberstack.com`. |
| **`SIGNUP_PILOT_MEMBERSTACK_MODE`** | `off` — skip Memberstack. **`lookup`** (default) — `GET /members/:email`; if found, PATCH `firstName`/`lastName` custom fields and use `mem_…` as Airtable Unique ID. **`create`** — if no member, `POST /members` with a random password (member must use **forgot password** on the site; use only in sandbox). |
| `MEMBERSTACK_SIGNUP_FREE_PLAN_ID` | Optional `pln_…` included when **create** mode creates a member. |

## Replacing Webflow later

1. In Webflow, remove extra Zapier action URLs and use **one** destination: either native “POST to URL” to `POST /api/signup` (or `/api/signup-pilot` renamed once promoted), or a small script that `fetch`es Railway with the same JSON shape as `public/signup.html` (`firstName`, `lastName`, `companyName`, `title` optional / empty, `email`, `phone`, `companyType`, `reasonToJoin`, `howDidYouHear`).
2. Map Webflow field names to that JSON if they differ.
3. Pause Zaps and watch Airtable + Memberstack for 48–72 hours (see `docs/zapier-railway-replacement-matrix.md`).

## Files touched

- `public/signup.html` / `signup-pilot.html` — Dealality signup UI; hero background `68108c2a063eeb5d1bd7b095_sales-home-hero-bg-dashdark-webflow-template.svg` (Webflow CDN). Regenerate pilot from `signup.html` if the main form changes.
- `public/css/signup-deality-theme.css` — shared signup form typography and inputs aligned with `public/deal-setup.html` (linked after the Webflow bundle on signup pages).
- `public/assets/dealality-logo.png` — horizontal logo with alpha (black matte removed once via `npm run knockout-dealality-logo`). Original baked export is kept as `dealality-logo-baked-original.png` (created on first run only).
- `lib/signup-airtable-upsert.js` — shared Airtable upsert used by `/api/signup` and `/api/signup-pilot`.
- `lib/memberstack/signup-resolve-member.js` — optional Memberstack resolve for the pilot.
- `api/signup-pilot.js` — pilot handler.
