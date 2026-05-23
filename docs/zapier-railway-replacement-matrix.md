# Zapier → Dealality (GitHub / Railway) replacement matrix

**Purpose:** One-page checklist to decide whether each Zap is still required now that **deal-capture-proxy** on Railway exposes direct APIs and hosted pages for Webflow embeds.

**How to use:** For each row, open the Zap in Zapier and confirm the **exact trigger** and **action steps**. If production Webflow (or the Dealality app) already hits the listed replacement, you can pause the Zap and watch Airtable/Memberstack for gaps.

**Confidence legend**

| Tag | Meaning |
|-----|--------|
| **Likely** | Clear analogue in this repo (route + handler). |
| **Verify** | Depends on whether Webflow still fires the old trigger or now calls Railway. |
| **Unlikely** | No obvious server-side replacement found in repo (often Memberstack write-back or analytics-style events). |

**Signup (Webflow → many Zapier webhooks):** production Railway path in `docs/signup-railway.md` (`GET /signup`, `POST /api/signup`, `POST /api/webhooks/memberstack`). Optional QA: `docs/signup-railway-pilot.md` (`/signup-pilot`).

---

| Zap name (from Zap list) | Inferred trigger (confirm in Zapier) | Primary data targets | Replacement in this repo (Railway) | Confidence |
|--------------------------|--------------------------------------|----------------------|--------------------------------------|------------|
| Company Profile Info - Add & Update | Webflow form / flow | Company Profile table | `POST /api/company-profile`, `PATCH /api/company-profile/:recordId`, `GET /api/company-profile/prefill` — see `api/company-profile.js` | Likely |
| Hotel Ownership - Add - Webflow to AirTable | Webflow (form or CMS) | Deals / ownership fields | **Verify:** may overlap with `POST /api/my-deals`, `PATCH /api/my-deals/:recordId`, or Deal Setup flows in `api/my-deals.js` / `public/deal-setup*.html` | Verify |
| Update Deal - Webflow to AirTable | Webflow | Deals | `PATCH /api/my-deals/:recordId` (auth: Memberstack + Dealality user) — `server.js` + `api/my-deals.js` | Likely *if* Webflow no longer writes Airtable directly |
| Add Deal - Webflow to AirTable | Webflow | Deals + Users | `POST /api/intake/deal` (shared secret) **or** `POST /api/my-deals` (logged-in) — `api/intake-deal.js`, `api/my-deals.js` | Likely *if* intake secret / app path is what Webflow uses |
| Add Deal - Webflow to AirTable (TEST) | Test site | Deals (+ Sign?) | Same as above; Adobe Sign in Zap is **not** mirrored by a single obvious route here — treat as test-only | Verify |
| Deal Activated by a User | Webflow / Memberstack / Airtable automation | Deals (status?) | **Partial:** deal status lives in Airtable; app updates via `PATCH /api/my-deals/:recordId`. If Zap is driven by a **Webflow-only** signal Zapier still sees, keep until that signal calls Railway | Verify |
| Deal Archived by a User | Same | Deals | Same as row above | Verify |
| Deal Visited by a User | Webflow analytics / CMS / custom event | Deals / activity | No dedicated “visit” webhook found in `server.js`. **Outreach / activity:** `GET /api/outreach/deal-activity-log` reads data; it does not replace a Webflow→Airtable visit ping | Unlikely |
| Edit Brand | Webflow | Brand Library / Basics | `PATCH /api/brand-library/brand/:recordId` and related `.../sustainability-esg`, `.../brand-footprint`, etc. — `server.js` + `api/brand-library.js` | Likely |
| Brand Setup | Webflow | Brand setup tables | Hosted `GET /brand-setup` → `public/brand-setup.html` plus library PATCH routes above | Likely *if* embed uses this origin, not raw Webflow→Airtable |
| Brand Edit OLD | Webflow | Brands | Almost certainly duplicate of **Edit Brand**; safe to retire after confirming no unique steps | Verify |
| Hotel Owner Profile - Webflow to AirTable | Webflow | Users / Company Profile | Overlap: `POST /api/intake/user`, `POST/PATCH /api/company-profile`, `GET/POST/PATCH /api/user-management` | Verify |
| Hotel Brand Profile - Webflow to AirTable | Webflow | Brand-related rows | Brand Library reads/writes under `/api/brand-library/*` | Verify |
| Add Company User | Webflow / admin | Users (platform) | `POST /api/user-management` — `api/user-management.js` | Likely |
| Update Memberstack - Auto Update Airtable | Memberstack event | Airtable Users | Server **reads** Memberstack JWT (`middleware/memberstackAuth.js`) and loads Users from Airtable; no broad “Memberstack webhook → sync all fields” route surfaced in `server.js` | Unlikely |
| PAUSED — Company User Update - AirTable to Memberstack | Airtable | Memberstack member fields | Repo uses `admin.memberstack.com` for token verify (`lib/memberstack/verify-token.js`); **pushing Airtable changes into Memberstack** is not evident in API routes | Unlikely (while paused, document manual process) |

---

## Quick reference — high-traffic routes (from `server.js`)

| Concern | Methods | Path prefix |
|--------|---------|-------------|
| Company profile | GET prefill, POST create, PATCH update | `/api/company-profile` |
| Deal CRUD | GET list/detail, POST create, PATCH update | `/api/my-deals` |
| Legacy / embedded intake | POST | `/api/intake/deal`, `/api/intake/user` |
| Platform users (company) | GET/POST/PATCH/DELETE | `/api/user-management` |
| Brand records | GET + many PATCH segments | `/api/brand-library/brand/:recordId` |
| Third-party operator (operator setup) | POST (+ GET list/detail) | `/api/intake/third-party-operator`, `/api/third-party-operators/*` |

---

## Suggested validation before disabling a Zap

1. In Zapier, export or screenshot **trigger filter** and **every action** (field mapping).
2. In Webflow, confirm whether the trigger is still published or replaced by a **fetch** to your Railway `PUBLIC_URL`.
3. Pause Zap during low traffic; compare Airtable record history for missed creates/updates for 48–72 hours.

*Last generated from repo route survey; Zap names taken from your Zap list screenshot. Update triggers after confirming in Zapier.*
