# Dealality Deployment and Webflow Embed Runbook

## 1) Environment Strategy

Dealality uses three execution environments with isolated credentials and data scopes:

- **Local**
  - Developer machine for feature work and QA.
  - Should use non-production credentials and bases.
- **Railway Staging**
  - Shared pre-production validation environment.
  - Mirrors production topology but with staging data/keys.
- **Railway Production**
  - Live user-facing environment.
  - Must use production-only credentials and production data bases.

## 2) Airtable PAT Strategy

- Use **staging Airtable PAT only** in:
  - Local
  - Railway Staging
- Use **production Airtable PAT only** in:
  - Railway Production
- In code, Airtable PAT is referenced via the generic env var:
  - `AIRTABLE_API_KEY`
- Base IDs are environment-specific and must differ by environment:
  - `AIRTABLE_BASE_ID`
  - `AIRTABLE_BASE_ID_ALT`

## 3) `AIRTABLE_BASE_ID` vs `AIRTABLE_BASE_ID_ALT`

- **`AIRTABLE_BASE_ID`**:
  - Primary Airtable base for core deal/workflow APIs and most operational endpoints.
- **`AIRTABLE_BASE_ID_ALT`**:
  - Alternate Airtable base used by:
    - Radar-related APIs
    - Clause Library APIs
    - Financial Term Library APIs
    - Related intelligence/reference endpoints

## 4) Webflow Embed Rule

- For Webflow embeds, use **direct page URLs** with `?embed=1`, for example:
  - `/some-page.html?embed=1`
- Do **not** use `/app#...` routes for Webflow embeds unless the goal is to load the **full app shell** (left nav + app routing).

## 5) Staging Direct Embed Test URLs

Use your staging host and append these paths:

- `/partner-directory.html?embed=1`
- `/deal-capture-radar-with-ranked-list.html?embed=1`
- `/brand-library-atelier-north.html?embed=1`
- `/my-deals.html?embed=1`
- `/new-deal-setup.html?embed=1`
- `/deal-summary.html?embed=1`
- `/clause-library.html?embed=1`
- `/financial-term-library.html?embed=1`

Example format:

- `https://<staging-domain>/partner-directory.html?embed=1`

## 6) Full App Shell Test URLs

Use your target host (staging or production) with hash routes:

- `/app#/home`
- `/app#/my-deals`
- `/app#/partner-directory`
- `/app#/opportunity-radar`

Example format:

- `https://<staging-domain>/app#/home`

## 7) Memberstack Testing Checklist

Validate each key journey and role-scoped visibility for:

- **Logged out**
- **Owner**
- **Brand**
- **Operator**
- **Admin**

Recommended checks per role:

- Route access and redirects
- Left-nav visibility and labels
- Page-level role gating
- API-backed data visibility

## 8) Railway Env Var Checklist

Set and validate per Railway environment:

- `AIRTABLE_API_KEY`
- `AIRTABLE_BASE_ID`
- `AIRTABLE_BASE_ID_ALT`
- Memberstack keys (if used in that environment)
- OpenAI/API keys used by readiness or AI features

Operational note:

- Confirm staging Railway service has staging PAT/base IDs.
- Confirm production Railway service has production PAT/base IDs.
- Do not reuse production credentials in staging.

## 9) Security Warning

- **Never** place Airtable PATs in Webflow settings, client-side scripts, or any frontend file.
- Airtable PATs must remain server-side in environment variables only.

