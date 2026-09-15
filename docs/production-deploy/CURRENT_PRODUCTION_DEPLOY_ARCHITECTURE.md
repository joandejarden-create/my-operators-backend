# Current production deploy architecture

Audited facts for live Dealality web backend. Do not treat this as a full Railway dashboard dump — only what was verified.

## Railway target

| Field | Value |
|-------|--------|
| Project | `serene-reverence` (`48bd3650-29f9-4db5-a357-293646f6a8b0`) |
| Service | `my-operators-backend` (`bff4ff32-9c78-406b-a212-6ddedee5be8e`) |
| Environment | `production` (`384c052e-81ed-4608-a550-1a7083e3e88f`) |
| Public URL | https://my-operators-backend-production.up.railway.app |
| Audited deployment ID | `2222ebdc` (hotfix: restore Golden Four HI share after ADP wipe) |

## Deploy method (historical + current short-term)

- **Method:** manual CLI `railway up` (whole working-tree upload).
- **Not historically:** GitHub auto-deploy from `main`.
- **Implication:** whatever tree is uploaded **replaces the entire service filesystem**. An ADP-only or HI-only checkout can wipe peer product surfaces.

## Repo Railway config (verified in tree)

Present:

- `railway.toml` — builder `RAILPACK`; `startCommand = "node server.js"`; `healthcheckPath = ""` (empty / no path); restart on failure (max retries 10).
- `.railwayignore` — lean excludes for `reports/*`, `data/*` with ADP/BAI allowlists; does **not** exclude `public/` or CSS.

Absent (audited):

- No `railway.json`
- No `Procfile`
- No `Dockerfile`
- No `.dockerignore`

## Runtime serving model

- Express `express.static` serves `public/`.
- `server.js` also registers many explicit `sendFile` routes for product HTML entrypoints.
- Customer-facing share/HTML/JS/CSS therefore live or die with the uploaded tree — ignore rules and incomplete checkouts are production-critical.

## Policy direction

- **Short-term:** keep CLI deploy, but only through hard-guarded `npm run deploy:production` (see `PRODUCTION_DEPLOY_GUARD.md`).
- **After PR #40 merges and Railway GitHub deploy can target `main` safely:** move to main-driven auto-deploy.
