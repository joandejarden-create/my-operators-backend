# DEPLOYMENT_QA

## Merge
- PR: https://github.com/joandejarden-create/my-operators-backend/pull/310
- Merge commit / DEPLOYED ARTIFACT SHA: `8fb85ad0c9c1cc38ef9c40d77b8ea5bc85d82bb1`
- CI on PR: all SUCCESS

## Canonical deploy
- Workflow: Dealality production release
- Product: ADP
- dry_run: false
- GHA: https://github.com/joandejarden-create/my-operators-backend/actions/runs/43933596552 → **SUCCESS**
- Railway deployment: `5f3f4313-c56d-4014-b867-1a65b5438bb4` (Online)
- PUBLIC ROUTE HEALTH: `/health` 200; `/owner-ai-demand-share.html` 200
- Note: `/health` `buildSha` remains Railway GitHub-linked SHA (`ffc15db0…`); CLI `railway up` does not rewrite that env. Content probes confirm Toronto published pack + share registry from artifact SHA.

## Production share
- Prod-HMAC share resolve: **200**
- Report API: **200** with Core `c8bd8b` + Choice Privileges / `308ba1`

## Follow-up (evidence/PDF lean allowlists)
`.railwayignore` updated to ship Toronto Core+Loyalty runtime periods, fixture profile, monthly-review pack, and current-report-pdf for evidence drawers + PDF static access. Requires follow-up PR + redeploy if production evidence still 404.
