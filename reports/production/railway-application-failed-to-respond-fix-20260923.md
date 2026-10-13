# Railway “Application failed to respond” — root cause & fix

**Date:** 2026-09-23  
**Symptom:** Clients clicking some Dealality live pages see Railway edge page: *Application failed to respond* (Request ID example: `HxQ6Ta-dR-WttOL-8u2xcg`).

## Root cause (ranked)

1. **Primary — upstream hang past Railway edge timeout**  
   Cold GDI / decision-outcomes Airtable reads used the SDK default **`requestTimeout: 300_000` ms** and had **no app-level timeout**. Railway’s proxy gives up much sooner → edge HTML error even though Node may still be waiting.

2. **Secondary — empty HTTP healthcheck**  
   `railway.toml` had `healthcheckPath = ""`, so Railway did not probe `/health` during deploy/restart windows.

3. **Latent — dirty ESM boot bombs**  
   Deploying with `index.js` exporting from a missing/untracked module crashes the process on boot → every request fails.

## Fixes shipped (this change)

| Fix | File(s) |
|-----|---------|
| HTTP healthcheck `/health` | `railway.toml` |
| Airtable `requestTimeout` ~20s (env `AIRTABLE_REQUEST_TIMEOUT_MS`) | `lib/group-demand-intelligence/airtable-opportunity-store.js`, `lib/decision-outcomes/airtable-store.js` |
| App-level `withTimeout` (~15s) on GDI list/detail/summary loads; progression degrades instead of hanging | `lib/http/with-timeout.js`, `api/group-demand-intelligence.js` |
| Predeploy ESM import smoke | `scripts/smoke-esm-imports.mjs`, wired into `deploy:production` |
| Postdeploy smoke includes `/health` + 15s abort | `scripts/test-production-critical-surfaces.mjs` |

## How to prevent recurrence

1. **Always** deploy with `npm run deploy:production` (never raw `railway up`).
2. Do not deploy a dirty tree that references untracked modules under `lib/group-demand-intelligence/`.
3. Confirm postdeploy smoke PASS (now requires `/health` 200 within 15s).
4. Optional ops envs:
   - `AIRTABLE_REQUEST_TIMEOUT_MS=20000`
   - `GDI_READ_TIMEOUT_MS=15000`

## Deploy this fix

```bash
# preferred when on main:
npm run deploy:production

# hotfix from this branch:
DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production
```

(Only use dirty override if you intentionally include uncommitted files; prefer committing first.)
