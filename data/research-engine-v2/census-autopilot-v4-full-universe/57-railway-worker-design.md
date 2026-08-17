# Railway Census V4 Worker design

## Service

**Name:** Dealality Census Worker (separate from web)

**Start command:**

```bash
npm run census:v4-full-build-worker
```

(`package.json` → `node scripts/v4-full-build-supervisor.mjs`)

**Config file in repo:** `railway.census-v4-worker.toml`

**Restart:** `ON_FAILURE` with high max retries (Railway restarts crashed supervisor).

## Env (Railway service variables)

- `ENABLE_CENSUS_V4_WORKER=1`
- `ENABLE_CENSUS_AUTOPILOT_V4=1`
- `ENABLE_VERIFIED_CENSUS_WRITES=1`
- Airtable production credentials (same as Census)
- Optional `SERPAPI_KEY`

Do **not** set `ENABLE_CENSUS_V4_WORKER=1` on the web service.

## Volume

Mount persistent volume at:

`/app/data/research-engine-v2/census-autopilot-v4-full-universe`

for lock, heartbeat, checkpoints across deploys.

## Deploy behavior

1. Railway sends SIGTERM → supervisor releases lease, checkpoint already on disk/volume  
2. New instance starts → acquires lease (or waits if overlap) → resumes from ticket  
3. Single-writer lock prevents duplicate inserts

## Current blocker in this workspace

`railway status` → **No linked project**. Joan must `railway link` and create the worker service (or add service in Railway UI) using the start command above.
