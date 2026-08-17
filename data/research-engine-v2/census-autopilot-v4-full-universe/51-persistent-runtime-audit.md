# Persistent runtime audit — why V4 kept stopping

## PRIMARY ANSWER

**Is `census:v4-full-build-supervisor` attached to a persistent production process?**

### **NO** (before this fix)

### Why

1. **Supervisor was one-shot** (`scripts/v4-full-build-supervisor.mjs` pre-fix): spawn controller once → `child.on("exit", () => process.exit(code))` → supervisor dies with controller.
2. **No Railway worker service** owned this command. Repo `start` is `node server.js` (web). Railway CLI: **no linked project** in this workspace. No Procfile. No dedicated Census worker deploy config existed.
3. **Cursor/agent terminal** ran controller/supervisor ad hoc. When the session/process ended, nothing restarted them.
4. Application-level “AUTO-RESUME = OPERATIONAL” only meant a **resume ticket file** existed — not that infrastructure kept a process alive.

## Current Dealality production process map

| Component | Role | Persistent? |
| --- | --- | --- |
| `npm start` → `server.js` | Web/API | Yes (Railway web service, assumed) |
| Market alerts cron routes | HTTP cron hooks | Request-scoped |
| `census:v4-full-build-supervisor` (old) | One controller spawn | **No** |
| `census:v4-full-build-worker` (new) | Persistent supervisor loop | **Yes when deployed / running** |

## Infrastructure gap (exact)

Nothing in Railway/process manager was configured to run:

`ENABLE_CENSUS_V4_WORKER=1 npm run census:v4-full-build-worker`

continuously. Therefore Joan (or Cursor) had to re-issue npm — which feels like “V4 keeps stopping.”

## Fix delivered

- Persistent supervisor loop + lease + heartbeat
- Controller exit contract (0/10/20/21/30/40)
- `railway.census-v4-worker.toml` + npm `census:v4-full-build-worker`
- `.railwayignore` allowlist for V4 data paths
- Env flags in `.env.example`

**Railway deploy still requires Joan to create/link the worker service** (CLI reports no linked project here). Until that service is live, laptop-independent persistence is **PARTIAL**.
