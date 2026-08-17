# AH Commercial Performance Hub — standalone Railway deploy

**Separate from Dealality.** This folder is a minimal Node static host for the AH Hospitality Advisors mockup only.

## One-time Railway setup

1. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo** (this repo).
2. In the new service **Settings**:
   - **Root Directory:** `engagements/ah-hospitality-advisors/railway`
   - **Start Command:** `npm start` (default if Railway detects `package.json`)
3. **Variables:** none required (`PORT` is set by Railway).
4. **Networking** → **Generate Domain** → e.g. `ah-commercial-hub-production.up.railway.app`
5. Optional: add a custom domain (e.g. `hub.ahhospitalityadvisors.com`) under **Custom Domain**.

Result: always-on URL like `https://<your-railway-domain>/` — no local server, no tunnel.

## Before each deploy (when mockup changes)

From this folder:

```bash
npm run sync    # copies latest HTML/JS from parent engagement folder
npm install     # first time only
```

Commit `static/` + any `server.js` changes, push — Railway redeploys automatically.

## Local smoke test

```bash
npm run sync
npm install
npm start
# open http://localhost:8080
```

## What gets deployed

| Path | Purpose |
|------|---------|
| `server.js` | Tiny Express static host + `/health` |
| `static/` | Mockup (`index.html` + JS) |
| `package.json` | Start script only — no Airtable, no Dealality deps |

## Cost

Railway Hobby plan: one extra service is typically a few dollars/month for a low-traffic static mockup. No database, no secrets, no env vars.
