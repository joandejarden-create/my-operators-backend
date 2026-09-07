# Access boundary

| Layer | What it is | Can pull Enrich GA4? |
|-------|------------|----------------------|
| Enrich Labs Helena | Native Helena with historical `get_traffic`, Webflow, `post_linkedin` | **YES (must run there)** |
| This Cursor session | Dealality repo agent + Cursor MCPs | **NO** |
| Zapier | Fallback only | Do **not** use yet |

## Evidence Enrich is the native home
- Phase 0: Enrich Labs identity + Cursor Connected UI
- Skills: `post_linkedin`, Webflow publish, card generator
- GA4 snapshot notes explicitly cite Enrich `get_traffic`
- Workspace assets under `agent.enrichlabs.ai` / enrichlabs-public-assets

## Evidence this session cannot reach it
- No Enrich tools in Cursor dynamic tool catalog
- No ENRICH_* credentials in `.env`
- Phase 0/0F documented Enrich dispatch as unproven from Cursor
