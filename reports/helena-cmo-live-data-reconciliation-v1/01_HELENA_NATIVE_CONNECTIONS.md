# Helena native connections inventory

| Source | Native? | Auth | Live read | Freshness |
|--------|---------|------|-----------|-----------|
| GA4 | PARTIAL | NO | LIVE_READ_FAILED | STALE_SNAPSHOT_ONLY |
| GSC | NO | NO | STALE_ONLY | STALE_SNAPSHOT_ONLY |
| LINKEDIN_JOAN | NO | NO | STALE_ONLY | STALE_SCRAPE |
| LINKEDIN_DEALALITY | NO | UNKNOWN | STALE_ONLY | STALE_SCRAPE |
| LINKEDIN_AO | NO | UNKNOWN | STALE_ONLY | STALE_SCRAPE |
| WEBFLOW_CMS | YES | YES | LIVE_READ_SUCCESS | LIVE |
| WEBFLOW_ANALYZE | YES | YES | PERMISSION_LIMITED | PERMISSION_LIMITED |
| PUBLIC_SITE_HTML | YES | N/A | LIVE_READ_SUCCESS | LIVE |
| AIRTABLE_GTM | YES | YES | LIVE_READ_SUCCESS | LIVE |
| AIRTABLE_MARKETING_OS | YES | YES | LIVE_READ_SUCCESS | LIVE |
| ZAPIER_AIRTABLE | YES | YES | LIVE_READ_SUCCESS | — |
| ZAPIER_WEBFLOW | YES | YES | LIVE_READ_SUCCESS | — |
| ZAPIER_MEMBERSTACK | YES | YES | PARTIAL | — |
| ENRICH_LABS | UNKNOWN_FROM_THIS_SESSION | UNKNOWN | LIVE_READ_FAILED | — |
| LANDING_EVENTS | PARTIAL | N/A | LIVE_READ_FAILED | UNKNOWN_IN_LOCAL_ENV |
| PRODUCT_USAGE_TELEMETRY | NO | N/A | LIVE_READ_FAILED | — |

## Environments distinguished
- **Cursor session (this agent):** Webflow MCP, Zapier (Airtable/Webflow/Memberstack + GA4 pending auth), Dealality Airtable PAT
- **Dealality repo code:** no GA4/GSC/LinkedIn API clients
- **Enrich Labs:** historically intended Helena runtime — **not invocable here**
