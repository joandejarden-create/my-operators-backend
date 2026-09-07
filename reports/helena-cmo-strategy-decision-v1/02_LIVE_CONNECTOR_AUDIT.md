# Live connector audit (READ)

| Source | Status |
|--------|--------|
| GA4 live API | NOT_CONNECTED |
| GSC live API | NOT_CONNECTED |
| LinkedIn analytics API | NOT_CONNECTED |
| Webflow CMS API | NOT_CONNECTED |
| Public website HTML | CONNECTED_AND_WORKING |
| Stale analytics snapshots | CONNECTED_AND_WORKING |
| Marketing OS Airtable | CONNECTED_BUT_NOT_WIRED_TO_HELENA |
| GTM Pilot Target List | CONNECTED_BUT_NOT_WIRED_TO_HELENA |
| GTM Owner Targets | CONNECTED_BUT_NOT_WIRED_TO_HELENA |
| Acquisition Network | CONNECTED_BUT_NOT_WIRED_TO_HELENA |
| Landing events | CONNECTED_BUT_NOT_WIRED_TO_HELENA |
| Email/outreach attribution | NOT_CONNECTED |

## Wired this phase (READ)
- `lib/helena-cmo/analytics/cmo-analytics-reader.js` — snapshot → CMO analytics contract
- Live public HTML inventory script (no CMS token)
- Strategy pack consumes both

## Not created
New Google/LinkedIn/Webflow credentials. No writes.
