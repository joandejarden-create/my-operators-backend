# Helena existing metrics inventory

| Metric pack | Source | Freshness | Used by CMO? | Why / why not |
|-------------|--------|-----------|--------------|---------------|
| GA4 snapshot | phase-0 export | STALE | Yes (6D/6E) | Only traffic grain available |
| GSC snapshot | phase-0 export | STALE | Yes | Only search grain |
| LinkedIn scrape | phase-0 | STALE | Yes | Engagement proxy |
| Webflow Insights catalog export | phase-0 | STALE | Partial | Superseded by live CMS |
| Marketing OS Performance | Airtable live | LIVE | Partial | Counts known; not full CMO panel |
| Pilot Target List | GTM live | LIVE | **No (6E)** | **Incorrectly treated as DATA_GAP** |
| Acquisition Network | GTM live | LIVE | No | Ignored |
| Monday Performance Digests | Enrich history | UNKNOWN | No | Missing external Helena sources |
| Webflow Analyze | MCP | PERMISSION_LIMITED | No | Entitlement |
