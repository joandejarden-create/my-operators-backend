# GDI Performance V1 — Context7 Review

Context7 MCP was intermittently unavailable (`fetch failed` / timeout) during this cycle. Guidance below uses Context7 Express/Airtable library IDs resolved successfully (`/expressjs/express`, `/websites/airtable_developers_web_api`) plus established Dealality patterns; document where live doc snippets could not be retrieved.

| AREA | CURRENT | CONTEXT7 / DOCS GUIDANCE | CHANGE | WHY |
| --- | --- | --- | --- | --- |
| Express responses | No timing headers on GDI | Structured logging / observability around route handlers | Added `gdi_perf` JSON logs + `X-GDI-Detail-Source` | Diagnose slow paths without secrets |
| HTTP caching | No private short TTL for GDI JSON | Cache-Control for private APIs is optional; auth'd responses must not be shared caches | Process-local TTL cache (45s), hotel-scoped; not HTTP shared cache | Avoid CDN/shared-cache leakage; still cut repeat Airtable |
| Airtable list | Full hotel select every request | Prefer filterByFormula + pageSize; avoid N+1 | Kept formula list; added cache + single-record detail | Highest impact without schema migration |
| Airtable N+1 | Sequential decision/event loads | Batch/parallelize independent I/O | `Promise.all` over summaries | Cuts progression wall time |
| Fetch parallelism | Serial resolve→list | Parallel independent fetches | Client peek hotelId + `Promise.all` | Removes serial wait |
| Compression | Railway/platform default | Prefer gzip/br for JSON | No app change | Platform already compresses |

## Changes directly supported by Context7 intent

- Parallelize independent I/O  
- Avoid repeated identical reads (short TTL private cache)  
- Prefer single-record fetch over full table scan for detail  
- Do not put authenticated tenant data in shared HTTP caches
