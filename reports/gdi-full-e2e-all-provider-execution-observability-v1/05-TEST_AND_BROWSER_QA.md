# Tests and browser QA

## Automated

`npm run test:gdi-full-e2e-execution-observability-v1` → **17/17 PASS**

Covers: 14 segments, call-vs-config, Apify lifecycle, Exa titles, cache-only, silent skips, failure/partial verdicts, correlation dedupe, persistence, run context, historical UNKNOWN, dashboard no secrets, inventory unused SerpAPI, admin wiring, server admin routes, Apify mock orchestration hooks.

## Browser

Opened `http://127.0.0.1:8093/gdi-execution-monitor-preview.html` on worktree server (`DEALALITY_ALLOW_WORKTREE_SERVER=1`).

Confirmed:

- YOTEL historical run id + FULL_E2E_PARTIAL
- All 14 segments UNKNOWN (honest historical)
- Provider matrix shows Exa Agent 1 aggregate call; Apify configured_but_no_calls; SerpAPI unused
- Apify lifecycle + stage timeline + certificate visible

Authenticated admin tab (`?tab=gdi-execution-monitor`) requires Memberstack admin login — same gate as GDI Research Audit.
