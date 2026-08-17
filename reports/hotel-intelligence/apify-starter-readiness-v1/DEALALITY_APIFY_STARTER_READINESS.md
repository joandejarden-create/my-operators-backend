# Apify Starter readiness — Hotel Intelligence usage tracking

**Date:** 2026-08-12  
**Scope:** Operational cost metadata only. No production Airtable / census writes. MCP integration unchanged.

## Status

| Item | Value |
| --- | --- |
| Plan | Apify Starter (upgraded) |
| MCP | `plugin-apify-apify` — **do not replace** |
| Local `APIFY_TOKEN` | Not required for interactive MCP workloads |
| Cost tracking | **Enabled** — `lib/hotel-intelligence/apify-usage/` |
| Ledger | `data/hotel-intelligence/apify-usage/ledger.json` |
| New Actor volume this task | **0** (read-only backfill of existing runs) |

## Auth

- **Current:** Cursor Apify MCP session (OAuth / plugin token). Sufficient for agent-initiated Actor runs and `GET /v2/actor-runs/{id}` cost reads.
- **Local token later:** Required when server-side Node jobs use `apify-client` or direct API — MCP does not inject credentials into local scripts. Optional env placeholders: `APIFY_TOKEN` / `APIFY_API_TOKEN` in `.env.example` (never commit secrets).

## Cost capture

Prefer Apify run payload field **`usageTotalUsd`** (plus `chargedEventCounts` / `accountedChargedEventCounts`).  
`get-actor-run` tool summaries often omit USD — use MCP resource:

`http://api.apify.internal:3333/v2/actor-runs/{runId}`

## Use cases

`HOTEL_DISCOVERY` · `HOTEL_IDENTITY` · `ROOM_COUNT` · `OWNER_INTELLIGENCE` · `LEGAL_ENTITY` · `CONTACT_INTELLIGENCE` · `MARKET_ALERT` · `OPPORTUNITY_DISCOVERY`

## Commands

```bash
npm run test:apify-usage-tracking
npm run apify-usage-record-run -- --use-case=ROOM_COUNT --run-json=path/to/run.json
npm run apify-usage-backfill-benchmark
```

## Historical backfill (benchmark runs)

5 known Tripadvisor runs recorded from MCP snapshots. Actual billed total **$0.7405** (`usageTotalUsd`), vs earlier SILVER PPE estimates — bill from Apify USD, not list-price × rows alone.

## Guardrails

- Metadata ledger ≠ authoritative hotel data  
- No production writes  
- Do not increase Actor volume solely because Starter is available  
- Never log or store Apify tokens
