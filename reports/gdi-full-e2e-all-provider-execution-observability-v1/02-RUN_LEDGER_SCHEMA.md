# Run ledger schema

Schema id: `gdi-full-e2e-execution-observability-v1`

Persisted at:

`data/group-demand-intelligence/execution-observability/{hotelId}/{runId}.json`

Index:

`data/group-demand-intelligence/execution-observability/{hotelId}/index.json`

Core fields: `segments` (14), `stages` (E2E A–P), `providerCalls[]`, `apifyLifecycle[]`, `summary`, `certificate`, `silentSkips` (derived), `lineage[]`.

Provider call rows include runId, hotelId, segmentId (or CROSS_SEGMENT), stage, providerId, operation, request/job id, correlationId, timestamps, status, retryCount, actual/estimated cost, cacheHit, counts, error/skip reason. No API keys or bearer tokens.
