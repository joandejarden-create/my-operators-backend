# Execution Monitor Verification — P1-C

Admin: **AI Demand Admin → GDI Full E2E Execution** (unchanged UI).

New canonical runs write the same `gdi-full-e2e-execution-observability-v1` ledger schema.

Expected fields for a fixture canonical run:

- Hotel + runId
- 14 segment rows with discovery + deepResearchStatus terminal
- Provider calls (fixture / historical / Apify SKIP)
- Certificate with `researchCompleteness` and `publicationAuthorized: false`
- Apify not showing JOB_SUBMITTED by default

No new dashboard. Fixture/mock calls use `cacheHit: true` / fixture provider IDs — distinguishable from live.
