# Execution Contract — P1-A/B

## Canonical run context

| Field | Source |
|-------|--------|
| `hotelId` | Matrix hotel loop |
| `runId` | `newFullE2eRunId` or `opts.executionRunId` |
| `researchRunId` | Shared across 14 cells |
| Process context | `setActiveExecutionRunContext` (PR #336) |
| Persistence | `data/group-demand-intelligence/execution-observability/{hotelId}/{runId}.json` |

Disable with `executionObservability: false` (not recommended for production Full E2E).

## Segment lifecycle (existing enums)

`PLANNED` → `STARTED` → terminal:

| Terminal | Meaning |
|----------|---------|
| `COMPLETED` | Strategy finished with fixture/cache/live evidence (0 candidates OK) |
| `EXECUTED` | Intermediate after provider activity (rolled to COMPLETED when call completes) |
| `SKIPPED` | Deliberate skip / no invocation evidence |
| `FAILED` | Cell failure |
| `BLOCKED_BUDGET` / `BLOCKED_CONFIG` | Gated |
| `UNKNOWN` | Historical only — never auto-upgraded |

Maps founder language `ACTUAL_RESEARCH_EXECUTED` → ledger `EXECUTED`/`COMPLETED` with provider call evidence.

## Provider lifecycle

Existing `PROVIDER_CALL_STATUS`: SUBMITTED, RUNNING, COMPLETED, FAILED, SKIPPED, CACHE_HIT, BLOCKED.

Deduped by `correlationId` (retries do not double-count spend).

## Apify lifecycle

Existing `APIFY_LIFECYCLE` retained. Default Full E2E path:

1. `APIFY_CONFIGURED`
2. `APIFY_SKIPPED` + reason `APIFY_NOT_ORCHESTRATED`

Skip reason codes: `APIFY_NOT_ORCHESTRATED`, `APIFY_UNAUTHORIZED`, `APIFY_DISABLED`, `APIFY_NOT_APPLICABLE`, `APIFY_BUDGET_BLOCKED`.

## Certification

Existing `certifyExecutionRun` verdicts:

- `FULL_E2E_EXECUTION_COMPLETE`
- `FULL_E2E_PARTIAL`
- `FULL_E2E_BLOCKED`
- `FULL_E2E_FAILED`

Operational certificate ≠ commercial quality ≠ publish authorization.

## Flags

| Option | Default | Effect |
|--------|---------|--------|
| `executionObservability` | true | Bind ledger |
| `enableApifyOrchestration` | false | Honest skip vs call bridge |
| `authorizeApifyLive` | false | Live Actor gate |
| `forceApifyMockOnly` | true | Mock-only when orchestration enabled |
| `apifyDisabled` | false | Explicit disabled skip |
