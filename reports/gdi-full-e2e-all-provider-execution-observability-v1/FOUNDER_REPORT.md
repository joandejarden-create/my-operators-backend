# GDI Full E2E — All-Provider Execution Observability V1

**Status:** Implemented (observability only — no paid research)  
**Contract:** `gdi-full-e2e-execution-observability-v1`

## Objective

Give the founder a permanent admin view of which of the 14 demand segments actually ran, which providers were invoked (including Apify lifecycle), costs, ingestion, stage progress, and whether Full E2E operationally finished — without reading terminal logs.

## Architecture summary

| Layer | Location |
|---|---|
| Provider inventory | `lib/.../observability/gdi-provider-inventory-v1.js` |
| Durable run ledger | `lib/.../observability/gdi-execution-ledger-store-v1.js` → `data/group-demand-intelligence/execution-observability/{hotelId}/` |
| Run context hooks | `gdi-execution-run-context-v1.js` (opt-in; no side effects when unset) |
| Historical recon | `gdi-historical-execution-reconciliation-v1.js` |
| Admin UI | AI Demand Admin tab **GDI Full E2E Execution** |
| API | Admin-only `/api/group-demand-intelligence/.../execution-monitor/*` |

## Provider inventory (discovered)

| Provider | Full E2E usage | Notes |
|---|---|---|
| Exa Agent | INVOKED_WHEN_AUTHORIZED | Segment titles from PR #333 preserved |
| Exa Search | OPTIONAL_WITHIN_AGENT_TOOLS | Nested in Agent tools |
| Apify | WIRED_DEFAULT_MOCK_FAIL_CLOSED | Actors enabled in registry; live needs `authorizeApifyLive` + `GDI_APIFY_LIVE=1` |
| Parallel | NOT_CANONICAL_FOURTEEN_SEGMENT_DEFAULT | Campaign-specific |
| SerpAPI | NOT_INVOKED | Unused in GDI Full E2E |
| Surfe | ON_DEMAND_FOUNDER_GATED | Contact reveal only |
| Firecrawl | OPTIONAL_WHEN_AUTHORIZED | Paid flag gated |
| Webhound | NOT_PRODUCTION_WRITES | Hard-case learning |
| Historical cache | INVOKED_DEFAULT_ZERO_COST_PATH | Common zero-cost path |
| Fixture/deterministic | INVOKED_IN_TESTS_AND_DRY_RUN | Mocks |

**Configured ≠ executed.** Provider matrix rows show inventory even when call count is zero.

## Apify findings

- Registry + Full E2E orchestration exist (`apify-fulle2e-orchestration-v1.js`).
- Preferred workflows include sports, social, preferred lodging, professional planners, local catering.
- Default path is mock / fail-closed — not silent skip after budgeting.
- Lifecycle states tracked: CONFIGURED → ENABLED → JOB_SUBMITTED → … → RESULTS_INGESTED / FAILED / SKIPPED.
- Historical YOTEL/Radisson/Bethesda recon: **APIFY_SKIPPED** (no durable Actor job IDs in report artifacts).

## Exa findings

- Segment-specific request titles (PR #333) unchanged.
- Live discovery bridge emits ledger rows when a Full E2E run context is active.
- Historical campaigns: Exa usage evidenced at aggregate level; per-segment request attribution marked **UNKNOWN** for pre-observability runs.

## 14-segment + stage contract

- All 14 canonical workflow IDs from `universal-contact-research-contract-v1`.
- Display names reused from `gdi-exa-segment-request-titles-v1`.
- Pipeline stages = `E2E_STAGE` A–P from `full-e2e-contract-v1.js`.
- Silent-skip detector: never scheduled / never started / started without research / failed / zero qualified / blocked / cache-only.
- Verdicts: `FULL_E2E_EXECUTION_COMPLETE` | `PARTIAL` | `BLOCKED` | `FAILED` (operational — not publish authorization).

## Admin

Path: `/app#/admin/ai-demand?tab=gdi-execution-monitor`  
Requires existing AI Demand Admin / founder admin gate.

## Historical reconciliation

Persisted runs (PARTIAL / UNKNOWN segment detail):

| Hotel | Run ID |
|---|---|
| YOTEL Geneva Lake | `hist_yotel_geneva_exa_30usd_scale_v1` |
| Radisson Blu Toronto | `hist_radisson_toronto_exa_campaigns_v1` |
| Bethesda Marriott | `hist_bethesda_full_e2e_template_v1` |

## Tests

```bash
npm run test:gdi-full-e2e-execution-observability-v1
```

17/17 PASS (mocked; no paid calls).

## Operational limitations

1. Future live campaigns must set execution run context (`setActiveExecutionRunContext`) for automatic Exa/Apify capture — hooks are no-ops otherwise.
2. Historical per-segment Exa attribution cannot be reconstructed for pre–PR #333 runs without fabricating data.
3. Admin shell login required for live browser monitor; UI wiring covered by regression tests + local worktree browser QA.
4. Does not activate Apify/Exa/Parallel or change budgets.

## Governance

No Exa / Apify / Parallel / Surfe / Airtable writes / CUSTOMER_PUBLISHED / Railway in this task.

## GitHub

Filled at closeout.
