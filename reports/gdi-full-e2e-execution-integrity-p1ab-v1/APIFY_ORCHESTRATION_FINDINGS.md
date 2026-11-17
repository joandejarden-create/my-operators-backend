# Apify Orchestration Findings — P1-B

## Previous state (audit CONFIRMED)

- Modules present: `apify-fulle2e-orchestration-v1.js`, actor registry, discovery provider.
- **Not imported** by `full-e2e-fourteen-segment-discovery-execution-v1.js` or hotel template.
- Inventory claimed `WIRED_DEFAULT_MOCK_FAIL_CLOSED` — overstated Full E2E coupling.
- Correct audit label: **CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E**.

## New state (this PR)

| Aspect | Status |
|--------|--------|
| Inventory lifecycle | `CONFIGURED_NOT_ORCHESTRATED` |
| Inventory usage | `CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E` |
| Fourteen-segment import | Bridge accounting **connected**; Actor dispatch **off by default** |
| Default ledger outcome | `APIFY_NOT_ORCHESTRATED` on Apify-primary segments |
| Live Actor jobs | Still blocked without explicit flags + env |
| Customer inventory | Unchanged |

## Is the bridge connected?

**Accounting: yes.** Canonical matrix calls `accountApifyForFourteenSegmentCell` for `workflowPrefersApify` segments.

**Live/mock Actor dispatch: only when** `enableApifyOrchestration === true` (still fail-closed for live).

## Authorization and budget gates (unchanged)

- `authorizeApifyLive`
- `forceMockOnly` / `forceApifyMockOnly`
- `isApifyLiveAuthorized` / GDI Apify live env flag inside provider
- Campaign ceiling / Apify spend ledger inside existing bridge

## Remaining work for real Apify runs

1. Founder authorize budget + `enableApifyOrchestration` + `authorizeApifyLive`.
2. Validate JOB_SUBMITTED → RESULTS_INGESTED path on one Apify-primary segment.
3. Prove commercial contribution to qualified opportunities (P2).
4. Do not broaden segment coverage without authorization.
