# All Provider Effectiveness Audit

Primary source: `lib/group-demand-intelligence/observability/gdi-provider-inventory-v1.js` (CONFIRMED static inventory) + Apify modules + YOTEL historical ledger.

## Provider inventory

| Provider | Lifecycle | Full E2E usage (inventory) | Evidence |
|----------|-----------|----------------------------|----------|
| Exa Agent | ACTIVE | INVOKED_WHEN_AUTHORIZED | YOTEL hist ledger `exa_agent`; adapters under `research/exa-agent/` |
| Exa Search | ACTIVE | OPTIONAL_WITHIN_AGENT_TOOLS | Nested tool use |
| Apify | ACTIVE | **WIRED_DEFAULT_MOCK_FAIL_CLOSED** | `apify-fulle2e-orchestration-v1.js`, `apify-actor-registry-v1.js` |
| Parallel | CONFIGURED_NOT_ORCHESTRATED | NOT_CANONICAL_FOURTEEN_SEGMENT_DEFAULT | Campaign-specific |
| SerpAPI | UNUSED | NOT_INVOKED | No Full E2E adapter |
| Surfe | ACTIVE | ON_DEMAND_FOUNDER_GATED | Contact enrich only |
| Firecrawl | ACTIVE | OPTIONAL_WHEN_AUTHORIZED | Fourteen-segment execution references |
| Webhound | LEGACY | NOT_PRODUCTION_WRITES | Hard-case learning |
| Historical cache | ACTIVE | INVOKED_DEFAULT_ZERO_COST_PATH | Common unpaid path |
| Fixture deterministic | ACTIVE | Fixture/discovery | Tests / zero-cost |

## Apify special investigation

### Configured (CONFIRMED)

- Registry of verified Store Actors (`apify-actor-registry-v1.js`) with `enabled: true` for Eventbrite scrapers and other families.
- Demand segments listed for sports, social, preferred lodging, planners, local catering, associations, CMI, education.
- Orchestration bridge `orchestrateApifyDiscoveryForWorkflow` with lifecycle hooks: ENABLED → JOB_SUBMITTED → COMPLETED → RESULTS_RETRIEVED → RESULTS_INGESTED / SKIPPED / FAILED.

### Live execution during Full E2E (CONFIRMED gap — strengthened)

- Default: **mock / fail-closed** unless `authorizeApifyLive` + `GDI_APIFY_LIVE=1` + `APIFY_TOKEN`.  
- Provider inventory notes: “configured ≠ executed.”  
- YOTEL / Radisson / Bethesda historical observability runs: Apify lifecycle **CONFIGURED → SKIPPED** (`no_apify_execution_evidence_in_historical_reports`); YOTEL/Radisson list **`exa_agent` only**; Bethesda lists **no** provider calls.  
- **Wiring gap (CONFIRMED):** `orchestrateApifyDiscoveryForWorkflow` is imported by tests / its own module — **not** by `full-e2e-fourteen-segment-discovery-execution-v1.js` or `gdi-full-e2e-hotel-template-v1.js`. Inventory label `WIRED_DEFAULT_MOCK_FAIL_CLOSED` overstates Full E2E coupling → treat as **CONFIGURED_NOT_ORCHESTRATED_IN_FULL_E2E**.  
- Separate rail: older need-date / Waterstone runners *can* call `runApifyDiscovery` — not the canonical fourteen-segment path.  
- This audit did **not** start Actors (forbidden).

### Lifecycle status for audited cohort

| Stage | Status |
|-------|--------|
| CONFIGURED | CONFIRMED |
| ENABLED (Full E2E fourteen-segment) | **NOT wired** (CONFIRMED) |
| JOB_SUBMITTED / RESULTS_INGESTED (cohort ledgers) | **Absent** (CONFIRMED) |
| SKIPPED (hist ledgers) | CONFIRMED |

**Integration gap:** Apify Actors + 14-segment preference matrix exist, but canonical Full E2E does not call the orchestration bridge. First remediation is wire-or-honest-SKIP on the shared path — not more Actor registrations.

## Exa review

- Stage A/B discovery + deep research adapters exist.  
- Segment-specific H1 titles: `gdi-exa-segment-request-titles-v1.js` (PR #333) — **CONFIRMED labels/titles**.  
- Whether missions/queries differ enough to change discovery outcomes: **LIKELY titles are stronger than undifferentiated headings; mission differentiation depth UNVERIFIED** without paired run artifacts comparing pre/post missions.  
- Cost attribution: paid budget ledger + Exa costDollars (inventory).

## Parallel / SerpAPI / others

- Parallel: not default fourteen-segment.  
- SerpAPI: unused in Full E2E.  
- Surfe: must remain founder-gated (do not auto-run).

## Economics

Reliable cross-provider cost-per-Sales-Ready metrics are **NOT INSPECTED** comprehensively in this pass (would require ledger joins across campaigns). YOTEL scale campaign is Exa-led; Apify contribution to that 42-pack is **not evidenced**.

## Required result

Exa is the proven paid workhorse for recent published inventories. Apify is real code with Actor registry but **not** demonstrated as live Full E2E value on the audited ledgers. Next: either certify live Apify on one hotel with ledger proof, or formally mark segments as SKIPPED_APIFY_UNAUTH when mock path is taken.
