# Provider inventory

Authoritative module: `lib/group-demand-intelligence/observability/gdi-provider-inventory-v1.js`

Key rule: **an installed package or enabled Actor is not proof of execution.** The provider matrix records `callsSubmitted` from the durable ledger only.

| Provider ID | Full E2E usage | Lifecycle |
|---|---|---|
| `exa_agent` | INVOKED_WHEN_AUTHORIZED | ACTIVE |
| `exa_search` | OPTIONAL_WITHIN_AGENT_TOOLS | ACTIVE |
| `apify` | WIRED_DEFAULT_MOCK_FAIL_CLOSED | ACTIVE |
| `parallel` | NOT_CANONICAL_FOURTEEN_SEGMENT_DEFAULT | CONFIGURED_NOT_ORCHESTRATED |
| `serpapi` | NOT_INVOKED | UNUSED |
| `surfe` | ON_DEMAND_FOUNDER_GATED | ACTIVE |
| `firecrawl` | OPTIONAL_WHEN_AUTHORIZED | ACTIVE |
| `webhound` | NOT_PRODUCTION_WRITES | LEGACY |
| `historical_cache` | INVOKED_DEFAULT_ZERO_COST_PATH | ACTIVE |
| `fixture_deterministic` | INVOKED_IN_TESTS_AND_DRY_RUN | ACTIVE |

Apify gap for default Full E2E: live Actors require `authorizeApifyLive` + live flag; otherwise lifecycle records APIFY_SKIPPED / mock — not silent success.
