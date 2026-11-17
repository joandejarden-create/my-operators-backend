/**
 * Discovered GDI research/discovery provider inventory V1.
 * Configuration ≠ execution. Orchestrator wiring notes are evidence-based.
 */

export const GDI_PROVIDER_INVENTORY_V1 = "gdi-provider-inventory-v1";

/** @typedef {'ACTIVE'|'DISABLED'|'LEGACY'|'UNUSED'|'CONFIGURED_NOT_ORCHESTRATED'} ProviderLifecycle */

export const GDI_PROVIDER_IDS = Object.freeze({
  EXA_AGENT: "exa_agent",
  EXA_SEARCH: "exa_search",
  APIFY: "apify",
  PARALLEL: "parallel",
  SERPAPI: "serpapi",
  SURFE: "surfe",
  FIRECRAWL: "firecrawl",
  WEBHOUND: "webhound",
  HISTORICAL_CACHE: "historical_cache",
  FIXTURE_DETERMINISTIC: "fixture_deterministic",
});

/**
 * Static inventory of providers present in the repo.
 * `fullE2eOrchestratorUsage` describes canonical fourteen-segment / Full E2E paths.
 */
export const GDI_PROVIDER_INVENTORY = Object.freeze([
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.EXA_AGENT,
    name: "Exa Agent",
    adapters: [
      "lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js",
      "lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js",
      "lib/group-demand-intelligence/research/exa-agent/exa-agent-events-contracts-v1.js",
    ],
    functions: ["DISCOVERY", "DEEP_RESEARCH"],
    segments: "all_14_via_events_or_scaling_family",
    config: ["EXA_API_KEY", "authorizePaidDiscovery", "GDI paid budget ledger"],
    featureFlags: ["authorizePaidDiscovery", "forceMockOnly"],
    paid: true,
    costAccounting: "gdi-authorized-paid-budget-ledger-v1 + Exa costDollars",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "INVOKED_WHEN_AUTHORIZED",
    notes: "Segment-specific H1 titles via gdi-exa-segment-request-titles-v1 (PR #333).",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.EXA_SEARCH,
    name: "Exa Search",
    adapters: [
      "lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js",
    ],
    functions: ["SEARCH"],
    segments: "varies",
    config: ["EXA_API_KEY"],
    featureFlags: [],
    paid: true,
    costAccounting: "provider quote units when used",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "OPTIONAL_WITHIN_AGENT_TOOLS",
    notes: "Often nested inside Exa Agent tool use rather than a standalone Full E2E stage.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.APIFY,
    name: "Apify",
    adapters: [
      "lib/group-demand-intelligence/research/apify/apify-discovery-provider-v1.js",
      "lib/group-demand-intelligence/research/apify/apify-fulle2e-orchestration-v1.js",
      "lib/group-demand-intelligence/research/apify/apify-actor-registry-v1.js",
      "lib/group-demand-intelligence/nextgen-discovery/adapters/apify-hunter-bridge-v1.js",
    ],
    functions: ["DISCOVERY_SCRAPE", "DATASET_RETRIEVE", "INGEST"],
    segments: [
      "sports_entertainment",
      "social_special_events",
      "preferred_lodging_partnerships",
      "professional_event_planners",
      "local_catering_day_meetings",
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "education_scientific",
    ],
    config: ["APIFY_TOKEN", "GDI_APIFY_LIVE=1", "authorizeApifyLive"],
    featureFlags: ["authorizeApifyLive", "forceMockOnly", "GDI_APIFY_LIVE"],
    paid: true,
    costAccounting: "apify spend ledger + hotel-intelligence apify-usage store",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "WIRED_DEFAULT_MOCK_FAIL_CLOSED",
    notes:
      "Actors enabled in registry; live Actor runs require explicit founder auth. Default Full E2E path is mock/blocked — configured ≠ executed.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.PARALLEL,
    name: "Parallel (web research)",
    adapters: [
      "lib/group-demand-intelligence (parallel references in research plans / scripts)",
    ],
    functions: ["WEB_RESEARCH"],
    segments: "campaign-specific",
    config: ["PARALLEL_* credentials when present"],
    featureFlags: ["authorizePaidDiscovery"],
    paid: true,
    costAccounting: "campaign ledgers when used",
    lifecycle: "CONFIGURED_NOT_ORCHESTRATED",
    fullE2eOrchestratorUsage: "NOT_CANONICAL_FOURTEEN_SEGMENT_DEFAULT",
    notes: "May appear in specialist campaigns; not a default fourteen-segment executor.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.SERPAPI,
    name: "SerpAPI",
    adapters: [],
    functions: ["SEARCH"],
    segments: "n/a",
    config: ["SERPAPI_KEY if present in env"],
    featureFlags: [],
    paid: true,
    costAccounting: "none in GDI Full E2E",
    lifecycle: "UNUSED",
    fullE2eOrchestratorUsage: "NOT_INVOKED",
    notes: "No GDI Full E2E adapter invocation found in canonical orchestrator.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.SURFE,
    name: "Surfe",
    adapters: [
      "lib/group-demand-intelligence/gdi-contact-on-demand-reveal-v1.js",
      "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js",
    ],
    functions: ["CONTACT_ENRICHMENT"],
    segments: "cross_segment",
    config: ["authorizeSurfe / authorizeLiveProvider"],
    featureFlags: ["authorizeSurfe"],
    paid: true,
    costAccounting: "contact research cost fields on research runs",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "ON_DEMAND_FOUNDER_GATED",
    notes: "Never default-on in Full E2E; founder-gated contact reveal only.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.FIRECRAWL,
    name: "Firecrawl",
    adapters: [
      "lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js",
    ],
    functions: ["CRAWL_EXTRACT"],
    segments: "varies",
    config: ["FIRECRAWL_API_KEY"],
    featureFlags: ["authorizePaidDiscovery"],
    paid: true,
    costAccounting: "paid budget ledger when authorized",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "OPTIONAL_WHEN_AUTHORIZED",
    notes: "Referenced as paid provider requiring authorizePaidDiscovery.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.WEBHOUND,
    name: "Webhound",
    adapters: ["lib/group-demand-intelligence/cost-ledger.js"],
    functions: ["HARD_CASE_LEARNING"],
    segments: "n/a_production",
    config: ["Webhound caps in cost-ledger"],
    featureFlags: [],
    paid: true,
    costAccounting: "webhoundSpentUsd on research runs",
    lifecycle: "LEGACY",
    fullE2eOrchestratorUsage: "NOT_PRODUCTION_WRITES",
    notes: "Hard-case learning only; never production GDI inventory writes.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.HISTORICAL_CACHE,
    name: "Historical / cached discovery ledger",
    adapters: [
      "lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js",
      "lib/group-demand-intelligence/research/segment-discovery-executor-v1.js",
    ],
    functions: ["REPLAY_CANDIDATES"],
    segments: "all_14",
    config: ["data/group-demand-intelligence/discovery-ledgers"],
    featureFlags: [],
    paid: false,
    costAccounting: "zero live cost; mark cacheHit=true",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "INVOKED_DEFAULT_ZERO_COST_PATH",
    notes: "Common Full E2E path when paid discovery is unavailable.",
  }),
  Object.freeze({
    providerId: GDI_PROVIDER_IDS.FIXTURE_DETERMINISTIC,
    name: "Fixture / deterministic discovery",
    adapters: [
      "lib/group-demand-intelligence/research/segment-discovery-executor-v1.js",
    ],
    functions: ["FIXTURE_DISCOVERY"],
    segments: "test_and_dry_run",
    config: [],
    featureFlags: ["forceMockOnly"],
    paid: false,
    costAccounting: "none",
    lifecycle: "ACTIVE",
    fullE2eOrchestratorUsage: "INVOKED_IN_TESTS_AND_DRY_RUN",
    notes: "Mocks and fixtures — never counts as paid provider execution.",
  }),
]);

export function listProviderInventory() {
  return GDI_PROVIDER_INVENTORY.map((p) => ({ ...p }));
}

export function getProviderInventoryEntry(providerId) {
  return (
    GDI_PROVIDER_INVENTORY.find((p) => p.providerId === providerId) || null
  );
}
