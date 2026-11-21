/**
 * Brand-neutral, expandable SEC discovery process design (factory workstream).
 * Design-only — does not execute paid research.
 */

import { SEARCH_FAMILIES, SEARCH_TAXONOMY_VERSION } from "./search-taxonomy-v2.js";
import {
  SEC_API_QUERY_FAMILIES,
  EXA_QUERY_FAMILIES,
  PERIOD_PARTITIONS,
} from "./contract-timing-v2-query-families-v1.js";
import { FOLLOW_UP } from "./sec-discovery-coverage-ledger-v3.js";

export const SEC_DISCOVERY_EXPANSION_DESIGN_V1 = "sec-discovery-expansion-design-v1";

/** Discovery must never be limited to these — they are seeds, not ceilings. */
export const NON_RESTRICTIVE_SEED_HINTS = Object.freeze({
  brandNamesAreNotCeilings: true,
  issuerRegistryIsNotExhaustive: true,
  queryFamilyExhaustionIsNotCoverageComplete: true,
  marriottHiltonNotRequired: true,
});

/**
 * Source categories that must be systematically discovered over time.
 */
export const DISCOVERY_SOURCE_CATEGORIES = Object.freeze([
  {
    id: "SEC_REGISTRANT_HOTEL_EXPOSURE",
    label: "SEC registrants with hotel ownership/ops/franchise/management/lease/financing exposure",
    method: "Content + SIC/industry + full-text discovery; expand CIK set from hits",
    priority: 1,
  },
  {
    id: "CMBS_ISSUERS_AND_COLLATERAL",
    label: "CMBS issuers and hotel collateral pools (424B schedules)",
    method: "Full-text franchise/management expiration columns; deal-level accession graph",
    priority: 1,
  },
  {
    id: "REIT_PUBLIC_DISCLOSURES",
    label: "Lodging REIT and public-company 10-K / 10-Q / 8-K / DEF 14A",
    method: "Eligible filing inventory per CIK + exhibit index enumeration",
    priority: 1,
  },
  {
    id: "EXHIBITS_AND_AMENDMENTS",
    label: "Hotel-related exhibits (EX-10*) and amendments",
    method: "Filing package index.json; treat parent ≠ exhibit",
    priority: 1,
  },
  {
    id: "INCORPORATED_BY_REFERENCE",
    label: "Incorporated-by-reference contractual documents",
    method: "Parse IBR pointers; queue FETCH_IBR follow-ups",
    priority: 2,
  },
  {
    id: "HISTORICAL_BACKFILL",
    label: "Historical filings + subsequent amendments (1994+)",
    method: "Period partitions + resumable checkpoints; cache-first EDGAR",
    priority: 2,
  },
  {
    id: "UNREGISTERED_RELEVANT_ISSUERS",
    label: "Relevant companies not previously in issuer registry",
    method: "Adaptive queue from entity extraction / related-party / CMBS depositor CIKs",
    priority: 1,
  },
  {
    id: "INTL_PUBLIC_DISCLOSURES",
    label: "International public disclosures beyond SEC",
    method: "Jurisdiction adapters (intl-readiness); persist globally; HPC prioritize Americas/Europe",
    priority: 3,
  },
]);

/**
 * Brand-neutral discovery loop for the factory engine.
 */
export function buildBrandNeutralDiscoveryProcess() {
  return {
    version: SEC_DISCOVERY_EXPANSION_DESIGN_V1,
    constraints: NON_RESTRICTIVE_SEED_HINTS,
    stages: [
      {
        id: "SEED_OR_RESUME",
        actions: [
          "Load coverage ledger V3 + checkpoints",
          "Accept optional seed CIKs/tickers WITHOUT treating them as the universe",
          "Load open follow-ups (exhibits, IBR, amendments)",
        ],
      },
      {
        id: "CONTENT_DISCOVERY",
        actions: [
          "Run brand-neutral full-text / filing-query families (not brand-name-only)",
          "Discover CMBS / REIT / EX-10 / amendment accessions",
          "Record every hit as a coverage row (QUEUED)",
        ],
      },
      {
        id: "ENTITY_EXTRACTION",
        actions: [
          "Extract hotel identities, owners, managers, franchisors, SPVs from document content",
          "Link CIK ↔ entity ↔ hotel without requiring prior brand allowlist",
          "Enqueue newly observed issuer CIKs into adaptive queue (capped)",
        ],
      },
      {
        id: "SOURCE_RELATIONSHIPS",
        actions: [
          "Graph parent accession → exhibits → IBR targets → later amendments",
          "Deduplicate by coverageUnitId",
          "Preserve historical versions (do not overwrite)",
        ],
      },
      {
        id: "RETRIEVE_AND_EXTRACT",
        actions: [
          "Cache-first free EDGAR retrieval",
          "Paid providers only under budget ledger + founder ceiling",
          "Extract contractual intelligence; update ledger extractionStatus",
          "Hand verified terms to ENRICH_CONTRACT_TERMS when authorized",
        ],
      },
      {
        id: "GEO_AND_HPC",
        actions: [
          "Persist verified intelligence globally (Research Mode)",
          "Prioritize HPC reconcile/enrich for Americas + Europe",
          "Queue INTL_SOURCE_EXPAND for non-SEC jurisdictions",
        ],
      },
      {
        id: "REPORT_COVERAGE",
        actions: [
          "Publish measurable coverage summary + blind spots + backlog",
          "Never claim complete SEC hotel intelligence discovery",
        ],
      },
    ],
    factoryIntegration: {
      proposedStage: "DISCOVER_EXPAND",
      existingReuse: ["DISCOVER", "EXTRACT", "VERIFY", "ENRICH_CONTRACT_TERMS", "PERSIST_RESEARCH"],
      modes: ["RESEARCH"],
      promotionUnchanged: true,
      publicationUnchanged: true,
      customerBlockedDefault: true,
      powershellSameEngine: true,
    },
    taxonomyVersion: SEARCH_TAXONOMY_VERSION,
    taxonomyFamilyCount: SEARCH_FAMILIES.length,
    seedQueryFamilies: {
      secApi: SEC_API_QUERY_FAMILIES.map((q) => q.id),
      exa: EXA_QUERY_FAMILIES.map((q) => q.id),
      note: "Seeds for incremental intel/$ — expandable; exhaustion ≠ complete coverage",
    },
    periodPartitions: PERIOD_PARTITIONS.map((p) => p.id),
    followUpVocabulary: Object.values(FOLLOW_UP),
  };
}

/**
 * Prioritize next research dollars by expected contractual intelligence yield.
 */
export function buildIncrementalIntelPerDollarRoadmap() {
  return {
    version: SEC_DISCOVERY_EXPANSION_DESIGN_V1,
    principle: "Maximize verified contractual terms / renewal / decision-window yield per USD",
    orderedWorkstreams: [
      {
        rank: 1,
        id: "EX10_HMA_FRANCHISE_EXHIBIT_HARVEST",
        why: "Terms pilot showed EX-10 management/franchise prose >> CMBS schedule rows for lifecycle terms",
        expectedIntel: "Term years, renewal options, notice days, fees, termination/CoC",
        costProfile: "Mostly free EDGAR + selective SEC-API full-text",
        dependsOn: ["coverage-ledger-v3", "ENRICH_CONTRACT_TERMS"],
      },
      {
        rank: 2,
        id: "CMBS_DEAL_GRAPH_EXPANSION",
        why: "Existing verified inventory is CMBS-heavy; many deals/issuers still unsearched",
        expectedIntel: "Hotel identities + franchise expiration columns + collateral pools",
        costProfile: "Free EDGAR primary; SEC-API for deal discovery",
        dependsOn: ["brand-neutral full-text", "coverage ledger"],
      },
      {
        rank: 3,
        id: "UNREGISTERED_ISSUER_ADAPTIVE_QUEUE",
        why: "Registry of ~32 companies is a seed, not the universe",
        expectedIntel: "New CIKs from depositors, related parties, managers, franchisees",
        costProfile: "Low — queue caps + free submissions inventory",
        dependsOn: ["entity extraction", "adaptive-discovery-queue-v2"],
      },
      {
        rank: 4,
        id: "LATER_AMENDMENT_AND_RENEWAL_PROBES",
        why: "Terms pilot: 0 executed renewals verified — needs systematic later-doc search",
        expectedIntel: "Executed renewals/extensions/terminations/reflags",
        costProfile: "Capped Exa + free EDGAR",
        dependsOn: ["ENRICH_CONTRACT_TERMS", "budget ledger"],
      },
      {
        rank: 5,
        id: "REIT_10K_PROPERTY_SCHEDULE_BACKFILL",
        why: "Large eligible inventories barely processed (pending >> processed)",
        expectedIntel: "Portfolio hotels + ownership/management relationships",
        costProfile: "Free EDGAR + checkpoint resume",
        dependsOn: ["eligible-filing-inventory-v2"],
      },
      {
        rank: 6,
        id: "IBR_AND_HISTORICAL_PERIOD_PARTITIONS",
        why: "Incorporated-by-reference + pre-2019 periods under-covered",
        expectedIntel: "Original agreements + amendments",
        costProfile: "Free EDGAR dominant",
        dependsOn: ["filing-package-exhibits-v2"],
      },
      {
        rank: 7,
        id: "INTL_NON_SEC_ADAPTERS",
        why: "SEC finds global hotels; durable intl coverage needs local disclosures",
        expectedIntel: "Non-US agreements / ownership",
        costProfile: "Separate authorization; no paid launch in this packet",
        dependsOn: ["intl-readiness-v1", "geo HPC priority Americas/Europe"],
      },
    ],
    stopRules: [
      "No uncontrolled large-scale paid research",
      "Budget ceiling + resume via PowerShell/Node factory",
      "Research Mode ≠ Canonical Promotion ≠ customer publication",
      "Do not claim exhaustive SEC hotel discovery",
    ],
  };
}

export default {
  buildBrandNeutralDiscoveryProcess,
  buildIncrementalIntelPerDollarRoadmap,
  DISCOVERY_SOURCE_CATEGORIES,
  NON_RESTRICTIVE_SEED_HINTS,
};
