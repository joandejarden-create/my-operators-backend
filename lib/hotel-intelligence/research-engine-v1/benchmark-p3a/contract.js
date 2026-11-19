/**
 * HE-INTEL-001 P3A — Research Method Benchmark Foundation contract.
 * Provider-neutral evaluation framework. Paid execution = $0 / refused.
 */

import { ENGINE_CONTRACT_ID, P1_POLICY } from "../contract.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_BASE_ID_FORBIDDEN,
  HPC_TABLE_ID,
} from "../airtable-graph-contract-v1.js";

export const BENCHMARK_CONTRACT_ID = "HE_INTEL_001_RESEARCH_METHOD_BENCHMARK_V1";
export const BENCHMARK_CONTRACT_VERSION = "1.0.0-p3a-foundation";
export const BENCHMARK_PHASE = "P3A";

export const P3A_POLICY = Object.freeze({
  paid_providers_enabled: false,
  paid_budget_usd: 0,
  surfe_enabled: false,
  pdl_enabled: false,
  production_airtable_writes: false,
  canonical_graph_writes: false,
  customer_publication: "BLOCKED",
  auto_publication: false,
  deployment: false,
  parent_engine: ENGINE_CONTRACT_ID,
  reuse_p1_gateway: true,
  reuse_p2_graph_contract: true,
  competing_framework_forbidden: true,
  graph_base_id: GRAPH_BASE_ID_CANONICAL,
  forbidden_graph_base_id: GRAPH_BASE_ID_FORBIDDEN,
  hpc_table_id: HPC_TABLE_ID,
});

export const BENCHMARK_MODE = Object.freeze({
  INDEPENDENT_DISCOVERY: "INDEPENDENT_DISCOVERY",
  INCREMENTAL_PRODUCTION: "INCREMENTAL_PRODUCTION",
});

export const BENCHMARK_ARM = Object.freeze({
  DEALALITY_NATIVE: "ARM_DEALALITY_NATIVE",
  EXA: "ARM_EXA",
  PARALLEL_TASK: "ARM_PARALLEL_TASK",
  PARALLEL_SEARCH: "ARM_PARALLEL_SEARCH",
  SERPAPI: "ARM_SERPAPI",
  BRAVE: "ARM_BRAVE",
  HYBRID_WATERFALL: "ARM_HYBRID_WATERFALL",
  MOCK: "ARM_MOCK",
});

/** Arms that require paid network calls — refused under P3A. */
export const PAID_BENCHMARK_ARMS = new Set([
  BENCHMARK_ARM.EXA,
  BENCHMARK_ARM.PARALLEL_TASK,
  BENCHMARK_ARM.PARALLEL_SEARCH,
  BENCHMARK_ARM.SERPAPI,
  BENCHMARK_ARM.BRAVE,
  BENCHMARK_ARM.HYBRID_WATERFALL,
]);

export const CLAIM_ACCEPTANCE = Object.freeze({
  ACCEPTED: "ACCEPTED",
  REJECTED: "REJECTED",
  UNKNOWN: "UNKNOWN",
  HELD_FOR_REVIEW: "HELD_FOR_REVIEW",
});

export const EVIDENCE_LAYER = Object.freeze({
  DISCOVERED_CLAIM: "DISCOVERED_CLAIM",
  SUPPORTING_SOURCE: "SUPPORTING_SOURCE",
  INDEPENDENT_VERIFICATION: "INDEPENDENT_VERIFICATION",
  CONFIDENCE: "CONFIDENCE",
  ACCEPTANCE_DECISION: "ACCEPTANCE_DECISION",
  CANONICAL_GRAPH_MAPPING: "CANONICAL_GRAPH_MAPPING",
  PUBLICATION_ELIGIBILITY: "PUBLICATION_ELIGIBILITY",
});

export const COST_STATUS = Object.freeze({
  REPORTED: "REPORTED",
  ESTIMATED: "ESTIMATED",
  UNKNOWN: "UNKNOWN",
  ZERO_AUTHORIZED: "ZERO_AUTHORIZED",
});

export function writeGuaranteesP3A() {
  return {
    contract: BENCHMARK_CONTRACT_ID,
    contract_version: BENCHMARK_CONTRACT_VERSION,
    phase: BENCHMARK_PHASE,
    paid_budget_usd: 0,
    paid_providers: false,
    surfe: false,
    production_airtable_writes: false,
    canonical_graph_writes: false,
    customer_publication: "BLOCKED",
    staging_only: true,
    inherits_p1_paid_policy: P1_POLICY.paid_providers_enabled === false,
  };
}

export function assertP3APaidBudgetZero(opts = {}) {
  const requested = Number(opts.paidBudgetUsd ?? opts.budgetUsd ?? 0);
  if (requested > 0) {
    return {
      ok: false,
      error: "P3A_PAID_BUDGET_FORBIDDEN",
      message: "P3A paid research budget is $0 until founder authorizes P3B",
    };
  }
  if (opts.paidProvidersEnabled === true) {
    return {
      ok: false,
      error: "P3A_PAID_PROVIDERS_FORBIDDEN",
      message: "P3A refuses paid provider execution",
    };
  }
  return { ok: true, paid_budget_usd: 0 };
}
