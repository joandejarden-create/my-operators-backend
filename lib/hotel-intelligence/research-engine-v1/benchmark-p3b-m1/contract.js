/**
 * HE-INTEL-001 P3B-M1 — controlled live research readiness contract.
 * $5 / $2.50 caps are PROPOSED — paid execution requires separate founder auth.
 */

import { ENGINE_CONTRACT_ID, P1_POLICY } from "../contract.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
} from "../airtable-graph-contract-v1.js";
import {
  BENCHMARK_CONTRACT_ID,
  BENCHMARK_MODE,
  BENCHMARK_ARM,
} from "../benchmark-p3a/contract.js";

export const P3B_M1_CONTRACT_ID = "HE_INTEL_001_P3B_M1_LIVE_BENCHMARK_V1";
export const P3B_M1_CONTRACT_VERSION = "1.1.0-m1-live-activation";
export const P3B_M1_PHASE = "P3B-M1";

/** Required auth id for any future paid enablement (not present = refuse). */
export const P3B_M1_FOUNDER_PAID_AUTH_ID =
  "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1";

export const P3B_M1_HARD_CAP_USD = 5.0;
export const P3B_M1_PER_HOTEL_CAP_USD = 2.5;

export const P3B_M1_POLICY = Object.freeze({
  phase: P3B_M1_PHASE,
  parent_engine: ENGINE_CONTRACT_ID,
  parent_benchmark: BENCHMARK_CONTRACT_ID,
  paid_budget_proposed_usd: P3B_M1_HARD_CAP_USD,
  paid_budget_authorized: false,
  per_hotel_cap_usd: P3B_M1_PER_HOTEL_CAP_USD,
  surfe_enabled: false,
  pdl_enabled: false,
  production_airtable_writes: false,
  canonical_graph_writes: false,
  customer_publication: "BLOCKED",
  auto_publication: false,
  deployment: false,
  graph_base_id: GRAPH_BASE_ID_CANONICAL,
  hpc_table_id: HPC_TABLE_ID,
  inherits_p1_paid_default: P1_POLICY.paid_providers_enabled === false,
  modes: [BENCHMARK_MODE.INDEPENDENT_DISCOVERY, BENCHMARK_MODE.INCREMENTAL_PRODUCTION],
  arms: [
    BENCHMARK_ARM.EXA,
    BENCHMARK_ARM.PARALLEL_SEARCH,
    BENCHMARK_ARM.PARALLEL_TASK,
    BENCHMARK_ARM.DEALALITY_NATIVE,
  ],
});

export function writeGuaranteesP3BM1() {
  return {
    contract: P3B_M1_CONTRACT_ID,
    contract_version: P3B_M1_CONTRACT_VERSION,
    phase: P3B_M1_PHASE,
    paid_budget_authorized: false,
    proposed_hard_cap_usd: P3B_M1_HARD_CAP_USD,
    proposed_per_hotel_cap_usd: P3B_M1_PER_HOTEL_CAP_USD,
    surfe: false,
    pdl: false,
    production_airtable_writes: false,
    canonical_graph_writes: false,
    staging_only: true,
  };
}

/**
 * Founder paid authorization gate — defaults to refuse.
 * Separate founder confirmation required before any live paid spend.
 */
export function resolveP3BM1FounderPaidAuthorization(opts = {}) {
  const authId = String(opts.authId || opts.founderAuthId || "").trim();
  const confirmed = opts.confirmPaidExecution === true;
  const budgetUsd = Number(opts.authorizedBudgetUsd);
  if (
    authId === P3B_M1_FOUNDER_PAID_AUTH_ID &&
    confirmed &&
    Number.isFinite(budgetUsd) &&
    budgetUsd > 0 &&
    budgetUsd <= P3B_M1_HARD_CAP_USD
  ) {
    return {
      ok: true,
      authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
      authorizedBudgetUsd: budgetUsd,
      note: "FOUNDER_PAID_AUTHORIZED",
    };
  }
  return {
    ok: false,
    authId: authId || null,
    error: "P3B_M1_PAID_NOT_AUTHORIZED",
    message:
      "Paid M1 execution requires FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 + confirmPaidExecution + authorizedBudgetUsd ≤ $5",
  };
}
