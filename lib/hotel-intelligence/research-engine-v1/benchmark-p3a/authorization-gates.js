/**
 * Provider authorization + budget gates for P3A/P3B.
 */

import {
  assertP3APaidBudgetZero,
  BENCHMARK_ARM,
  PAID_BENCHMARK_ARMS,
  P3A_POLICY,
} from "./contract.js";
import { createProviderGateway, PROVIDER_IDS } from "../provider-interface.js";

export function assertBenchmarkArmAuthorized(armId, opts = {}) {
  const paidAuth = assertP3APaidBudgetZero(opts);
  if (!paidAuth.ok) return paidAuth;

  if (armId === BENCHMARK_ARM.MOCK || armId === BENCHMARK_ARM.DEALALITY_NATIVE) {
    if (opts.paidNetworkCall === true) {
      return { ok: false, error: "P3A_PAID_NETWORK_FORBIDDEN", armId };
    }
    return { ok: true, armId, paid: false };
  }

  if (PAID_BENCHMARK_ARMS.has(armId)) {
    if (opts.allowPaid !== true || P3A_POLICY.paid_providers_enabled === true) {
      // P3A_POLICY.paid_providers_enabled is always false in P3A
    }
    if (opts.allowPaid !== true || P3A_POLICY.paid_providers_enabled !== true) {
      return {
        ok: false,
        error: "P3A_PAID_ARM_REFUSED",
        armId,
        message: "Paid arm execution requires founder-authorized P3B envelope",
      };
    }
  }

  if (armId === "ARM_SURFE" || opts.provider === PROVIDER_IDS.SURFE) {
    return { ok: false, error: "SURFE_FORBIDDEN_IN_BENCHMARK", armId };
  }

  return { ok: true, armId };
}

export function createP3AProviderGateway(opts = {}) {
  // Force fail-closed paid policy for P3A harness
  return createProviderGateway({
    ...opts,
    paidProvidersEnabled: false,
    surfeEnabled: false,
    perHotelUsdCap: 0,
    perOrgUsdCap: 0,
  });
}

export function proposedMinimumPaidBenchmarkBudget() {
  // Exact proposed minimum for P3B founder review — NOT authorized in P3A.
  return {
    phase: "P3B",
    authorized: false,
    envelopeId: "M1_MINI_PILOT_SIX_HOTEL_SUBSET",
    hotels: 3,
    arms: [
      BENCHMARK_ARM.EXA,
      BENCHMARK_ARM.PARALLEL_SEARCH,
      BENCHMARK_ARM.PARALLEL_TASK,
      BENCHMARK_ARM.DEALALITY_NATIVE,
    ],
    estimatedMaxUsd: 0.53,
    hardCapUsd: 5.0,
    perHotelHardStopUsd: 2.5,
    retriesDefault: 0,
    surfe: false,
    notes: [
      "Conservative list-price ceiling from public docs (2026-11-19)",
      "SerpAPI / native USD remain UNKNOWN until plan/dashboard confirm — search-count capped",
      "Actual spend must stop at lower of ledger vs hard cap",
      "Do not execute until founder authorizes P3B",
    ],
    lineItems: [
      { arm: "EXA", estMaxUsd: 0.13, status: "ESTIMATED" },
      { arm: "PARALLEL_SEARCH", estMaxUsd: 0.02, status: "ESTIMATED" },
      { arm: "PARALLEL_TASK_CORE", estMaxUsd: 0.08, status: "ESTIMATED" },
      { arm: "PARALLEL_TASK_PRO_OPTIONAL", estMaxUsd: 0.3, status: "ESTIMATED" },
      { arm: "SERPAPI", estMaxUsd: null, status: "UNKNOWN", searchCap: 15 },
      { arm: "NATIVE", estMaxUsd: null, status: "UNKNOWN", creditCap: 24 },
    ],
  };
}
