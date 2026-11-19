/**
 * Atomic shared-budget ledger for P3B-M1.
 * Wraps P1 reserve/settle/release and enforces run hard cap + per-hotel cap
 * before any provider spend. Concurrent reserves cannot bypass the shared cap.
 */

import { createBudgetLedger, BUDGET_LEDGER_VERSION } from "../budget-ledger.js";
import { FAILURE_CLASS } from "../contract.js";
import {
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
} from "./contract.js";

export const M1_SHARED_BUDGET_LEDGER_VERSION = "he-intel-p3b-m1-shared-budget-v1";

/**
 * @param {object} [opts]
 * @param {number} [opts.runUsdCap]
 * @param {number} [opts.perHotelUsdCap]
 */
export function createM1SharedBudgetLedger(opts = {}) {
  const runUsdCap = Number(opts.runUsdCap ?? P3B_M1_HARD_CAP_USD);
  const perHotelUsdCap = Number(opts.perHotelUsdCap ?? P3B_M1_PER_HOTEL_CAP_USD);
  const base = createBudgetLedger({
    perHotelUsdCap,
    perOrgUsdCap: opts.perOrgUsdCap ?? null,
    perHotelCreditCap: opts.perHotelCreditCap ?? null,
  });

  /** Serialize reserve checks so concurrent callers cannot oversubscribe. */
  let chain = Promise.resolve();
  let runReservedUsd = 0;
  let runSettledProviderUsd = 0;
  let runSettledModelTokenUsd = 0;
  let runSettledOrchestrationUsd = 0;
  const costBreakdown = [];

  function syncReserve(entry = {}) {
    const cost = Number(entry.cost_usd);
    const costStatus = entry.cost_usd_status || (Number.isFinite(cost) ? "ESTIMATED" : "UNKNOWN");
    if (costStatus === "UNKNOWN" || !Number.isFinite(cost)) {
      return {
        ok: false,
        error: FAILURE_CLASS.BUDGET_EXHAUSTED,
        scope: "usd_unknown_under_cap",
        note: "unknown_cost_cannot_be_zeroed",
      };
    }
    if (runReservedUsd + cost > runUsdCap + 1e-9) {
      return {
        ok: false,
        error: FAILURE_CLASS.BUDGET_EXHAUSTED,
        scope: "shared_run_usd",
        runReservedUsd,
        attempted: cost,
        runUsdCap,
      };
    }
    const prior = base.getOperation?.(entry.operation_id);
    if (prior?.status === "SETTLED" || prior?.status === "RESERVED") {
      return base.reserve(entry);
    }
    const reserved = base.reserve(entry);
    if (reserved.ok && !reserved.already_settled && !reserved.already_reserved) {
      runReservedUsd = Number((runReservedUsd + cost).toFixed(6));
    }
    return reserved;
  }

  function reserve(entry = {}) {
    // Synchronous atomic check for Node single-thread + Promise queue for async callers
    return syncReserve(entry);
  }

  async function reserveAsync(entry = {}) {
    const run = chain.then(() => syncReserve(entry));
    chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  function settle(operationId, actual = {}) {
    const prior = base.getOperation?.(operationId);
    if (prior?.status === "SETTLED") {
      return base.settle(operationId, actual);
    }
    const result = base.settle(operationId, actual);
    if (!result.ok) return result;

    const providerCost =
      actual.provider_cost_usd != null
        ? Number(actual.provider_cost_usd)
        : actual.cost_usd != null
          ? Number(actual.cost_usd)
          : null;
    const modelTokenCost =
      actual.model_token_cost_usd != null ? Number(actual.model_token_cost_usd) : 0;
    const orchestrationCost =
      actual.orchestration_cost_usd != null ? Number(actual.orchestration_cost_usd) : 0;

    if (Number.isFinite(providerCost)) {
      runSettledProviderUsd = Number((runSettledProviderUsd + providerCost).toFixed(6));
    }
    if (Number.isFinite(modelTokenCost) && modelTokenCost) {
      runSettledModelTokenUsd = Number((runSettledModelTokenUsd + modelTokenCost).toFixed(6));
    }
    if (Number.isFinite(orchestrationCost) && orchestrationCost) {
      runSettledOrchestrationUsd = Number(
        (runSettledOrchestrationUsd + orchestrationCost).toFixed(6)
      );
    }

    // Release reserved headroom difference when actual < reserved
    if (prior?.status === "RESERVED" && Number.isFinite(prior.reserved_usd)) {
      const actualTotal =
        (Number.isFinite(providerCost) ? providerCost : prior.reserved_usd) +
        (Number.isFinite(modelTokenCost) ? modelTokenCost : 0) +
        (Number.isFinite(orchestrationCost) ? orchestrationCost : 0);
      const delta = Number((prior.reserved_usd - actualTotal).toFixed(6));
      if (delta > 0) {
        runReservedUsd = Number(Math.max(0, runReservedUsd - delta).toFixed(6));
      }
    }

    costBreakdown.push({
      at: new Date().toISOString(),
      operation_id: operationId,
      provider_cost_usd: providerCost,
      model_token_cost_usd: modelTokenCost || 0,
      orchestration_cost_usd: orchestrationCost || 0,
      provider: prior?.provider || actual.provider || null,
      hotel_id: prior?.hotel_id || actual.hotel_id || null,
    });
    return result;
  }

  function release(operationId, meta = {}) {
    const prior = base.getOperation?.(operationId);
    const result = base.release(operationId, meta);
    if (result.ok && prior?.status === "RESERVED" && Number.isFinite(prior.reserved_usd)) {
      runReservedUsd = Number(Math.max(0, runReservedUsd - prior.reserved_usd).toFixed(6));
    }
    return result;
  }

  function snapshot() {
    return {
      version: M1_SHARED_BUDGET_LEDGER_VERSION,
      parent: BUDGET_LEDGER_VERSION,
      runUsdCap,
      perHotelUsdCap,
      runReservedUsd,
      runSettledProviderUsd,
      runSettledModelTokenUsd,
      runSettledOrchestrationUsd,
      totalSettledUsd: Number(
        (
          runSettledProviderUsd +
          runSettledModelTokenUsd +
          runSettledOrchestrationUsd
        ).toFixed(6)
      ),
      costBreakdown: [...costBreakdown],
      parentSnapshot: base.snapshot(),
      canParallelBypassSharedBudget: false,
    };
  }

  return {
    reserve,
    reserveAsync,
    settle,
    release,
    getOperation: (...a) => base.getOperation(...a),
    recordUsage: (...a) => base.recordUsage?.(...a),
    canSpend: (...a) => base.canSpend?.(...a),
    snapshot,
    getBaseLedger: () => base,
  };
}
