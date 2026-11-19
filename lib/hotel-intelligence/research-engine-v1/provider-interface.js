/**
 * Provider-neutral research interface for HE-INTEL-001.
 * P1: all paid providers refused. Same evidence gates for every future arm.
 *
 * All execution goes through createExecutionGateway (auth → reserve → call → settle).
 * No adapter may be invoked outside that path.
 */

import { FAILURE_CLASS, P1_POLICY } from "./contract.js";
import { createBudgetLedger } from "./budget-ledger.js";
import {
  createExecutionGateway,
  EXECUTION_GATEWAY_VERSION,
  buildOperationId,
} from "./execution-gateway.js";

export const PROVIDER_IDS = Object.freeze({
  DEALALITY_NATIVE: "DEALALITY_NATIVE",
  EXA: "EXA",
  PARALLEL_TASK: "PARALLEL_TASK",
  PARALLEL_SEARCH: "PARALLEL_SEARCH",
  SERPAPI: "SERPAPI",
  BRAVE: "BRAVE",
  HYBRID_WATERFALL: "HYBRID_WATERFALL",
  SURFE: "SURFE",
  MOCK: "MOCK",
});

export const PAID_PROVIDERS = new Set([
  PROVIDER_IDS.EXA,
  PROVIDER_IDS.PARALLEL_TASK,
  PROVIDER_IDS.PARALLEL_SEARCH,
  PROVIDER_IDS.SERPAPI,
  PROVIDER_IDS.BRAVE,
  PROVIDER_IDS.SURFE,
  PROVIDER_IDS.HYBRID_WATERFALL,
]);

/**
 * @typedef {object} ProviderCallRequest
 * @property {string} provider
 * @property {string} operation
 * @property {object} [params]
 * @property {string} [hotelId]
 * @property {string} [runId]
 * @property {string} [caseId]
 * @property {string} [operationId]
 */

/**
 * Create a provider gateway bound to a budget ledger.
 * By default refuses paid calls (P1). Adapters are sealed against bypass.
 *
 * @param {object} [opts]
 * @param {object} [opts.ledger] — budget ledger with reserve/settle (created if omitted)
 * @param {boolean} [opts.paidProvidersEnabled]
 * @param {boolean} [opts.surfeEnabled]
 * @param {object} [opts.adapters]
 */
export function createProviderGateway(opts = {}) {
  const ledger =
    opts.ledger ||
    createBudgetLedger({
      perHotelUsdCap: opts.perHotelUsdCap ?? null,
      perOrgUsdCap: opts.perOrgUsdCap ?? null,
      perHotelCreditCap: opts.perHotelCreditCap ?? null,
    });

  if (typeof ledger.reserve !== "function" || typeof ledger.settle !== "function") {
    throw new Error("provider_gateway_requires_ledger_with_reserve_settle");
  }

  const paidEnabled =
    opts.paidProvidersEnabled === true && P1_POLICY.paid_providers_enabled === true;
  const surfeEnabled =
    opts.surfeEnabled === true && P1_POLICY.surfe_enabled === true;

  const execution = createExecutionGateway({
    ledger,
    adapters: opts.adapters || {},
    paidProvidersEnabled: opts.paidProvidersEnabled === true,
    surfeEnabled: opts.surfeEnabled === true,
  });

  return {
    call: (req) => execution.execute(req),
    execute: (req) => execution.execute(req),
    getUsage: () => execution.getUsage(),
    getLedger: () => ledger,
    getExecutionGateway: () => execution,
    attemptDirectAdapterCall: (provider, req) =>
      execution.attemptDirectAdapterCall(provider, req),
    paidEnabled,
    surfeEnabled,
    execution_gateway_version: EXECUTION_GATEWAY_VERSION,
    listProviders: () => Object.values(PROVIDER_IDS),
  };
}

/** Offline mock adapter for deterministic harness (cost explicitly known). */
export function createMockProviderAdapter({ costUsd = 0, credits = 0 } = {}) {
  return {
    estimateCost: () => ({
      cost_usd: costUsd,
      cost_usd_status: "ESTIMATED",
      credits,
    }),
    call: async (req) => ({
      ok: true,
      provider: PROVIDER_IDS.MOCK,
      operation: req.operation,
      facts: [],
      cost_usd: costUsd,
      cost_usd_status: "REPORTED",
      credits,
      source_attribution: ["mock://deterministic"],
    }),
  };
}

export { buildOperationId, EXECUTION_GATEWAY_VERSION, createExecutionGateway };
export { FAILURE_CLASS };
