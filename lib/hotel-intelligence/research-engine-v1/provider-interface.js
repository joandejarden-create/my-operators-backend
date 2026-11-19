/**
 * Provider-neutral research interface for HE-INTEL-001.
 * P1: all paid providers refused. Same evidence gates for every future arm.
 */

import { FAILURE_CLASS, P1_POLICY } from "./contract.js";

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
 */

/**
 * Create a provider gateway. By default refuses paid calls (P1).
 */
export function createProviderGateway(opts = {}) {
  const paidEnabled = opts.paidProvidersEnabled === true && P1_POLICY.paid_providers_enabled === true;
  const surfeEnabled = opts.surfeEnabled === true && P1_POLICY.surfe_enabled === true;
  const adapters = opts.adapters || {};
  const usage = [];

  function record(entry) {
    usage.push({
      at: new Date().toISOString(),
      ...entry,
    });
  }

  async function call(req = {}) {
    const provider = String(req.provider || "").trim();
    const operation = String(req.operation || "").trim();
    if (!provider || !operation) {
      return {
        ok: false,
        error: FAILURE_CLASS.INPUT_INVALID,
        message: "provider and operation required",
      };
    }

    if (provider === PROVIDER_IDS.SURFE && !surfeEnabled) {
      record({
        provider,
        operation,
        status: "REFUSED",
        reason: FAILURE_CLASS.PROVIDER_UNAUTHORIZED,
        cost_usd: null,
        cost_usd_status: "NOT_APPLICABLE",
      });
      return { ok: false, error: FAILURE_CLASS.PROVIDER_UNAUTHORIZED, provider, operation };
    }

    if (PAID_PROVIDERS.has(provider) && !paidEnabled) {
      record({
        provider,
        operation,
        status: "REFUSED",
        reason: FAILURE_CLASS.PROVIDER_UNAUTHORIZED,
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        note: "P1 paid providers disabled; unknown cost must not be reported as zero",
      });
      return {
        ok: false,
        error: FAILURE_CLASS.PROVIDER_UNAUTHORIZED,
        provider,
        operation,
        message: "paid_provider_disabled_in_p1",
      };
    }

    const adapter = adapters[provider];
    if (!adapter || typeof adapter.call !== "function") {
      record({
        provider,
        operation,
        status: "REFUSED",
        reason: "ADAPTER_MISSING",
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
      });
      return { ok: false, error: "ADAPTER_MISSING", provider, operation };
    }

    const result = await adapter.call(req);
    const costUsd =
      result?.cost_usd === 0 || Number.isFinite(Number(result?.cost_usd))
        ? Number(result.cost_usd)
        : null;
    const costStatus =
      result?.cost_usd_status ||
      (costUsd == null ? "UNKNOWN" : "REPORTED");
    // Never coerce missing cost to 0
    record({
      provider,
      operation,
      status: result?.ok ? "OK" : "FAILED",
      cost_usd: costUsd,
      cost_usd_status: costStatus,
      credits: result?.credits ?? null,
      tokens: result?.tokens ?? null,
      retry: Boolean(req.retry),
      hotel_id: req.hotelId || null,
      run_id: req.runId || null,
      source_attribution: result?.source_attribution || null,
    });
    return result;
  }

  return {
    call,
    getUsage: () => [...usage],
    paidEnabled,
    surfeEnabled,
    listProviders: () => Object.values(PROVIDER_IDS),
  };
}

/** Offline mock adapter for deterministic harness (cost explicitly known). */
export function createMockProviderAdapter({ costUsd = 0 } = {}) {
  return {
    call: async (req) => ({
      ok: true,
      provider: PROVIDER_IDS.MOCK,
      operation: req.operation,
      facts: [],
      cost_usd: costUsd,
      cost_usd_status: "REPORTED",
      credits: 0,
      source_attribution: ["mock://deterministic"],
    }),
  };
}
