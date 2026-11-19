/**
 * Fail-closed provider execution gateway.
 *
 * Every payable (and mock) provider call must:
 *   1) pass authorization
 *   2) validate remaining budget
 *   3) reserve cost/credits before execution
 *   4) reconcile actual usage afterward
 *   5) account retries/failures
 *   6) use durable operation identity (no duplicate spend on resume)
 *
 * Adapters must not be invoked outside this gateway.
 *
 * P1: paid providers remain disabled (P1_POLICY). Full live reservation
 * semantics for multi-provider P3 benchmarking may deepen later; the
 * fail-closed contract (auth → reserve → execute → settle, no bypass) is
 * established and tested here.
 */

import crypto from "node:crypto";
import { FAILURE_CLASS, P1_POLICY } from "./contract.js";

export const EXECUTION_GATEWAY_VERSION = "he-intel-execution-gateway-v1";

/** Local provider id constants — avoid circular import with provider-interface. */
export const GATEWAY_PROVIDER_IDS = Object.freeze({
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

export const GATEWAY_PAID_PROVIDERS = new Set([
  GATEWAY_PROVIDER_IDS.EXA,
  GATEWAY_PROVIDER_IDS.PARALLEL_TASK,
  GATEWAY_PROVIDER_IDS.PARALLEL_SEARCH,
  GATEWAY_PROVIDER_IDS.SERPAPI,
  GATEWAY_PROVIDER_IDS.BRAVE,
  GATEWAY_PROVIDER_IDS.SURFE,
  GATEWAY_PROVIDER_IDS.HYBRID_WATERFALL,
]);

export function buildOperationId({
  caseId = null,
  hotelId = null,
  provider,
  operation,
  params = {},
  attempt = 0,
} = {}) {
  const payload = JSON.stringify({
    caseId: caseId || null,
    hotelId: hotelId || null,
    provider: String(provider || ""),
    operation: String(operation || ""),
    params,
    attempt: Number(attempt) || 0,
  });
  const digest = crypto.createHash("sha256").update(payload).digest("hex").slice(0, 24);
  return `op_${digest}`;
}

/**
 * Seal adapter map so callers cannot invoke adapter.call without going through gateway.
 * Authorization is operation-scoped (authorizedOperationIds Set) — not a shared mutable
 * boolean that concurrent workers could race. Direct calls throw
 * PROVIDER_ADAPTER_BYPASS_FORBIDDEN.
 */
export function sealProviderAdapters(
  adapters = {},
  gatewayRef = { authorizedOperationIds: new Set() }
) {
  if (!gatewayRef.authorizedOperationIds) {
    gatewayRef.authorizedOperationIds = new Set();
  }
  // Legacy boolean kept false forever; op-scoped Set is the only path.
  gatewayRef.active = false;
  const sealed = {};
  for (const [id, adapter] of Object.entries(adapters || {})) {
    if (!adapter || typeof adapter.call !== "function") {
      sealed[id] = adapter;
      continue;
    }
    const rawCall = adapter.call.bind(adapter);
    sealed[id] = {
      ...adapter,
      __rawCall: rawCall,
      call: async (req = {}, ...rest) => {
        const opId = String(req?.operation_id || req?.operationId || "").trim();
        if (!opId || !gatewayRef.authorizedOperationIds.has(opId)) {
          const err = new Error("PROVIDER_ADAPTER_BYPASS_FORBIDDEN");
          err.code = "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
          throw err;
        }
        return rawCall(req, ...rest);
      },
    };
  }
  return sealed;
}

function isAmbiguousProviderFailure(err) {
  const code = String(err?.code || "").toUpperCase();
  const msg = String(err?.message || err || "").toUpperCase();
  if (
    code.includes("TIMEOUT") ||
    code.includes("ABORT") ||
    code === "PROVIDER_TIMEOUT" ||
    code === "BILLING_UNKNOWN" ||
    code === "AMBIGUOUS_PROVIDER_OUTCOME"
  ) {
    return true;
  }
  if (err?.billing_status === "UNKNOWN" || err?.ambiguous === true) return true;
  if (msg.includes("TIMEOUT") || msg.includes("ABORT") || msg.includes("ECONNRESET")) {
    return true;
  }
  return false;
}

/**
 * Create execution gateway bound to a budget ledger with reserve/settle/release.
 */
export function createExecutionGateway(opts = {}) {
  const ledger = opts.ledger;
  if (!ledger || typeof ledger.reserve !== "function" || typeof ledger.settle !== "function") {
    throw new Error("execution_gateway_requires_budget_ledger_with_reserve_settle");
  }

  const founderAuth = opts.founderPaidAuthorization || null;
  const benchmarkPaidOverride =
    opts.allowBenchmarkPaidOverride === true &&
    opts.paidProvidersEnabled === true &&
    founderAuth?.ok === true &&
    String(founderAuth?.authId || "") ===
      "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1";
  const paidEnabled =
    (opts.paidProvidersEnabled === true &&
      P1_POLICY.paid_providers_enabled === true) ||
    benchmarkPaidOverride;
  const surfeEnabled =
    opts.surfeEnabled === true && P1_POLICY.surfe_enabled === true;

  const gatewayRef = { active: false, authorizedOperationIds: new Set() };
  const adapters = sealProviderAdapters(opts.adapters || {}, gatewayRef);
  const usage = [];

  function record(entry) {
    usage.push({ at: new Date().toISOString(), ...entry });
  }

  function authorize(provider) {
    if (provider === GATEWAY_PROVIDER_IDS.SURFE && !surfeEnabled) {
      return {
        ok: false,
        error: FAILURE_CLASS.PROVIDER_UNAUTHORIZED,
        message: "surfe_disabled",
      };
    }
    if (GATEWAY_PAID_PROVIDERS.has(provider) && !paidEnabled) {
      return {
        ok: false,
        error: FAILURE_CLASS.PROVIDER_UNAUTHORIZED,
        message: benchmarkPaidOverride
          ? "paid_provider_disabled"
          : "paid_provider_disabled_in_p1",
      };
    }
    return { ok: true };
  }

  /**
   * Execute one provider operation through authorization → reserve → call → settle.
   */
  async function execute(req = {}) {
    const provider = String(req.provider || "").trim();
    const operation = String(req.operation || "").trim();
    if (!provider || !operation) {
      return {
        ok: false,
        error: FAILURE_CLASS.INPUT_INVALID,
        message: "provider and operation required",
      };
    }

    const attempt = Number(req.attempt || (req.retry ? 1 : 0)) || 0;
    const operationId =
      req.operationId ||
      buildOperationId({
        caseId: req.caseId,
        hotelId: req.hotelId,
        provider,
        operation,
        params: req.params || {},
        attempt,
      });

    // Resume: already settled → no re-spend
    const prior = ledger.getOperation?.(operationId);
    if (prior?.status === "SETTLED") {
      record({
        provider,
        operation,
        operation_id: operationId,
        status: "REPLAY_SETTLED",
        cost_usd: prior.actual_usd ?? null,
        cost_usd_status: prior.actual_usd_status || "UNKNOWN",
        credits: prior.actual_credits ?? 0,
        hotel_id: req.hotelId || null,
        run_id: req.runId || null,
        retry: attempt > 0,
      });
      return {
        ok: true,
        replayed: true,
        operation_id: operationId,
        result: prior.result || { ok: true, replayed: true },
        cost_usd: prior.actual_usd ?? null,
        cost_usd_status: prior.actual_usd_status || "UNKNOWN",
      };
    }
    if (prior?.status === "PENDING_RECONCILE") {
      record({
        provider,
        operation,
        operation_id: operationId,
        status: "PENDING_RECONCILE_BLOCKS_RETRY",
        cost_usd: prior.reserved_usd ?? null,
        cost_usd_status: "UNKNOWN",
        hotel_id: req.hotelId || null,
        retry: attempt > 0,
      });
      return {
        ok: false,
        error: "PENDING_RECONCILE_BLOCKS_RETRY",
        provider,
        operation,
        operation_id: operationId,
        message: "Ambiguous prior outcome must be reconciled before reuse",
      };
    }

    const auth = authorize(provider);
    if (!auth.ok) {
      ledger.recordUsage?.({
        hotel_id: req.hotelId,
        org_key: req.orgKey,
        provider,
        operation,
        operation_id: operationId,
        status: "REFUSED",
        reason: auth.error,
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        credits: 0,
        retry: attempt > 0,
      });
      record({
        provider,
        operation,
        operation_id: operationId,
        status: "REFUSED",
        reason: auth.error,
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        hotel_id: req.hotelId || null,
        run_id: req.runId || null,
        retry: attempt > 0,
      });
      return {
        ok: false,
        error: auth.error,
        provider,
        operation,
        operation_id: operationId,
        message: auth.message,
      };
    }

    const adapter = adapters[provider];
    if (!adapter || typeof adapter.__rawCall !== "function") {
      record({
        provider,
        operation,
        operation_id: operationId,
        status: "REFUSED",
        reason: "ADAPTER_MISSING",
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
      });
      return {
        ok: false,
        error: "ADAPTER_MISSING",
        provider,
        operation,
        operation_id: operationId,
      };
    }

    const estimate =
      typeof adapter.estimateCost === "function"
        ? adapter.estimateCost(req)
        : {
            cost_usd: req.estimated_cost_usd ?? null,
            cost_usd_status:
              req.estimated_cost_usd == null ? "UNKNOWN" : "ESTIMATED",
            credits: req.estimated_credits ?? 0,
          };

    const reservation = ledger.reserve({
      operation_id: operationId,
      hotel_id: req.hotelId,
      org_key: req.orgKey,
      provider,
      operation,
      cost_usd: estimate.cost_usd,
      cost_usd_status: estimate.cost_usd_status,
      credits: estimate.credits ?? 0,
      retry: attempt > 0,
    });
    if (!reservation.ok) {
      record({
        provider,
        operation,
        operation_id: operationId,
        status: "REFUSED",
        reason: reservation.error || FAILURE_CLASS.BUDGET_EXHAUSTED,
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        hotel_id: req.hotelId || null,
        retry: attempt > 0,
      });
      return {
        ok: false,
        error: reservation.error || FAILURE_CLASS.BUDGET_EXHAUSTED,
        provider,
        operation,
        operation_id: operationId,
        scope: reservation.scope || null,
      };
    }

    let result;
    gatewayRef.authorizedOperationIds.add(operationId);
    try {
      result = await adapter.__rawCall({
        ...req,
        operation_id: operationId,
        operationId,
      });
    } catch (err) {
      const ambiguous = isAmbiguousProviderFailure(err);
      if (ambiguous && typeof ledger.markPendingReconcile === "function") {
        ledger.markPendingReconcile(operationId, {
          reason: "AMBIGUOUS_PROVIDER_OUTCOME",
          error: String(err?.code || err?.message || err),
          dispatched: err?.dispatched === true,
        });
      } else {
        ledger.release(operationId, {
          reason: "EXECUTION_THREW",
          error: String(err?.code || err?.message || err),
        });
      }
      record({
        provider,
        operation,
        operation_id: operationId,
        status: ambiguous ? "PENDING_RECONCILE" : "FAILED",
        reason: String(err?.code || err?.message || err),
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        hotel_id: req.hotelId || null,
        retry: attempt > 0,
      });
      return {
        ok: false,
        error: ambiguous
          ? "PENDING_RECONCILE"
          : FAILURE_CLASS.PROVIDER_FAILED,
        provider,
        operation,
        operation_id: operationId,
        message: String(err?.message || err),
        pending_reconcile: ambiguous,
      };
    } finally {
      gatewayRef.authorizedOperationIds.delete(operationId);
      gatewayRef.active = false;
    }

    const costUsd =
      result?.cost_usd === 0 || Number.isFinite(Number(result?.cost_usd))
        ? Number(result.cost_usd)
        : null;
    const costStatus =
      result?.cost_usd_status || (costUsd == null ? "UNKNOWN" : "REPORTED");

    // Fail-closed: result with unknown/unbounded cost after paid dispatch → pending
    if (
      costStatus === "UNKNOWN" &&
      result?.billing_uncertain === true &&
      typeof ledger.markPendingReconcile === "function"
    ) {
      ledger.markPendingReconcile(operationId, {
        reason: "UNKNOWN_BILLING_AFTER_DISPATCH",
      });
      return {
        ok: false,
        error: "PENDING_RECONCILE",
        provider,
        operation,
        operation_id: operationId,
        pending_reconcile: true,
        message: "Provider outcome billed uncertainly; held for reconcile",
      };
    }

    const settle = ledger.settle(operationId, {
      status: result?.ok ? "OK" : "FAILED",
      cost_usd: costUsd,
      cost_usd_status: costStatus,
      credits: result?.credits ?? 0,
      tokens: result?.tokens ?? null,
      provider_cost_usd: result?.provider_cost_usd ?? costUsd,
      model_token_cost_usd: result?.model_token_cost_usd || 0,
      orchestration_cost_usd: result?.orchestration_cost_usd || 0,
      result: { ok: Boolean(result?.ok), provider, operation },
    });

    record({
      provider,
      operation,
      operation_id: operationId,
      status: result?.ok ? "OK" : "FAILED",
      cost_usd: costUsd,
      cost_usd_status: costStatus,
      credits: result?.credits ?? null,
      tokens: result?.tokens ?? null,
      retry: attempt > 0,
      hotel_id: req.hotelId || null,
      run_id: req.runId || null,
      source_attribution: result?.source_attribution || null,
      settled: Boolean(settle?.ok),
    });

    return {
      ...result,
      operation_id: operationId,
      reservation_id: reservation.reservation_id || operationId,
    };
  }

  return {
    version: EXECUTION_GATEWAY_VERSION,
    execute,
    /** Alias used by createProviderGateway.call */
    call: execute,
    getUsage: () => [...usage],
    getAdapters: () => adapters,
    paidEnabled,
    surfeEnabled,
    /** Test helper: attempt raw sealed adapter call (must throw). */
    attemptDirectAdapterCall: async (provider, req = {}) => {
      const adapter = adapters[provider];
      if (!adapter) throw new Error("ADAPTER_MISSING");
      return adapter.call(req);
    },
  };
}
