/**
 * Fail-closed provider execution gateway.
 *
 * Every payable (and mock) provider call must:
 *   1) pass authorization
 *   2) validate remaining budget
 *   3) reserve cost/credits before execution
 *   4) claim exclusive dispatch (durable lease) before live call
 *   5) invoke sealed adapter.call under operation-scoped authorization
 *   6) reconcile actual usage afterward
 *
 * Adapters must not be invoked outside this gateway.
 * Sealed adapters do NOT expose a callable raw transport bypass.
 */

import crypto from "node:crypto";
import { FAILURE_CLASS, P1_POLICY } from "./contract.js";

export const EXECUTION_GATEWAY_VERSION = "he-intel-execution-gateway-v1.1";

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
 * Authorization is operation-scoped. No externally callable __rawCall bypass.
 */
export function sealProviderAdapters(
  adapters = {},
  gatewayRef = { authorizedOperationIds: new Set() }
) {
  if (!gatewayRef.authorizedOperationIds) {
    gatewayRef.authorizedOperationIds = new Set();
  }
  gatewayRef.active = false;
  const sealed = {};
  for (const [id, adapter] of Object.entries(adapters || {})) {
    if (!adapter || typeof adapter.call !== "function") {
      sealed[id] = adapter;
      continue;
    }
    const boundCall = adapter.call.bind(adapter);
    sealed[id] = {
      ...adapter,
      // Explicitly refuse raw bypass — gateway must use sealed .call
      __rawCall: undefined,
      get __rawTransport() {
        const err = new Error("PROVIDER_ADAPTER_BYPASS_FORBIDDEN");
        err.code = "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
        throw err;
      },
      call: async (req = {}, ...rest) => {
        const opId = String(req?.operation_id || req?.operationId || "").trim();
        if (!opId || !gatewayRef.authorizedOperationIds.has(opId)) {
          const err = new Error("PROVIDER_ADAPTER_BYPASS_FORBIDDEN");
          err.code = "PROVIDER_ADAPTER_BYPASS_FORBIDDEN";
          throw err;
        }
        return boundCall(req, ...rest);
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
  const workerId =
    opts.workerId || ledger.workerId || `gw_${process.pid}_${crypto.randomBytes(3).toString("hex")}`;

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
   * Execute one provider operation through authorization → reserve → claim → sealed call → settle.
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

    if (typeof ledger.isFatal === "function" && ledger.isFatal()) {
      const fatal = ledger.getFatal?.() || { error: "LEDGER_FATAL" };
      return {
        ok: false,
        error: fatal.error || "LEDGER_FATAL",
        fatal: true,
        message: fatal.reason || "ledger_fatal_fail_closed",
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
    if (prior?.status === "CLAIMED") {
      return {
        ok: false,
        error: "OPERATION_IN_FLIGHT",
        provider,
        operation,
        operation_id: operationId,
        message: "Operation already claimed for dispatch",
        operation_state: prior,
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
    if (!adapter || typeof adapter.call !== "function") {
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
        : provider === GATEWAY_PROVIDER_IDS.MOCK
          ? {
              cost_usd: Number(req.estimated_cost_usd ?? 0) || 0,
              cost_usd_status: "ZERO_AUTHORIZED",
              credits: req.estimated_credits ?? 0,
              bounded: true,
            }
          : {
              cost_usd: req.estimated_cost_usd ?? null,
              cost_usd_status:
                req.estimated_cost_usd == null ? "UNKNOWN" : "ESTIMATED",
              credits: req.estimated_credits ?? 0,
              bounded: Number.isFinite(Number(req.estimated_cost_usd)),
            };

    // Fail-closed: unbounded / UNKNOWN cost cannot proceed (MOCK zero path excluded)
    if (
      estimate.cost_usd_status === "UNKNOWN" ||
      estimate.bounded === false ||
      !Number.isFinite(Number(estimate.cost_usd))
    ) {
      record({
        provider,
        operation,
        operation_id: operationId,
        status: "REFUSED",
        reason: FAILURE_CLASS.BUDGET_EXHAUSTED,
        note: "fail_closed_unbounded_cost",
      });
      return {
        ok: false,
        error: FAILURE_CLASS.BUDGET_EXHAUSTED,
        provider,
        operation,
        operation_id: operationId,
        scope: "usd_unknown_under_cap",
        message: "cost_cannot_be_bounded",
      };
    }

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
        in_flight: reservation.error === "OPERATION_IN_FLIGHT",
      };
    }

    // Durable claim — only a newly acquired claim permits adapter dispatch
    let claim = { ok: false, newly_acquired: false, claim_token: null };
    if (typeof ledger.claimDispatch === "function") {
      claim = ledger.claimDispatch(operationId, { workerId });
      if (
        !claim.ok ||
        claim.newly_acquired !== true ||
        claim.already_claimed === true ||
        !claim.claim_token
      ) {
        record({
          provider,
          operation,
          operation_id: operationId,
          status: "CLAIM_REFUSED",
          reason: claim.error || "CLAIM_NOT_NEWLY_ACQUIRED",
        });
        return {
          ok: false,
          error: claim.error || "OPERATION_IN_FLIGHT",
          provider,
          operation,
          operation_id: operationId,
          already_settled: claim.already_settled === true,
          in_flight: true,
          message:
            claim.note ||
            "Only a newly acquired claim permits provider dispatch",
        };
      }
    } else if (reservation.already_reserved || reservation.in_flight) {
      // Without claim API, refuse second dispatch on existing RESERVED
      return {
        ok: false,
        error: "OPERATION_IN_FLIGHT",
        provider,
        operation,
        operation_id: operationId,
        message: "RESERVED without claim API — refuse duplicate dispatch",
      };
    }
    // P1 in-memory ledgers without claimDispatch: first successful reserve may dispatch once.

    let result;
    gatewayRef.authorizedOperationIds.add(operationId);
    try {
      // Enforce sealed contract — never call unbound raw transport
      result = await adapter.call({
        ...req,
        operation_id: operationId,
        operationId,
        claim_token: claim.claim_token || null,
      });
      if (typeof ledger.markDispatched === "function") {
        ledger.markDispatched(operationId, {
          claim_token: claim.claim_token,
          provider_run_id:
            result?.parallel_run?.run_id ||
            result?.provider_run_id ||
            result?.run_id ||
            null,
          provider_status: result?.parallel_run?.status || result?.provider_status || null,
        });
      }
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

    if (
      (costStatus === "UNKNOWN" || result?.billing_uncertain === true) &&
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

    // Task created without completion is not a successful research outcome
    if (result?.task_created_only === true && result?.task_completed !== true) {
      const settlePartial = ledger.settle(operationId, {
        status: "TASK_CREATED_INCOMPLETE",
        cost_usd: costUsd,
        cost_usd_status: costStatus,
        credits: result?.credits ?? 0,
        provider_cost_usd: result?.provider_cost_usd ?? costUsd,
        model_token_cost_usd: result?.model_token_cost_usd || 0,
        orchestration_cost_usd: result?.orchestration_cost_usd || 0,
        result: {
          ok: false,
          provider,
          operation,
          task_created_only: true,
          provider_run_id: result?.parallel_run?.run_id || null,
        },
      });
      return {
        ok: false,
        error: "TASK_CREATED_NOT_COMPLETE",
        provider,
        operation,
        operation_id: operationId,
        parallel_run: result?.parallel_run || null,
        settled: Boolean(settlePartial?.ok),
        message: "Task creation is not a successful ownership research outcome",
        research_success: false,
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
      research_success: Boolean(result?.ok) && result?.task_created_only !== true,
    };
  }

  return {
    version: EXECUTION_GATEWAY_VERSION,
    execute,
    call: execute,
    getUsage: () => [...usage],
    getAdapters: () => adapters,
    paidEnabled,
    surfeEnabled,
    workerId,
    /** Test helper: attempt sealed adapter call without auth (must throw). */
    attemptDirectAdapterCall: async (provider, req = {}) => {
      const adapter = adapters[provider];
      if (!adapter) throw new Error("ADAPTER_MISSING");
      return adapter.call(req);
    },
    /** Adversarial: attempt to reach raw bypass (must throw). */
    attemptRawBypass: async (provider, req = {}) => {
      const adapter = adapters[provider];
      if (!adapter) throw new Error("ADAPTER_MISSING");
      if (typeof adapter.__rawCall === "function") {
        return adapter.__rawCall(req);
      }
      // Accessing __rawTransport getter throws
      return adapter.__rawTransport;
    },
  };
}
