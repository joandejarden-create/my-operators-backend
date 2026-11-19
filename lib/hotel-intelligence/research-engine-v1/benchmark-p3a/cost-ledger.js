/**
 * Benchmark cost / credit ledger with hard ceilings and shared-budget enforcement.
 * Extends P1 budget ledger semantics; unknown charges stay UNKNOWN (never $0).
 */

import { createBudgetLedger, BUDGET_LEDGER_VERSION } from "../budget-ledger.js";
import { COST_STATUS, P3A_POLICY } from "./contract.js";

export const BENCHMARK_COST_LEDGER_VERSION = "he-intel-p3a-cost-ledger-v1";

export function createBenchmarkCostLedger(opts = {}) {
  const runUsdCap = opts.runUsdCap ?? 0; // P3A default $0
  const perHotelUsdCap = opts.perHotelUsdCap ?? 0;
  const perOrgUsdCap = opts.perOrgUsdCap ?? null;
  const sharedBudgetUsd = opts.sharedBudgetUsd ?? runUsdCap;
  const allowPaid = opts.allowPaid === true && P3A_POLICY.paid_providers_enabled === true;

  const base = createBudgetLedger({
    perHotelUsdCap: allowPaid ? perHotelUsdCap : 0,
    perOrgUsdCap,
    perHotelCreditCap: opts.perHotelCreditCap ?? null,
  });

  const entries = [];
  let runUsd = 0;
  let runUsdStatus = COST_STATUS.ZERO_AUTHORIZED;
  let refused = 0;
  const stopReasons = [];

  function append(entry) {
    entries.push({
      at: new Date().toISOString(),
      ...entry,
    });
  }

  function refuse(reason, meta = {}) {
    refused += 1;
    stopReasons.push({ reason, ...meta, at: new Date().toISOString() });
    append({
      kind: "REFUSED",
      reason,
      cost_usd: null,
      cost_usd_status: COST_STATUS.UNKNOWN,
      ...meta,
    });
    return { ok: false, error: reason, ...meta };
  }

  function recordUsage(input = {}) {
    if (!allowPaid && Number(input.costUsd || 0) > 0) {
      return refuse("P3A_PAID_SPEND_FORBIDDEN", {
        provider: input.provider,
        operation: input.operation,
      });
    }
    if (!allowPaid) {
      append({
        kind: "DRY_OR_ZERO",
        provider: input.provider,
        operation: input.operation,
        hotelId: input.hotelId,
        orgKey: input.orgKey,
        armId: input.armId,
        mode: input.mode,
        search_requests: Number(input.searchRequests || 0),
        model_input_tokens: Number(input.modelInputTokens || 0),
        model_output_tokens: Number(input.modelOutputTokens || 0),
        provider_credits: Number(input.credits || 0),
        failed: Boolean(input.failed),
        retry: Boolean(input.retry),
        cost_usd: 0,
        cost_usd_status: COST_STATUS.ZERO_AUTHORIZED,
        estimated_usd: input.estimatedUsd ?? null,
        estimated_usd_status: input.estimatedUsdStatus || COST_STATUS.UNKNOWN,
        actual_vs_estimated:
          input.estimatedUsd == null ? "UNKNOWN" : "ACTUAL_ZERO_AUTHORIZED",
      });
      return { ok: true, cost_usd: 0, cost_usd_status: COST_STATUS.ZERO_AUTHORIZED };
    }

    // Paid path (P3B+) — shared budget cannot be bypassed by parallel arms
    const usd =
      input.costUsd == null || input.costUsdStatus === COST_STATUS.UNKNOWN
        ? null
        : Number(input.costUsd);
    if (usd == null) {
      return refuse("UNKNOWN_COST_UNDER_CAP", { provider: input.provider });
    }
    if (runUsd + usd > sharedBudgetUsd) {
      return refuse("SHARED_BUDGET_EXHAUSTED", {
        runUsd,
        attempted: usd,
        sharedBudgetUsd,
      });
    }

    const reserve = base.reserve({
      hotelId: input.hotelId,
      orgKey: input.orgKey,
      costUsd: usd,
      credits: input.credits || 0,
      operationId: input.operationId,
      provider: input.provider,
      operation: input.operation,
    });
    if (!reserve.ok) {
      return refuse(reserve.error || "BUDGET_EXHAUSTED", reserve);
    }
    base.settle({
      operationId: reserve.operationId || input.operationId,
      costUsd: usd,
      credits: input.credits || 0,
      ok: !input.failed,
    });
    runUsd = Number((runUsd + usd).toFixed(6));
    runUsdStatus = COST_STATUS.REPORTED;
    append({
      kind: "USAGE",
      ...input,
      cost_usd: usd,
      cost_usd_status: COST_STATUS.REPORTED,
    });
    return { ok: true, cost_usd: usd, cost_usd_status: COST_STATUS.REPORTED, runUsd };
  }

  function snapshot() {
    return {
      version: BENCHMARK_COST_LEDGER_VERSION,
      parent_ledger: BUDGET_LEDGER_VERSION,
      allowPaid,
      runUsdCap,
      perHotelUsdCap,
      sharedBudgetUsd,
      runUsd,
      runUsdStatus: allowPaid ? runUsdStatus : COST_STATUS.ZERO_AUTHORIZED,
      refused,
      stopReasons,
      entries: [...entries],
      parentSnapshot: typeof base.snapshot === "function" ? base.snapshot() : null,
    };
  }

  return {
    recordUsage,
    refuse,
    snapshot,
    getBaseLedger: () => base,
    canParallelBypassSharedBudget: () => false,
    allowPaid,
  };
}
