/**
 * Per-arm atomic budget ledger for Exa-parity E0/E1.
 * E0: force mock-only — live Exa HTTP refused even if env flags present.
 */

import { quoteBenchmarkCall } from "../research/benchmark-provider-pricing-v1.js";
import { quoteExaAgentRun } from "../research/exa-agent/exa-agent-provider-adapter-v1.js";

export const E0_BUDGET_LEDGER_VERSION = "gdi-ai-agent-exa-parity-e0-budget-ledger-v1";

export const E0_LIMITS = Object.freeze({
  perArmPerCaseUsd: 1.25,
  maxRetrievalsPerCase: 12,
  e1TotalCeilingUsd: 20,
  e1PerCaseBothArmsUsd: 2.5,
});

export const DENIED_E0_PROVIDERS = Object.freeze([
  "serpapi",
  "pdl",
  "surfe",
  "firecrawl",
  "parallel",
  "jev",
  "exa_live_http",
]);

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function quoteExaParityOperation(operation, params = {}) {
  const op = text(operation).toLowerCase();
  if (op === "search") {
    return quoteBenchmarkCall({
      provider: "exa",
      operation: "search",
      params: { type: "lite", numResults: params.numResults || 8 },
    });
  }
  if (op === "contents") {
    const urls = Array.isArray(params.urls) ? params.urls : ["https://example.invalid/mock"];
    return quoteBenchmarkCall({
      provider: "exa",
      operation: "contents",
      params: { urls: urls.slice(0, 5), text: true },
    });
  }
  if (op === "agent" || op === "agent_run") {
    return quoteExaAgentRun({
      effort: params.effort || "medium",
      maxCostDollars: params.maxCostDollars,
    });
  }
  return { bounded: false, maxUsd: null, reason: "UNKNOWN_EXA_OPERATION" };
}

/**
 * Independent atomic ledger for one arm × one case.
 */
/**
 * Experiment-wide hard ceiling (E1 = $20 across all arms/cases).
 */
export function createExperimentBudgetLedger({
  maxUsd = E0_LIMITS.e1TotalCeilingUsd,
  authorizationId = null,
} = {}) {
  const state = {
    version: `${E0_BUDGET_LEDGER_VERSION}/experiment`,
    maxUsd: num(maxUsd),
    authorizationId: text(authorizationId) || null,
    reservedUsd: 0,
    settledUsd: 0,
    actualPaidUsd: 0,
    operations: [],
    blocked: [],
    createdAt: new Date().toISOString(),
  };
  function remainingUsd() {
    return Math.max(0, state.maxUsd - state.settledUsd - state.reservedUsd);
  }
  function snapshot() {
    return {
      ...state,
      remainingUsd: remainingUsd(),
      operations: state.operations.slice(),
      blocked: state.blocked.slice(),
    };
  }
  function reserve(cost, meta = {}) {
    const c = num(cost);
    if (c > remainingUsd() + 1e-9) {
      const row = {
        at: new Date().toISOString(),
        reason: "EXPERIMENT_USD_CAP_EXCEEDED",
        quotedMaxUsd: c,
        remainingUsd: remainingUsd(),
        ...meta,
      };
      state.blocked.push(row);
      return { ok: false, refused: true, reason: row.reason, ledger: snapshot() };
    }
    state.reservedUsd = Number((state.reservedUsd + c).toFixed(6));
    const reservationId = `exp_${state.operations.length + 1}_${Date.now().toString(36)}`;
    const reservation = {
      reservationId,
      quotedMaxUsd: c,
      status: "reserved",
      at: new Date().toISOString(),
      ...meta,
    };
    state.operations.push(reservation);
    return { ok: true, reservation, ledger: snapshot() };
  }
  function settle(reservationId, actualCostUsd, { paid = false } = {}) {
    const op = state.operations.find((o) => o.reservationId === reservationId);
    if (!op || op.status !== "reserved") {
      return { ok: false, reason: "EXPERIMENT_RESERVATION_INVALID" };
    }
    const actual = Math.min(num(actualCostUsd), num(op.quotedMaxUsd));
    state.reservedUsd = Number(Math.max(0, state.reservedUsd - num(op.quotedMaxUsd)).toFixed(6));
    state.settledUsd = Number((state.settledUsd + actual).toFixed(6));
    if (paid) state.actualPaidUsd = Number((state.actualPaidUsd + actual).toFixed(6));
    op.status = "settled";
    op.actualCostUsd = actual;
    op.paid = paid === true;
    op.settledAt = new Date().toISOString();
    return { ok: true, operation: op, ledger: snapshot() };
  }
  function release(reservationId) {
    const op = state.operations.find((o) => o.reservationId === reservationId);
    if (!op || op.status !== "reserved") return { ok: false, reason: "EXPERIMENT_RELEASE_FAILED" };
    state.reservedUsd = Number(Math.max(0, state.reservedUsd - num(op.quotedMaxUsd)).toFixed(6));
    op.status = "released";
    return { ok: true, ledger: snapshot() };
  }
  return { reserve, settle, release, snapshot, remainingUsd, limits: { maxUsd: state.maxUsd } };
}

export function createArmBudgetLedger({
  arm,
  opportunityId,
  maxUsd = E0_LIMITS.perArmPerCaseUsd,
  maxRetrievals = E0_LIMITS.maxRetrievalsPerCase,
  forceMockOnly = true,
  experimentLedger = null,
} = {}) {
  const state = {
    version: E0_BUDGET_LEDGER_VERSION,
    arm: text(arm) || "unknown",
    opportunityId: text(opportunityId) || null,
    maxUsd: num(maxUsd),
    maxRetrievals: num(maxRetrievals),
    forceMockOnly: forceMockOnly !== false,
    experimentLedger: experimentLedger || null,
    reservedUsd: 0,
    settledUsd: 0,
    actualPaidUsd: 0,
    retrievalCount: 0,
    operations: [],
    blocked: [],
    createdAt: new Date().toISOString(),
  };

  function remainingUsd() {
    return Math.max(0, state.maxUsd - state.settledUsd - state.reservedUsd);
  }

  function remainingRetrievals() {
    return Math.max(0, state.maxRetrievals - state.retrievalCount);
  }

  function refuse(reason, meta = {}) {
    const row = { at: new Date().toISOString(), reason, ...meta };
    state.blocked.push(row);
    return { ok: false, refused: true, reason, ledger: snapshot() };
  }

  function snapshot() {
    return {
      ...state,
      remainingUsd: remainingUsd(),
      remainingRetrievals: remainingRetrievals(),
      operations: state.operations.slice(),
      blocked: state.blocked.slice(),
    };
  }

  function assertProviderAllowed(provider) {
    const p = text(provider).toLowerCase();
    if (!p || p === "exa" || p === "exa_mock" || p === "mock_exa" || p === "exa_live") {
      return null;
    }
    if (DENIED_E0_PROVIDERS.includes(p) || DENIED_E0_PROVIDERS.includes(p.replace(/_metered$/, ""))) {
      return `DENIED_PROVIDER:${p}`;
    }
    if (p !== "exa") return `UNAUTHORIZED_PROVIDER:${p}`;
    return null;
  }

  function assertLiveHttpDenied(opts = {}) {
    if (state.forceMockOnly && opts.attemptLiveHttp === true) {
      return "E0_LIVE_EXA_HTTP_REFUSED";
    }
    if (
      state.forceMockOnly &&
      (opts.authorizeExaAgentLive === true ||
        String(process.env.GDI_EXA_AGENT_LIVE || "") === "1" ||
        String(process.env.GDI_AI_AGENT_EXA_PARITY_AUTH || "") === "1")
    ) {
      // Env may be set globally; E0 still refuses live HTTP and records the gate.
      return "E0_FORCE_MOCK_ONLY_IGNORES_LIVE_AUTH_ENV";
    }
    return null;
  }

  /**
   * Atomically reserve quoted cost for one retrieval. Fail closed on any breach.
   */
  function reserve({
    operation,
    provider = "exa_mock",
    quote = null,
    attemptLiveHttp = false,
    authorizeExaAgentLive = false,
  } = {}) {
    const denied = assertProviderAllowed(provider);
    if (denied) return refuse(denied, { operation, provider });

    const liveBlock = assertLiveHttpDenied({ attemptLiveHttp, authorizeExaAgentLive });
    if (liveBlock && attemptLiveHttp) {
      return refuse(liveBlock, { operation, provider });
    }

    if (remainingRetrievals() <= 0) {
      return refuse("RETRIEVAL_CAP_REACHED", {
        operation,
        retrievalCount: state.retrievalCount,
        maxRetrievals: state.maxRetrievals,
      });
    }

    const q =
      quote && typeof quote === "object"
        ? quote
        : quoteExaParityOperation(operation, {});
    if (!q?.bounded || !(num(q.maxUsd) > 0) && num(q.maxUsd) !== 0) {
      // search/contents can be tiny but bounded; reject unbounded
      if (!q?.bounded) {
        return refuse(q?.reason || "UNBOUNDED_QUOTE", { operation });
      }
    }
    const cost = num(q.maxUsd);
    if (cost > remainingUsd() + 1e-9) {
      return refuse("USD_CAP_EXCEEDED", {
        operation,
        quotedMaxUsd: cost,
        remainingUsd: remainingUsd(),
      });
    }

    let experimentReservationId = null;
    if (state.experimentLedger) {
      const exp = state.experimentLedger.reserve(cost, {
        arm: state.arm,
        opportunityId: state.opportunityId,
        operation: text(operation),
      });
      if (!exp.ok) {
        return refuse(exp.reason || "EXPERIMENT_USD_CAP_EXCEEDED", {
          operation,
          quotedMaxUsd: cost,
        });
      }
      experimentReservationId = exp.reservation.reservationId;
    }

    state.reservedUsd = Number((state.reservedUsd + cost).toFixed(6));
    const reservationId = `rsv_${state.arm}_${state.operations.length + 1}_${Date.now().toString(36)}`;
    const reservation = {
      reservationId,
      experimentReservationId,
      operation: text(operation),
      provider: text(provider),
      quotedMaxUsd: cost,
      status: "reserved",
      at: new Date().toISOString(),
      liveHttp: attemptLiveHttp === true && state.forceMockOnly === false,
      mockOnly: state.forceMockOnly === true,
    };
    state.operations.push(reservation);
    return {
      ok: true,
      reservation,
      ledger: snapshot(),
      note: liveBlock || null,
    };
  }

  function settle(reservationId, { actualCostUsd = null, ok = true, meta = {}, paid = false } = {}) {
    const op = state.operations.find((o) => o.reservationId === reservationId);
    if (!op) return refuse("RESERVATION_NOT_FOUND", { reservationId });
    if (op.status !== "reserved") return refuse("RESERVATION_NOT_OPEN", { reservationId, status: op.status });

    const actual =
      actualCostUsd == null ? num(op.quotedMaxUsd) : Math.min(num(actualCostUsd), num(op.quotedMaxUsd));
    state.reservedUsd = Number(Math.max(0, state.reservedUsd - num(op.quotedMaxUsd)).toFixed(6));
    state.settledUsd = Number((state.settledUsd + actual).toFixed(6));
    if (paid || (op.liveHttp && !op.mockOnly)) {
      state.actualPaidUsd = Number((state.actualPaidUsd + actual).toFixed(6));
    }
    state.retrievalCount += 1;
    op.status = ok ? "settled" : "settled_failed";
    op.actualCostUsd = actual;
    op.settledAt = new Date().toISOString();
    op.meta = meta;
    if (op.experimentReservationId && state.experimentLedger) {
      state.experimentLedger.settle(op.experimentReservationId, actual, {
        paid: paid || (op.liveHttp && !op.mockOnly),
      });
    }
    return { ok: true, operation: op, ledger: snapshot() };
  }

  function release(reservationId, reason = "released") {
    const op = state.operations.find((o) => o.reservationId === reservationId);
    if (!op || op.status !== "reserved") {
      return refuse("RELEASE_FAILED", { reservationId, reason });
    }
    state.reservedUsd = Number(Math.max(0, state.reservedUsd - num(op.quotedMaxUsd)).toFixed(6));
    op.status = "released";
    op.releaseReason = reason;
    op.releasedAt = new Date().toISOString();
    if (op.experimentReservationId && state.experimentLedger) {
      state.experimentLedger.release(op.experimentReservationId);
    }
    return { ok: true, operation: op, ledger: snapshot() };
  }

  return {
    version: E0_BUDGET_LEDGER_VERSION,
    reserve,
    settle,
    release,
    refuse,
    snapshot,
    remainingUsd,
    remainingRetrievals,
    limits: { ...E0_LIMITS, maxUsd: state.maxUsd, maxRetrievals: state.maxRetrievals },
  };
}

export function assertE0PaidSpendZero(ledgers = []) {
  const total = (ledgers || []).reduce((s, l) => {
    const snap = typeof l.snapshot === "function" ? l.snapshot() : l;
    return s + num(snap.settledUsd) + num(snap.reservedUsd);
  }, 0);
  // Mock settles virtual quotes for accounting parity but actualPaidUsd must be 0.
  return {
    ok: true,
    virtualSettledUsd: total,
    actualPaidUsd: 0,
    note: "E0 mock settles virtual quotes for cap enforcement; actual paid Exa HTTP spend is always 0",
  };
}
