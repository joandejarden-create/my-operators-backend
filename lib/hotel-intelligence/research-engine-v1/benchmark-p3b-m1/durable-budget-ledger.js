/**
 * Durable shared-budget ledger for P3B-M1 live activation.
 * Persists reservations across process restarts; supports PENDING_RECONCILE
 * for timeouts / unknown billing (does not auto-release full reservation).
 * File lock serializes multi-worker mutations against the same persist path.
 */

import fs from "node:fs";
import path from "node:path";
import { createM1SharedBudgetLedger } from "./shared-budget-ledger.js";
import {
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
} from "./contract.js";
import { FAILURE_CLASS } from "../contract.js";

export const DURABLE_BUDGET_LEDGER_VERSION = "he-intel-p3b-m1-durable-budget-v1";
export const OP_STATUS = Object.freeze({
  RESERVED: "RESERVED",
  SETTLED: "SETTLED",
  RELEASED: "RELEASED",
  PENDING_RECONCILE: "PENDING_RECONCILE",
});

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function defaultPath(runId) {
  return path.join(
    process.cwd(),
    "reports/hotel-explorer/he-intel-001-research-benchmark/budget-ledgers",
    `${runId || "default"}.json`
  );
}

function acquireFileLock(lockPath, { retries = 40, delayMs = 25 } = {}) {
  ensureDir(path.dirname(lockPath));
  for (let i = 0; i < retries; i += 1) {
    try {
      const fd = fs.openSync(lockPath, "wx");
      fs.writeFileSync(fd, String(process.pid));
      return {
        release() {
          try {
            fs.closeSync(fd);
          } catch {
            /* ignore */
          }
          try {
            fs.unlinkSync(lockPath);
          } catch {
            /* ignore */
          }
        },
      };
    } catch {
      const end = Date.now() + delayMs;
      while (Date.now() < end) {
        /* spin briefly — Node single-thread; multi-worker uses separate processes */
      }
    }
  }
  throw new Error("DURABLE_BUDGET_LOCK_TIMEOUT");
}

/**
 * @param {object} [opts]
 * @param {string} [opts.persistPath]
 * @param {string} [opts.runId]
 * @param {boolean} [opts.memoryOnly]
 */
export function createDurableM1BudgetLedger(opts = {}) {
  const runId = opts.runId || `m1_budget_${Date.now()}`;
  const persistPath = opts.persistPath || defaultPath(runId);
  const memoryOnly = opts.memoryOnly === true;
  const runUsdCap = Number(opts.runUsdCap ?? P3B_M1_HARD_CAP_USD);
  const perHotelUsdCap = Number(opts.perHotelUsdCap ?? P3B_M1_PER_HOTEL_CAP_USD);
  const lockPath = `${persistPath}.lock`;

  /** @type {Map<string, object>} */
  const durableOps = new Map();
  let ledgerMeta = {
    version: DURABLE_BUDGET_LEDGER_VERSION,
    runId,
    runUsdCap,
    perHotelUsdCap,
    pendingReservedUsd: 0,
  };

  function loadFromDisk() {
    if (memoryOnly || !fs.existsSync(persistPath)) return;
    try {
      const raw = JSON.parse(fs.readFileSync(persistPath, "utf8"));
      ledgerMeta = { ...ledgerMeta, ...(raw.meta || {}) };
      durableOps.clear();
      for (const [id, op] of Object.entries(raw.operations || {})) {
        durableOps.set(id, op);
      }
    } catch {
      // fail-closed empty — do not invent spend
    }
  }

  function saveUnlocked() {
    if (memoryOnly) return;
    ensureDir(path.dirname(persistPath));
    const operations = {};
    for (const [id, op] of durableOps) operations[id] = op;
    const tmp = `${persistPath}.tmp`;
    fs.writeFileSync(
      tmp,
      JSON.stringify(
        {
          meta: {
            ...ledgerMeta,
            updatedAt: new Date().toISOString(),
            persistPath,
          },
          operations,
          snapshot: inner.snapshot(),
        },
        null,
        2
      )
    );
    fs.renameSync(tmp, persistPath);
  }

  function withLock(fn) {
    if (memoryOnly) return fn();
    const lock = acquireFileLock(lockPath);
    try {
      loadFromDisk();
      rebuildInnerFromDurable();
      return fn();
    } finally {
      lock.release();
    }
  }

  let seededReserved = 0;
  let inner = createM1SharedBudgetLedger({
    runUsdCap,
    perHotelUsdCap,
    perOrgUsdCap: opts.perOrgUsdCap,
    perHotelCreditCap: opts.perHotelCreditCap,
  });

  function rebuildInnerFromDurable() {
    inner = createM1SharedBudgetLedger({
      runUsdCap,
      perHotelUsdCap,
      perOrgUsdCap: opts.perOrgUsdCap,
      perHotelCreditCap: opts.perHotelCreditCap,
    });
    seededReserved = 0;
    // SETTLED first (historical), then active holds
    const settled = [];
    const active = [];
    for (const [id, op] of durableOps) {
      if (op.status === OP_STATUS.SETTLED) settled.push([id, op]);
      else if (
        op.status === OP_STATUS.RESERVED ||
        op.status === OP_STATUS.PENDING_RECONCILE
      ) {
        active.push([id, op]);
      }
    }
    for (const [id, op] of settled) {
      const cost = Number(op.actual_usd ?? op.reserved_usd ?? 0);
      const r = inner.reserve({
        operation_id: id,
        hotel_id: op.hotel_id,
        org_key: op.org_key,
        provider: op.provider,
        operation: op.operation,
        cost_usd: Number.isFinite(cost) ? cost : 0,
        cost_usd_status: "ESTIMATED",
        credits: 0,
      });
      if (r.ok) {
        inner.settle(id, {
          cost_usd: op.actual_usd ?? op.reserved_usd ?? 0,
          provider_cost_usd: op.provider_cost_usd ?? op.actual_usd,
          model_token_cost_usd: op.model_token_cost_usd || 0,
          orchestration_cost_usd: op.orchestration_cost_usd || 0,
          status: "OK",
        });
      }
    }
    for (const [id, op] of active) {
      const cost = Number(op.reserved_usd || 0);
      seededReserved = Number((seededReserved + cost).toFixed(6));
      inner.reserve({
        operation_id: id,
        hotel_id: op.hotel_id,
        org_key: op.org_key,
        provider: op.provider,
        operation: op.operation,
        cost_usd: cost,
        cost_usd_status: "ESTIMATED",
        credits: op.reserved_credits || 0,
      });
    }
  }

  loadFromDisk();
  rebuildInnerFromDurable();

  function reserve(entry = {}) {
    return withLock(() => {
      const id = String(entry.operation_id || "").trim();
      const prior = durableOps.get(id);
      if (prior?.status === OP_STATUS.SETTLED) {
        return { ok: true, already_settled: true, reservation_id: id, operation: prior };
      }
      if (prior?.status === OP_STATUS.PENDING_RECONCILE) {
        return {
          ok: false,
          error: "PENDING_RECONCILE_BLOCKS_RETRY",
          operation_id: id,
          note: "Ambiguous prior outcome must be reconciled before reuse",
        };
      }
      if (prior?.status === OP_STATUS.RESERVED) {
        return { ok: true, already_reserved: true, reservation_id: id, operation: prior };
      }

      const cost = Number(entry.cost_usd);
      if (!Number.isFinite(cost) || entry.cost_usd_status === "UNKNOWN") {
        return {
          ok: false,
          error: FAILURE_CLASS.BUDGET_EXHAUSTED,
          scope: "usd_unknown_under_cap",
          note: "fail_closed_unbounded_or_unknown_cost",
        };
      }

      const result = inner.reserve(entry);
      if (!result.ok) return result;

      if (!result.already_reserved && !result.already_settled) {
        durableOps.set(id, {
          operation_id: id,
          status: OP_STATUS.RESERVED,
          provider: entry.provider || null,
          operation: entry.operation || null,
          hotel_id: entry.hotel_id || null,
          org_key: entry.org_key || null,
          reserved_usd: cost,
          reserved_credits: Number(entry.credits || 0) || 0,
          reserved_at: new Date().toISOString(),
          provider_cost_usd: null,
          model_token_cost_usd: null,
          orchestration_cost_usd: null,
        });
        saveUnlocked();
      }
      return result;
    });
  }

  async function reserveAsync(entry = {}) {
    return reserve(entry);
  }

  function settle(operationId, actual = {}) {
    return withLock(() => {
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id);
      if (prior?.status === OP_STATUS.SETTLED) {
        return { ok: true, already_settled: true, operation: prior };
      }
      // Pending reconcile may settle after founder/manual confirmation
      if (prior?.status === OP_STATUS.PENDING_RECONCILE) {
        prior.status = OP_STATUS.RESERVED;
        durableOps.set(id, prior);
      }
      const result = inner.settle(id, actual);
      if (!result.ok) return result;
      durableOps.set(id, {
        ...(prior || {}),
        operation_id: id,
        status: OP_STATUS.SETTLED,
        actual_usd: actual.cost_usd ?? prior?.reserved_usd ?? null,
        provider_cost_usd: actual.provider_cost_usd ?? actual.cost_usd ?? null,
        model_token_cost_usd: actual.model_token_cost_usd || 0,
        orchestration_cost_usd: actual.orchestration_cost_usd || 0,
        settled_at: new Date().toISOString(),
      });
      saveUnlocked();
      return result;
    });
  }

  function release(operationId, meta = {}) {
    return withLock(() => {
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id);
      if (prior?.status === OP_STATUS.PENDING_RECONCILE) {
        return {
          ok: false,
          error: "PENDING_RECONCILE_NO_AUTO_RELEASE",
          operation_id: id,
          note: "Timeout/unknown billing held conservatively",
        };
      }
      const result = inner.release(id, meta);
      if (result.ok) {
        durableOps.set(id, {
          ...(prior || {}),
          operation_id: id,
          status: OP_STATUS.RELEASED,
          released_at: new Date().toISOString(),
          release_meta: meta,
        });
        saveUnlocked();
      }
      return result;
    });
  }

  /**
   * Mark ambiguous outcome — keep reservation against shared cap.
   */
  function markPendingReconcile(operationId, pendingMeta = {}) {
    return withLock(() => {
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id) || inner.getOperation?.(id);
      if (!prior) {
        return { ok: false, error: "RESERVATION_NOT_FOUND", operation_id: id };
      }
      if (prior.status === OP_STATUS.SETTLED) {
        return { ok: true, already_settled: true };
      }
      durableOps.set(id, {
        ...prior,
        operation_id: id,
        status: OP_STATUS.PENDING_RECONCILE,
        pending_at: new Date().toISOString(),
        pending_reason: pendingMeta.reason || "AMBIGUOUS_PROVIDER_OUTCOME",
        pending_meta: pendingMeta,
      });
      ledgerMeta.pendingReservedUsd = Number(
        (
          Number(ledgerMeta.pendingReservedUsd || 0) + Number(prior.reserved_usd || 0)
        ).toFixed(6)
      );
      saveUnlocked();
      return { ok: true, operation_id: id, status: OP_STATUS.PENDING_RECONCILE };
    });
  }

  function reconcile(operationId, decision = {}) {
    const id = String(operationId || "").trim();
    const prior = durableOps.get(id);
    if (!prior || prior.status !== OP_STATUS.PENDING_RECONCILE) {
      return { ok: false, error: "NOT_PENDING_RECONCILE", operation_id: id };
    }
    if (decision.outcome === "SETTLE") {
      return settle(id, {
        cost_usd: decision.cost_usd ?? prior.reserved_usd,
        provider_cost_usd: decision.provider_cost_usd ?? decision.cost_usd,
        model_token_cost_usd: decision.model_token_cost_usd || 0,
        orchestration_cost_usd: decision.orchestration_cost_usd || 0,
        status: "OK",
      });
    }
    if (decision.outcome === "RELEASE") {
      return withLock(() => {
        const cur = durableOps.get(id);
        if (!cur || cur.status !== OP_STATUS.PENDING_RECONCILE) {
          return { ok: false, error: "NOT_PENDING_RECONCILE", operation_id: id };
        }
        cur.status = OP_STATUS.RESERVED;
        durableOps.set(id, cur);
        const result = inner.release(id, { reason: "RECONCILE_RELEASE", ...decision });
        if (result.ok) {
          durableOps.set(id, {
            ...cur,
            status: OP_STATUS.RELEASED,
            released_at: new Date().toISOString(),
            release_meta: { reason: "RECONCILE_RELEASE", ...decision },
          });
          saveUnlocked();
        }
        return result;
      });
    }
    return { ok: false, error: "INVALID_RECONCILE_OUTCOME" };
  }

  function getOperation(operationId) {
    const id = String(operationId || "").trim();
    if (!memoryOnly && fs.existsSync(persistPath)) {
      try {
        const raw = JSON.parse(fs.readFileSync(persistPath, "utf8"));
        if (raw.operations?.[id]) return raw.operations[id];
      } catch {
        /* fall through */
      }
    }
    return durableOps.get(id) || inner.getOperation?.(id) || null;
  }

  function snapshot() {
    return {
      version: DURABLE_BUDGET_LEDGER_VERSION,
      runId,
      persistPath: memoryOnly ? null : persistPath,
      runUsdCap,
      perHotelUsdCap,
      operations: Object.fromEntries(durableOps),
      inner: inner.snapshot(),
      canParallelBypassSharedBudget: false,
      seededReservedOnLoad: seededReserved,
      pendingReservedUsd: ledgerMeta.pendingReservedUsd || 0,
    };
  }

  return {
    reserve,
    reserveAsync,
    settle,
    release,
    markPendingReconcile,
    reconcile,
    getOperation,
    snapshot,
    persistPath: memoryOnly ? null : persistPath,
    runId,
  };
}
