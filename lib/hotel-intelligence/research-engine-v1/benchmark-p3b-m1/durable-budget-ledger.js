/**
 * Durable shared-budget ledger for P3B-M1 live activation.
 *
 * Fail-closed on corruption, cap mismatch, failed reconstruction, or
 * unreadable/partial writes. Supports PENDING_RECONCILE and a durable
 * dispatch claim/lease so RESERVED ops cannot double-dispatch.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { createM1SharedBudgetLedger } from "./shared-budget-ledger.js";
import {
  P3B_M1_HARD_CAP_USD,
  P3B_M1_PER_HOTEL_CAP_USD,
} from "./contract.js";
import { FAILURE_CLASS } from "../contract.js";

export const DURABLE_BUDGET_LEDGER_VERSION = "he-intel-p3b-m1-durable-budget-v1.1";
export const OP_STATUS = Object.freeze({
  RESERVED: "RESERVED",
  CLAIMED: "CLAIMED",
  SETTLED: "SETTLED",
  RELEASED: "RELEASED",
  PENDING_RECONCILE: "PENDING_RECONCILE",
});

export const LEDGER_FATAL = Object.freeze({
  CORRUPT: "LEDGER_STATE_CORRUPT",
  CAP_MISMATCH: "LEDGER_CAP_MISMATCH",
  RECONSTRUCTION_FAILED: "LEDGER_RECONSTRUCTION_FAILED",
  LOCK_UNSAFE: "LEDGER_LOCK_UNSAFE",
  FATAL: "LEDGER_FATAL",
});

const DEFAULT_LEASE_MS = 120_000;
const STALE_LOCK_MS = 30_000;

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

function isFiniteNonNeg(n) {
  return Number.isFinite(n) && n >= 0;
}

function integrityHash(operations, meta) {
  const payload = JSON.stringify({
    operations: operations || {},
    runUsdCap: meta?.runUsdCap,
    perHotelUsdCap: meta?.perHotelUsdCap,
    version: meta?.version,
  });
  return crypto.createHash("sha256").update(payload).digest("hex");
}

function validatePersistedShape(raw) {
  if (!raw || typeof raw !== "object") {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "not_object" };
  }
  if (!raw.meta || typeof raw.meta !== "object") {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "missing_meta" };
  }
  if (!raw.operations || typeof raw.operations !== "object") {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "missing_operations" };
  }
  if (raw.tmpIncomplete === true || raw.partialWrite === true) {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "partial_write_marker" };
  }
  // Mandatory version — unrecognized / missing / legacy without verified migration → fail closed
  const version = String(raw.meta.version || "").trim();
  if (!version) {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "missing_version" };
  }
  if (version !== DURABLE_BUDGET_LEDGER_VERSION) {
    return {
      ok: false,
      error: LEDGER_FATAL.CORRUPT,
      reason: "unrecognized_or_legacy_ledger_version",
      found: version,
      expected: DURABLE_BUDGET_LEDGER_VERSION,
    };
  }
  // Mandatory integrity hash for current durable ledger version
  if (raw.integrity == null || raw.integrity === "") {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "missing_integrity_hash" };
  }
  const expected = integrityHash(raw.operations, raw.meta);
  if (String(raw.integrity) !== expected) {
    return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: "integrity_mismatch" };
  }
  for (const [id, op] of Object.entries(raw.operations)) {
    if (!op || typeof op !== "object") {
      return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: `bad_op_${id}` };
    }
    if (!Object.values(OP_STATUS).includes(op.status)) {
      return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: `bad_status_${id}` };
    }
    const reserved = Number(op.reserved_usd);
    if (op.reserved_usd != null && !isFiniteNonNeg(reserved)) {
      return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: `bad_reserved_${id}` };
    }
    if (op.actual_usd != null && !isFiniteNonNeg(Number(op.actual_usd))) {
      return { ok: false, error: LEDGER_FATAL.CORRUPT, reason: `bad_actual_${id}` };
    }
  }
  return { ok: true };
}

function pidAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Acquire exclusive lock. Stale locks (dead PID or aged) may be reclaimed
 * only when reclaim is clearly safe; otherwise fail closed.
 */
export function acquireFileLock(lockPath, { retries = 80, delayMs = 25, now = Date.now } = {}) {
  ensureDir(path.dirname(lockPath));
  for (let i = 0; i < retries; i += 1) {
    try {
      const fd = fs.openSync(lockPath, "wx");
      fs.writeFileSync(
        fd,
        JSON.stringify({ pid: process.pid, at: now(), host: process.pid })
      );
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
      // Attempt stale reclaim
      try {
        const raw = fs.readFileSync(lockPath, "utf8");
        let meta;
        try {
          meta = JSON.parse(raw);
        } catch {
          // Corrupt lock — if aged enough by mtime, reclaim; else fail closed later
          meta = null;
        }
        const st = fs.statSync(lockPath);
        const age = now() - Number(st.mtimeMs || 0);
        const ownerPid = Number(meta?.pid);
        const dead = meta && !pidAlive(ownerPid);
        const aged = age > STALE_LOCK_MS;
        if ((dead && aged) || (!meta && aged)) {
          try {
            fs.unlinkSync(lockPath);
            continue;
          } catch {
            /* race — retry */
          }
        }
      } catch {
        /* lock disappeared — retry */
      }
      const end = now() + delayMs;
      while (now() < end) {
        /* brief spin */
      }
    }
  }
  const err = new Error(LEDGER_FATAL.LOCK_UNSAFE);
  err.code = LEDGER_FATAL.LOCK_UNSAFE;
  throw err;
}

/**
 * @param {object} [opts]
 */
export function createDurableM1BudgetLedger(opts = {}) {
  const runId = opts.runId || `m1_budget_${Date.now()}`;
  const persistPath = opts.persistPath || defaultPath(runId);
  const memoryOnly = opts.memoryOnly === true;
  const approvedRunUsdCap = Number(opts.runUsdCap ?? P3B_M1_HARD_CAP_USD);
  const approvedPerHotelUsdCap = Number(opts.perHotelUsdCap ?? P3B_M1_PER_HOTEL_CAP_USD);
  const lockPath = `${persistPath}.lock`;
  const workerId = opts.workerId || `worker_${process.pid}_${crypto.randomBytes(4).toString("hex")}`;
  const leaseMs = Number(opts.leaseMs || DEFAULT_LEASE_MS);

  /** @type {Map<string, object>} */
  const durableOps = new Map();
  let ledgerMeta = {
    version: DURABLE_BUDGET_LEDGER_VERSION,
    runId,
    runUsdCap: approvedRunUsdCap,
    perHotelUsdCap: approvedPerHotelUsdCap,
    pendingReservedUsd: 0,
  };
  let fatalError = null;
  let seededReserved = 0;
  let inner = createM1SharedBudgetLedger({
    runUsdCap: approvedRunUsdCap,
    perHotelUsdCap: approvedPerHotelUsdCap,
    perOrgUsdCap: opts.perOrgUsdCap,
    perHotelCreditCap: opts.perHotelCreditCap,
  });

  function markFatal(code, reason, detail = {}) {
    fatalError = { ok: false, error: code, reason, ...detail };
    return fatalError;
  }

  function assertNotFatal() {
    if (fatalError) {
      return { ok: false, ...fatalError, fatal: true };
    }
    return null;
  }

  function validateCharge(entry = {}) {
    const cost = Number(entry.cost_usd);
    if (entry.cost_usd_status === "UNKNOWN" || !Number.isFinite(cost)) {
      return {
        ok: false,
        error: FAILURE_CLASS.BUDGET_EXHAUSTED,
        scope: "usd_unknown_under_cap",
        note: "fail_closed_unbounded_or_unknown_cost",
      };
    }
    if (cost < 0 || !isFiniteNonNeg(cost)) {
      return {
        ok: false,
        error: "INVALID_CHARGE",
        note: "negative_or_nonfinite_charge_rejected",
      };
    }
    for (const k of [
      "provider_cost_usd",
      "model_token_cost_usd",
      "orchestration_cost_usd",
    ]) {
      if (entry[k] != null && !isFiniteNonNeg(Number(entry[k]))) {
        return {
          ok: false,
          error: "INVALID_CHARGE",
          note: `invalid_${k}`,
        };
      }
    }
    return { ok: true, cost };
  }

  function loadFromDisk({ requireExists = false } = {}) {
    if (memoryOnly) return { ok: true, empty: true };
    if (!fs.existsSync(persistPath)) {
      if (requireExists) {
        return markFatal(LEDGER_FATAL.CORRUPT, "expected_ledger_missing");
      }
      // Recover interrupted write: .tmp alone is corruption / incomplete
      if (fs.existsSync(`${persistPath}.tmp`)) {
        return markFatal(LEDGER_FATAL.CORRUPT, "interrupted_write_tmp_present");
      }
      return { ok: true, empty: true };
    }
    let rawText;
    try {
      rawText = fs.readFileSync(persistPath, "utf8");
    } catch (err) {
      return markFatal(LEDGER_FATAL.CORRUPT, "unreadable", {
        message: String(err?.message || err),
      });
    }
    if (!rawText || !String(rawText).trim()) {
      return markFatal(LEDGER_FATAL.CORRUPT, "empty_file");
    }
    let raw;
    try {
      raw = JSON.parse(rawText);
    } catch (err) {
      return markFatal(LEDGER_FATAL.CORRUPT, "json_parse_failed", {
        message: String(err?.message || err),
      });
    }
    const shape = validatePersistedShape(raw);
    if (!shape.ok) {
      return markFatal(shape.error, shape.reason);
    }

    // Cap envelope: persisted caps must match approved constructor envelope
    const fileRunCap = Number(raw.meta.runUsdCap);
    const fileHotelCap = Number(raw.meta.perHotelUsdCap);
    if (
      !Number.isFinite(fileRunCap) ||
      !Number.isFinite(fileHotelCap) ||
      Math.abs(fileRunCap - approvedRunUsdCap) > 1e-9 ||
      Math.abs(fileHotelCap - approvedPerHotelUsdCap) > 1e-9
    ) {
      return markFatal(LEDGER_FATAL.CAP_MISMATCH, "persisted_caps_disagree_with_approved_envelope", {
        approvedRunUsdCap,
        approvedPerHotelUsdCap,
        fileRunCap,
        fileHotelCap,
      });
    }

    ledgerMeta = {
      ...ledgerMeta,
      ...raw.meta,
      runUsdCap: approvedRunUsdCap,
      perHotelUsdCap: approvedPerHotelUsdCap,
    };
    durableOps.clear();
    for (const [id, op] of Object.entries(raw.operations || {})) {
      durableOps.set(id, op);
    }
    return { ok: true, empty: false };
  }

  function saveUnlocked() {
    if (memoryOnly) return;
    if (fatalError) {
      throw Object.assign(new Error(fatalError.error), fatalError);
    }
    ensureDir(path.dirname(persistPath));
    const operations = {};
    for (const [id, op] of durableOps) operations[id] = op;
    const meta = {
      ...ledgerMeta,
      runUsdCap: approvedRunUsdCap,
      perHotelUsdCap: approvedPerHotelUsdCap,
      updatedAt: new Date().toISOString(),
      persistPath,
    };
    const body = {
      meta,
      operations,
      integrity: integrityHash(operations, meta),
      snapshot: inner.snapshot(),
    };
    const tmp = `${persistPath}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(body, null, 2));
    fs.renameSync(tmp, persistPath);
  }

  function rebuildInnerFromDurable() {
    inner = createM1SharedBudgetLedger({
      runUsdCap: approvedRunUsdCap,
      perHotelUsdCap: approvedPerHotelUsdCap,
      perOrgUsdCap: opts.perOrgUsdCap,
      perHotelCreditCap: opts.perHotelCreditCap,
    });
    seededReserved = 0;
    const settled = [];
    const active = [];
    for (const [id, op] of durableOps) {
      if (op.status === OP_STATUS.SETTLED) settled.push([id, op]);
      else if (
        op.status === OP_STATUS.RESERVED ||
        op.status === OP_STATUS.CLAIMED ||
        op.status === OP_STATUS.PENDING_RECONCILE
      ) {
        active.push([id, op]);
      }
    }
    for (const [id, op] of settled) {
      const cost = Number(op.actual_usd ?? op.reserved_usd ?? 0);
      if (!isFiniteNonNeg(cost)) {
        return markFatal(LEDGER_FATAL.RECONSTRUCTION_FAILED, `settled_bad_cost_${id}`);
      }
      const r = inner.reserve({
        operation_id: id,
        hotel_id: op.hotel_id,
        org_key: op.org_key,
        provider: op.provider,
        operation: op.operation,
        cost_usd: cost,
        cost_usd_status: "ESTIMATED",
        credits: 0,
      });
      if (!r.ok && !r.already_settled && !r.already_reserved) {
        return markFatal(LEDGER_FATAL.RECONSTRUCTION_FAILED, `settle_reserve_failed_${id}`, r);
      }
      const s = inner.settle(id, {
        cost_usd: op.actual_usd ?? op.reserved_usd ?? 0,
        provider_cost_usd: op.provider_cost_usd ?? op.actual_usd,
        model_token_cost_usd: op.model_token_cost_usd || 0,
        orchestration_cost_usd: op.orchestration_cost_usd || 0,
        status: "OK",
      });
      if (!s.ok && !s.already_settled) {
        return markFatal(LEDGER_FATAL.RECONSTRUCTION_FAILED, `settle_failed_${id}`, s);
      }
    }
    for (const [id, op] of active) {
      const cost = Number(op.reserved_usd || 0);
      if (!isFiniteNonNeg(cost)) {
        return markFatal(LEDGER_FATAL.RECONSTRUCTION_FAILED, `active_bad_cost_${id}`);
      }
      seededReserved = Number((seededReserved + cost).toFixed(6));
      const r = inner.reserve({
        operation_id: id,
        hotel_id: op.hotel_id,
        org_key: op.org_key,
        provider: op.provider,
        operation: op.operation,
        cost_usd: cost,
        cost_usd_status: "ESTIMATED",
        credits: op.reserved_credits || 0,
      });
      if (!r.ok && !r.already_reserved && !r.already_settled) {
        return markFatal(LEDGER_FATAL.RECONSTRUCTION_FAILED, `active_reserve_failed_${id}`, r);
      }
    }
    // CLAIMED ops remain CLAIMED across restart. claimDispatch never re-authorizes
    // (including same worker), so a surviving CLAIMED cannot cause a second paid dispatch.
    return { ok: true };
  }

  function withLock(fn) {
    const blocked = assertNotFatal();
    if (blocked) return blocked;
    if (memoryOnly) {
      return fn();
    }
    let lock;
    try {
      lock = acquireFileLock(lockPath);
    } catch (err) {
      return markFatal(LEDGER_FATAL.LOCK_UNSAFE, String(err?.message || err));
    }
    try {
      const loaded = loadFromDisk();
      if (!loaded.ok) return loaded;
      const rebuilt = rebuildInnerFromDurable();
      if (!rebuilt.ok) return rebuilt;
      return fn();
    } finally {
      lock.release();
    }
  }

  // Initial load
  if (!memoryOnly) {
    const loaded = loadFromDisk();
    if (!loaded.ok) {
      // leave fatalError set — all ops refuse
    } else {
      const rebuilt = rebuildInnerFromDurable();
      if (!rebuilt.ok) {
        // fatal already set
      }
    }
  }

  function reserve(entry = {}) {
    return withLock(() => {
      const blocked = assertNotFatal();
      if (blocked) return blocked;
      const id = String(entry.operation_id || "").trim();
      if (!id) return { ok: false, error: "OPERATION_ID_REQUIRED" };

      const charge = validateCharge(entry);
      if (!charge.ok) return charge;

      const prior = durableOps.get(id);
      if (prior?.status === OP_STATUS.SETTLED) {
        return { ok: true, already_settled: true, reservation_id: id, operation: prior };
      }
      if (prior?.status === OP_STATUS.PENDING_RECONCILE) {
        return {
          ok: false,
          error: "PENDING_RECONCILE_BLOCKS_RETRY",
          operation_id: id,
          operation: prior,
        };
      }
      if (prior?.status === OP_STATUS.CLAIMED) {
        return {
          ok: false,
          error: "OPERATION_IN_FLIGHT",
          operation_id: id,
          operation: prior,
          note: "Claimed by another worker; no second reserve/dispatch",
        };
      }
      if (prior?.status === OP_STATUS.RESERVED) {
        // RESERVED = in-flight/unknown until claim ownership established
        return {
          ok: true,
          already_reserved: true,
          in_flight: true,
          reservation_id: id,
          operation: prior,
          note: "RESERVED_REQUIRES_CLAIM_BEFORE_DISPATCH",
        };
      }

      const result = inner.reserve({ ...entry, cost_usd: charge.cost });
      if (!result.ok) return result;

      if (!result.already_reserved && !result.already_settled) {
        durableOps.set(id, {
          operation_id: id,
          status: OP_STATUS.RESERVED,
          provider: entry.provider || null,
          operation: entry.operation || null,
          hotel_id: entry.hotel_id || null,
          org_key: entry.org_key || null,
          reserved_usd: charge.cost,
          reserved_credits: Number(entry.credits || 0) || 0,
          reserved_at: new Date().toISOString(),
          provider_cost_usd: null,
          model_token_cost_usd: null,
          orchestration_cost_usd: null,
          claim_worker_id: null,
          lease_expires_at_ms: null,
          dispatched: false,
          provider_run_id: null,
        });
        saveUnlocked();
      }
      return { ...result, reservation_id: id };
    });
  }

  /**
   * Claim exclusive dispatch rights for a RESERVED operation.
   * Exactly one worker may succeed across processes.
   */
  function claimDispatch(operationId, claimOpts = {}) {
    return withLock(() => {
      const blocked = assertNotFatal();
      if (blocked) return blocked;
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id);
      if (!prior) return { ok: false, error: "RESERVATION_NOT_FOUND", operation_id: id };
      if (prior.status === OP_STATUS.SETTLED) {
        return { ok: false, error: "ALREADY_SETTLED", operation: prior, already_settled: true };
      }
      if (prior.status === OP_STATUS.PENDING_RECONCILE) {
        return { ok: false, error: "PENDING_RECONCILE_BLOCKS_RETRY", operation: prior };
      }
      if (prior.status === OP_STATUS.CLAIMED) {
        // Never re-authorize dispatch — even for the same worker ID
        return {
          ok: false,
          error: "OPERATION_IN_FLIGHT",
          newly_acquired: false,
          operation: prior,
          note: "Existing CLAIMED blocks all further claimDispatch including same worker",
        };
      }
      if (prior.status !== OP_STATUS.RESERVED) {
        return { ok: false, error: "NOT_RESERVABLE_FOR_CLAIM", status: prior.status };
      }
      if (prior.provider_run_id || prior.dispatched === true) {
        // Ambiguous prior dispatch evidence — escalate to pending reconcile
        durableOps.set(id, {
          ...prior,
          status: OP_STATUS.PENDING_RECONCILE,
          pending_at: new Date().toISOString(),
          pending_reason: "DISPATCH_EVIDENCE_WITHOUT_SETTLE",
        });
        saveUnlocked();
        return { ok: false, error: "PENDING_RECONCILE_BLOCKS_RETRY", operation_id: id };
      }

      const token = crypto.randomBytes(12).toString("hex");
      const expires = Date.now() + Number(claimOpts.leaseMs || leaseMs);
      const claimed = {
        ...prior,
        status: OP_STATUS.CLAIMED,
        claim_worker_id: claimOpts.workerId || workerId,
        claim_token: token,
        lease_expires_at_ms: expires,
        claimed_at: new Date().toISOString(),
      };
      durableOps.set(id, claimed);
      saveUnlocked();
      return {
        ok: true,
        newly_acquired: true,
        claim_token: token,
        operation: claimed,
        lease_expires_at_ms: expires,
      };
    });
  }

  function markDispatched(operationId, meta = {}) {
    return withLock(() => {
      const blocked = assertNotFatal();
      if (blocked) return blocked;
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id);
      if (!prior || prior.status !== OP_STATUS.CLAIMED) {
        return { ok: false, error: "NOT_CLAIMED", operation_id: id };
      }
      if (meta.claim_token && meta.claim_token !== prior.claim_token) {
        return { ok: false, error: "CLAIM_TOKEN_MISMATCH" };
      }
      durableOps.set(id, {
        ...prior,
        dispatched: true,
        dispatched_at: new Date().toISOString(),
        provider_run_id: meta.provider_run_id || prior.provider_run_id || null,
        provider_status: meta.provider_status || null,
      });
      saveUnlocked();
      return { ok: true };
    });
  }

  async function reserveAsync(entry = {}) {
    return reserve(entry);
  }

  function settle(operationId, actual = {}) {
    return withLock(() => {
      const blocked = assertNotFatal();
      if (blocked) return blocked;
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id);
      if (prior?.status === OP_STATUS.SETTLED) {
        return { ok: true, already_settled: true, operation: prior };
      }
      for (const k of ["cost_usd", "provider_cost_usd", "model_token_cost_usd", "orchestration_cost_usd"]) {
        if (actual[k] != null && !isFiniteNonNeg(Number(actual[k]))) {
          return { ok: false, error: "INVALID_CHARGE", note: `settle_${k}` };
        }
      }
      if (prior?.status === OP_STATUS.PENDING_RECONCILE) {
        // Allow explicit settle of pending (manual reconcile path)
        prior.status = OP_STATUS.CLAIMED;
        durableOps.set(id, prior);
      }
      const result = inner.settle(id, actual);
      if (!result.ok) return result;
      // Preserve conservative: if actual unknown, keep reserved amount as actual floor
      const actualUsd =
        actual.cost_usd != null && Number.isFinite(Number(actual.cost_usd))
          ? Number(actual.cost_usd)
          : prior?.reserved_usd ?? null;
      durableOps.set(id, {
        ...(prior || {}),
        operation_id: id,
        status: OP_STATUS.SETTLED,
        actual_usd: actualUsd,
        provider_cost_usd: actual.provider_cost_usd ?? actualUsd,
        model_token_cost_usd: actual.model_token_cost_usd || 0,
        orchestration_cost_usd: actual.orchestration_cost_usd || 0,
        settled_at: new Date().toISOString(),
        result: actual.result || prior?.result || null,
      });
      saveUnlocked();
      return result;
    });
  }

  function release(operationId, meta = {}) {
    return withLock(() => {
      const blocked = assertNotFatal();
      if (blocked) return blocked;
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
      if (prior?.status === OP_STATUS.CLAIMED && prior.dispatched === true) {
        // Dispatched claim cannot silent-release — pending reconcile
        durableOps.set(id, {
          ...prior,
          status: OP_STATUS.PENDING_RECONCILE,
          pending_at: new Date().toISOString(),
          pending_reason: "RELEASE_AFTER_DISPATCH_FORBIDDEN",
        });
        saveUnlocked();
        return {
          ok: false,
          error: "PENDING_RECONCILE_NO_AUTO_RELEASE",
          operation_id: id,
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

  function markPendingReconcile(operationId, pendingMeta = {}) {
    return withLock(() => {
      const blocked = assertNotFatal();
      if (blocked) return blocked;
      const id = String(operationId || "").trim();
      const prior = durableOps.get(id) || inner.getOperation?.(id);
      if (!prior) {
        return { ok: false, error: "RESERVATION_NOT_FOUND", operation_id: id };
      }
      if (prior.status === OP_STATUS.SETTLED) {
        return { ok: true, already_settled: true };
      }
      // Keep full reserved amount against shared cap
      durableOps.set(id, {
        ...prior,
        operation_id: id,
        status: OP_STATUS.PENDING_RECONCILE,
        pending_at: new Date().toISOString(),
        pending_reason: pendingMeta.reason || "AMBIGUOUS_PROVIDER_OUTCOME",
        pending_meta: pendingMeta,
        reserved_usd: prior.reserved_usd, // conservative preserve
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
    const prior = getOperation(id);
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
        const blocked = assertNotFatal();
        if (blocked) return blocked;
        const cur = durableOps.get(id);
        if (!cur || cur.status !== OP_STATUS.PENDING_RECONCILE) {
          return { ok: false, error: "NOT_PENDING_RECONCILE", operation_id: id };
        }
        cur.status = OP_STATUS.RESERVED;
        cur.dispatched = false;
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
    if (fatalError) return null;
    if (!memoryOnly && fs.existsSync(persistPath)) {
      try {
        const raw = JSON.parse(fs.readFileSync(persistPath, "utf8"));
        const shape = validatePersistedShape(raw);
        if (!shape.ok) {
          markFatal(shape.error, shape.reason);
          return null;
        }
        if (raw.operations?.[id]) return raw.operations[id];
      } catch {
        markFatal(LEDGER_FATAL.CORRUPT, "getOperation_read_failed");
        return null;
      }
    }
    return durableOps.get(id) || inner.getOperation?.(id) || null;
  }

  function snapshot() {
    return {
      version: DURABLE_BUDGET_LEDGER_VERSION,
      runId,
      persistPath: memoryOnly ? null : persistPath,
      runUsdCap: approvedRunUsdCap,
      perHotelUsdCap: approvedPerHotelUsdCap,
      operations: Object.fromEntries(durableOps),
      inner: inner.snapshot(),
      canParallelBypassSharedBudget: false,
      seededReservedOnLoad: seededReserved,
      pendingReservedUsd: ledgerMeta.pendingReservedUsd || 0,
      fatalError,
      workerId,
    };
  }

  return {
    reserve,
    reserveAsync,
    claimDispatch,
    markDispatched,
    settle,
    release,
    markPendingReconcile,
    reconcile,
    getOperation,
    snapshot,
    persistPath: memoryOnly ? null : persistPath,
    runId,
    workerId,
    isFatal: () => Boolean(fatalError),
    getFatal: () => fatalError,
  };
}
