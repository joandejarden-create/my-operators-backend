/**
 * Durable pilot budget ledger — persists identity, hotel↔case bindings,
 * cumulative usage, outstanding reservations, and deadlines across restarts.
 * Uses the same case-store root; reconciles against operation journals.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  outstandingReservationsFromJournal,
  restoreBudgetUsageFromJournal,
} from "./research-operation-journal.js";

export const CI_DEV_PILOT_LEDGER_VERSION = "ci-dev-pilot-budget-ledger-v1";

export function buildPilotId(policy) {
  const ids = [...(policy.hotel_ids || [])].sort().join(",");
  // Identity excludes mutable hard caps so raising CONTEXT_DEV_PILOT_CREDIT_HARD_CAP
  // does not orphan durable ledgers. Anchor defaults to legacy "8" fingerprint.
  const anchor = policy.pilot_identity_digest_anchor ?? "8";
  const digest = crypto
    .createHash("sha256")
    .update(`${policy.version || "v1"}|${ids}|${anchor}`)
    .digest("hex")
    .slice(0, 16);
  return `pilot_${digest}`;
}

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function writeJsonAtomic(filePath, data) {
  ensureDir(path.dirname(filePath));
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  try {
    fs.renameSync(tmp, filePath);
  } catch (err) {
    if (err && (err.code === "EEXIST" || err.code === "EPERM" || err.code === "EACCES")) {
      try {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      } catch {
        /* ignore */
      }
      fs.renameSync(tmp, filePath);
    } else {
      try {
        fs.unlinkSync(tmp);
      } catch {
        /* ignore */
      }
      throw err;
    }
  }
}

function ledgerRecoveryError(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  err.recovery = true;
  Object.assign(err, extra);
  return err;
}

/**
 * Validate a persisted ledger. Does not mutate or delete the file.
 * @returns {{ ok: true, state } | { ok: false, code, message }}
 */
export function validatePilotLedgerState(raw, { pilotId, expectedVersion = CI_DEV_PILOT_LEDGER_VERSION } = {}) {
  if (raw == null || typeof raw !== "object" || Array.isArray(raw)) {
    return { ok: false, code: "LEDGER_CORRUPT", message: "Ledger root is not an object" };
  }
  if (raw.version != null && raw.version !== expectedVersion) {
    return {
      ok: false,
      code: "LEDGER_INCOMPATIBLE",
      message: `Ledger version ${raw.version} incompatible with ${expectedVersion}`,
    };
  }
  if (!raw.pilot_id || (pilotId && raw.pilot_id !== pilotId)) {
    return {
      ok: false,
      code: "LEDGER_INCOMPATIBLE",
      message: `Ledger pilot_id mismatch (saved=${raw.pilot_id}, expected=${pilotId})`,
    };
  }
  if (!Array.isArray(raw.hotel_ids)) {
    return { ok: false, code: "LEDGER_CORRUPT", message: "hotel_ids must be an array" };
  }
  if (typeof raw.hotels !== "object" || raw.hotels == null || Array.isArray(raw.hotels)) {
    return { ok: false, code: "LEDGER_CORRUPT", message: "hotels must be an object map" };
  }
  if (!Array.isArray(raw.outstanding_workflow_reservations)) {
    return {
      ok: false,
      code: "LEDGER_CORRUPT",
      message: "outstanding_workflow_reservations must be an array",
    };
  }
  if (!Number.isFinite(Number(raw.batch_started_ms))) {
    return { ok: false, code: "LEDGER_CORRUPT", message: "batch_started_ms missing or invalid" };
  }
  return { ok: true, state: raw };
}

/**
 * @param {object} policy — from buildCiDevPilotExecutionPolicy
 * @param {{ storeRoot?: string, nowFn?: Function, pilotId?: string }} opts
 */
export function createPilotSpendTracker(policy, opts = {}) {
  const nowFn = opts.nowFn || (() => Date.now());
  const storeRoot = opts.storeRoot || policy.case_store_root || null;
  const pilotId = opts.pilotId || buildPilotId(policy);
  const batchLimit = Number(policy.limits.total.max_elapsed_ms_batch);
  const caseLimit = Number(policy.limits.per_case.max_elapsed_ms_per_case);
  const totalContext = Number(policy.limits.total.context_dev);
  const perCaseContext = Number(policy.limits.per_case.context_dev_max);
  const usdPerCredit = Number(policy.cost_accounting?.context_dev?.usd_per_credit);
  const usdCap = Number(policy.cost_accounting?.context_dev?.usd_cap);
  const usdEnforced =
    policy.credit_limited_usd_unknown !== true &&
    Number.isFinite(usdPerCredit) &&
    usdPerCredit > 0 &&
    Number.isFinite(usdCap) &&
    usdCap > 0;

  const ledgerDir = storeRoot ? path.join(storeRoot, "pilot-ledgers") : null;
  const ledgerPath = ledgerDir ? path.join(ledgerDir, `${pilotId}.json`) : null;
  const runnerLockPath = ledgerDir ? path.join(ledgerDir, `${pilotId}.runner.lock`) : null;
  let runnerLock = null;

  function acquireRunnerLock() {
    if (!runnerLockPath) return { ok: true, skipped: true };
    ensureDir(path.dirname(runnerLockPath));
    const now = Date.now();
    const ttlMs = Number(opts.runnerLockTtlMs) || 3_600_000;
    const lock = {
      pilot_id: pilotId,
      lock_token: `plk_${crypto.randomBytes(6).toString("hex")}`,
      pid: process.pid,
      acquired_at: new Date(now).toISOString(),
      expires_at_ms: now + ttlMs,
    };
    try {
      const fd = fs.openSync(runnerLockPath, "wx");
      try {
        fs.writeFileSync(fd, `${JSON.stringify(lock, null, 2)}\n`);
      } finally {
        fs.closeSync(fd);
      }
      runnerLock = lock;
      return { ok: true, lock };
    } catch (err) {
      if (err?.code !== "EEXIST") {
        return { ok: false, reason: "RUNNER_LOCK_IO", error: err.message };
      }
    }
    let existing = null;
    try {
      existing = JSON.parse(fs.readFileSync(runnerLockPath, "utf8"));
    } catch {
      existing = null;
    }
    const holderPid = Number(existing?.pid);
    const sameProcess = Number.isFinite(holderPid) && holderPid === process.pid;
    let holderAlive = false;
    if (Number.isFinite(holderPid) && holderPid > 0) {
      try {
        process.kill(holderPid, 0);
        holderAlive = true;
      } catch {
        holderAlive = false;
      }
    }
    // Same process may open a second tracker (tests / reload) against the same lock.
    if (sameProcess && existing?.expires_at_ms > now) {
      runnerLock = existing;
      return { ok: true, lock: existing, reentrant: true };
    }
    // Dead holder → steal. Live other process → exclusive fail.
    if (existing?.expires_at_ms && existing.expires_at_ms > now && holderAlive && !sameProcess) {
      return {
        ok: false,
        reason: "PILOT_RUNNER_LOCKED",
        lock: existing,
        message: "Another pilot runner holds the exclusive ledger lock",
      };
    }
    try {
      fs.unlinkSync(runnerLockPath);
    } catch {
      return { ok: false, reason: "PILOT_RUNNER_LOCKED", lock: existing };
    }
    return acquireRunnerLock();
  }

  function releaseRunnerLock() {
    if (!runnerLockPath || !runnerLock) return;
    try {
      const existing = JSON.parse(fs.readFileSync(runnerLockPath, "utf8"));
      if (existing.lock_token === runnerLock.lock_token) {
        fs.unlinkSync(runnerLockPath);
      }
    } catch {
      /* ignore */
    }
    runnerLock = null;
  }

  const lockAcq = opts.skipRunnerLock === true ? { ok: true, skipped: true } : acquireRunnerLock();
  if (!lockAcq.ok) {
    const err = new Error(lockAcq.message || lockAcq.reason || "PILOT_RUNNER_LOCKED");
    err.code = lockAcq.reason || "PILOT_RUNNER_LOCKED";
    err.lock = lockAcq.lock || null;
    throw err;
  }

  function emptyState(batchStarted) {
    return {
      version: CI_DEV_PILOT_LEDGER_VERSION,
      pilot_id: pilotId,
      hotel_ids: [...(policy.hotel_ids || [])],
      batch_started_ms: batchStarted,
      batch_deadline_ms: batchStarted + batchLimit,
      context_dev_spent: 0,
      usd_spent: 0,
      cancelled_batch: false,
      cancel_reason: null,
      hotels: {},
      outstanding_workflow_reservations: [],
      updated_at: new Date(batchStarted).toISOString(),
    };
  }

  /**
   * Fail closed on corruption / incompatible saved state.
   * Never silently restore a fresh budget over a damaged file.
   */
  function loadOrInit() {
    if (!ledgerPath || !fs.existsSync(ledgerPath)) {
      return emptyState(nowFn());
    }
    let raw;
    try {
      const text = fs.readFileSync(ledgerPath, "utf8");
      if (!text.trim()) {
        throw ledgerRecoveryError("LEDGER_CORRUPT", "Ledger file is empty", {
          ledger_path: ledgerPath,
          preserved: true,
        });
      }
      raw = JSON.parse(text);
    } catch (err) {
      if (err?.recovery) throw err;
      throw ledgerRecoveryError("LEDGER_CORRUPT", `Ledger JSON parse failed: ${err.message}`, {
        ledger_path: ledgerPath,
        preserved: true,
        cause: err,
      });
    }
    const validated = validatePilotLedgerState(raw, { pilotId });
    if (!validated.ok) {
      throw ledgerRecoveryError(validated.code, validated.message, {
        ledger_path: ledgerPath,
        preserved: true,
        pilot_id: pilotId,
      });
    }
    return validated.state;
  }

  let state = loadOrInit();

  function persist() {
    state.updated_at = new Date(nowFn()).toISOString();
    if (!ledgerPath) return;
    writeJsonAtomic(ledgerPath, state);
  }

  function recomputeBatchTotals() {
    state.context_dev_spent = Object.values(state.hotels).reduce(
      (s, h) => s + Number(h.spent || 0),
      0
    );
    if (Number.isFinite(usdPerCredit) && usdEnforced) {
      state.usd_spent = state.context_dev_spent * usdPerCredit;
    } else {
      state.usd_spent = "UNKNOWN";
    }
  }

  function caseState(hotelId) {
    if (!state.hotels[hotelId]) {
      const started = nowFn();
      state.hotels[hotelId] = {
        case_id: null,
        started_ms: started,
        case_deadline_ms: started + caseLimit,
        spent: 0,
        journal_outstanding: 0,
        cancelled: false,
        cancel_reason: null,
      };
      persist();
    }
    return state.hotels[hotelId];
  }

  function getBoundCaseId(hotelId) {
    return state.hotels[hotelId]?.case_id || null;
  }

  function batchElapsed() {
    return nowFn() - state.batch_started_ms;
  }

  function caseElapsed(hotelId) {
    return nowFn() - caseState(hotelId).started_ms;
  }

  function remainingCaseMs(hotelId) {
    return Math.max(0, caseLimit - caseElapsed(hotelId));
  }

  function remainingBatchMs() {
    return Math.max(0, batchLimit - batchElapsed());
  }

  function workflowOutstandingFor(hotelId) {
    return state.outstanding_workflow_reservations
      .filter((r) => r.hotel_id === hotelId && r.status === "OUTSTANDING")
      .reduce((s, r) => s + Number(r.credits || 0), 0);
  }

  /**
   * Outstanding credit liability without double-counting workflow envelope + journal.
   * When a hotel has an open workflow reservation, journal outstanding is absorbed
   * into that envelope (same reserved credits, not additive).
   */
  function outstandingLiabilityCredits(hotelId = null) {
    if (hotelId != null) {
      const wf = workflowOutstandingFor(hotelId);
      const journal = Number(caseState(hotelId).journal_outstanding || 0);
      return wf > 0 ? wf : journal;
    }
    let total = 0;
    const hotelIds = new Set([
      ...Object.keys(state.hotels),
      ...state.outstanding_workflow_reservations.map((r) => r.hotel_id),
    ]);
    for (const id of hotelIds) {
      total += outstandingLiabilityCredits(id);
    }
    return total;
  }

  function totalOutstanding(hotelId) {
    return outstandingLiabilityCredits(hotelId);
  }

  function remainingBatchContext() {
    return Math.max(0, totalContext - state.context_dev_spent - outstandingLiabilityCredits());
  }

  function remainingCaseContext(hotelId) {
    const cs = caseState(hotelId);
    return Math.max(0, perCaseContext - Number(cs.spent || 0) - outstandingLiabilityCredits(hotelId));
  }

  /**
   * USD remaining after spent AND outstanding liability (credits × rate).
   * Uses non-double-counting outstanding liability.
   */
  function remainingUsd() {
    if (!usdEnforced) return Infinity; // USD not an active gate (UNKNOWN / credit-limited)
    const outstandingUsd = outstandingLiabilityCredits() * usdPerCredit;
    return Math.max(0, usdCap - state.usd_spent - outstandingUsd);
  }

  function hasOutstanding(hotelId) {
    return outstandingLiabilityCredits(hotelId) > 0;
  }

  function dispatchTimeGate(hotelId) {
    if (state.cancelled_batch) {
      return { ok: false, allowance: 0, reason: state.cancel_reason || "BATCH_CANCELLED" };
    }
    const cs = caseState(hotelId);
    if (cs.cancelled) {
      return { ok: false, allowance: 0, reason: cs.cancel_reason || "CASE_CANCELLED" };
    }
    if (batchElapsed() >= batchLimit) {
      state.cancelled_batch = true;
      state.cancel_reason = "BATCH_DEADLINE_EXCEEDED";
      persist();
      return { ok: false, allowance: 0, reason: "BATCH_DEADLINE_EXCEEDED" };
    }
    if (caseElapsed(hotelId) >= caseLimit) {
      cs.cancelled = true;
      cs.cancel_reason = "CASE_DEADLINE_EXCEEDED";
      persist();
      return { ok: false, allowance: 0, reason: "CASE_DEADLINE_EXCEEDED" };
    }
    return { ok: true };
  }

  function allowDispatch(hotelId, { requested = perCaseContext } = {}) {
    const timeGate = dispatchTimeGate(hotelId);
    if (!timeGate.ok) return timeGate;
    if (remainingBatchContext() <= 0) {
      return { ok: false, allowance: 0, reason: "BATCH_CONTEXT_EXHAUSTED" };
    }
    if (remainingCaseContext(hotelId) <= 0) {
      return { ok: false, allowance: 0, reason: "CASE_CONTEXT_EXHAUSTED" };
    }
    if (usdEnforced && remainingUsd() <= 0) {
      state.cancelled_batch = true;
      state.cancel_reason = "USD_CAP_EXHAUSTED";
      persist();
      return { ok: false, allowance: 0, reason: "USD_CAP_EXHAUSTED" };
    }
    const byUsd = usdEnforced ? Math.floor(remainingUsd() / usdPerCredit) : Number.POSITIVE_INFINITY;
    const allowance = Math.max(
      0,
      Math.min(Number(requested) || 0, remainingCaseContext(hotelId), remainingBatchContext(), byUsd)
    );
    if (allowance <= 0) {
      return { ok: false, allowance: 0, reason: "NO_ALLOWANCE" };
    }
    return { ok: true, allowance, reason: null };
  }

  /**
   * Gate a single Context.dev operation after an optional workflow envelope reserve.
   *
   * When a workflow reservation is OUTSTANDING, that envelope already counts as
   * liability in remainingCaseContext — nested ops must use envelope headroom
   * (envelope − journal spent − open journal reserved), not remainingCaseContext
   * (which would always be 0 after a full envelope reserve).
   *
   * When no envelope is open (shared-pool per-op gating), fall through to allowDispatch.
   */
  function allowOperationDispatch(
    hotelId,
    { requested = 1, budgetUsage = null, workflowReservationCredits = null } = {}
  ) {
    const timeGate = dispatchTimeGate(hotelId);
    if (!timeGate.ok) return timeGate;

    const need = Math.max(0, Number(requested) || 0);
    const wf =
      workflowReservationCredits != null && Number.isFinite(Number(workflowReservationCredits))
        ? Math.max(0, Number(workflowReservationCredits))
        : workflowOutstandingFor(hotelId);

    if (wf > 0) {
      const journalSpent = Number(
        budgetUsage?.context_dev_spent != null
          ? budgetUsage.context_dev_spent
          : caseState(hotelId).spent || 0
      );
      const journalReserved = Number(budgetUsage?.context_dev_reserved || 0);
      const headroom = Math.max(0, wf - journalSpent - journalReserved);
      if (usdEnforced && remainingUsd() <= 0) {
        state.cancelled_batch = true;
        state.cancel_reason = "USD_CAP_EXHAUSTED";
        persist();
        return { ok: false, allowance: 0, reason: "USD_CAP_EXHAUSTED" };
      }
      const byUsd = usdEnforced ? Math.floor(remainingUsd() / usdPerCredit) : Number.POSITIVE_INFINITY;
      const allowance = Math.max(0, Math.min(need, headroom, byUsd));
      if (allowance <= 0) {
        return {
          ok: false,
          allowance: 0,
          reason: headroom <= 0 ? "CASE_CONTEXT_EXHAUSTED" : "NO_ALLOWANCE",
          envelope_credits: wf,
          headroom,
        };
      }
      return {
        ok: true,
        allowance,
        reason: null,
        envelope_credits: wf,
        headroom,
        mode: "workflow_envelope",
      };
    }

    return { ...allowDispatch(hotelId, { requested: need }), mode: "open_pool" };
  }

  function reserve(hotelId, credits) {
    const gate = allowDispatch(hotelId, { requested: credits });
    if (!gate.ok) {
      return { ok: false, reservation: null, reason: gate.reason, allowance: 0 };
    }
    const reservation = {
      id: `res_${crypto.randomBytes(6).toString("hex")}`,
      hotel_id: hotelId,
      credits: gate.allowance,
      status: "OUTSTANDING",
      reserved_at: new Date(nowFn()).toISOString(),
      case_id: caseState(hotelId).case_id || null,
    };
    state.outstanding_workflow_reservations.push(reservation);
    persist();
    return { ok: true, reservation, allowance: gate.allowance, reason: null };
  }

  function bindCase(hotelId, caseId) {
    const cs = caseState(hotelId);
    if (caseId) cs.case_id = caseId;
    for (const r of state.outstanding_workflow_reservations) {
      if (r.hotel_id === hotelId && r.status === "OUTSTANDING" && !r.case_id) {
        r.case_id = caseId;
      }
    }
    persist();
    return cs;
  }

  /**
   * Reconcile from durable case + operation journal.
   * When journal restore yields complete provider-confirmed spend, apply it
   * absolutely (may decrease an inflated ledger/case snapshot).
   * Otherwise keep Math.max so legacy gaps are not under-counted.
   * Workflow reservations are cleared; journal_outstanding carries remaining liability
   * (avoids double-counting workflow + journal for the same envelope).
   */
  function reconcileFromCase(hotelId, caseRecord) {
    const cs = caseState(hotelId);
    if (caseRecord?.case_id) cs.case_id = caseRecord.case_id;

    const journal = caseRecord?.operation_journal || [];
    const restored = restoreBudgetUsageFromJournal(caseRecord?.budget_usage || {}, journal);
    const outstanding = outstandingReservationsFromJournal(journal);

    const journalSpent = Number(restored.context_dev_spent || 0);
    const authoritative =
      restored.context_dev_spend_source === "provider_key_metadata_per_request" ||
      restored.context_dev_spend_source === "partial_provider_metadata_plus_liability";
    if (authoritative) {
      cs.spent = journalSpent;
    } else {
      cs.spent = Math.max(Number(cs.spent || 0), journalSpent);
    }
    cs.journal_outstanding = Math.max(
      Number(outstanding.context_dev_outstanding || 0),
      Number(restored.context_dev_reserved || 0)
    );
    recomputeBatchTotals();

    for (const r of state.outstanding_workflow_reservations) {
      if (r.hotel_id === hotelId && r.status === "OUTSTANDING") {
        r.status = "RECONCILED";
        r.reconciled_at = new Date(nowFn()).toISOString();
        r.reconciled_spent = journalSpent;
        r.reconciled_journal_outstanding = cs.journal_outstanding;
      }
    }
    persist();
    return {
      spent: cs.spent,
      journal_outstanding: cs.journal_outstanding,
      open_reservations: outstanding.open_reservations,
      liability_credits: outstandingLiabilityCredits(hotelId),
      spend_source: restored.context_dev_spend_source || null,
      snapshot_spent_superseded: restored.context_dev_snapshot_spent_superseded ?? null,
    };
  }

  function settleOrKeepOutstanding(hotelId, reservationId, { spentCredits = null, keepOutstanding = false } = {}) {
    const res = state.outstanding_workflow_reservations.find((r) => r.id === reservationId);
    if (!res || res.status !== "OUTSTANDING") {
      return { ok: false, reason: "RESERVATION_NOT_FOUND" };
    }
    const cs = caseState(hotelId);
    if (keepOutstanding || spentCredits == null) {
      persist();
      return { ok: true, outstanding: true, reservation: res };
    }
    const used = Math.max(0, Number(spentCredits) || 0);
    cs.spent = Number(cs.spent || 0) + used;
    recomputeBatchTotals();
    res.status = "SETTLED";
    res.settled_credits = used;
    res.settled_at = new Date(nowFn()).toISOString();
    persist();
    return { ok: true, spent: cs.spent, reservation: res };
  }

  function allowProviderDispatch(hotelId) {
    if (state.cancelled_batch) {
      return { ok: false, reason: state.cancel_reason || "BATCH_CANCELLED" };
    }
    const cs = caseState(hotelId);
    if (cs.cancelled) {
      return { ok: false, reason: cs.cancel_reason || "CASE_CANCELLED" };
    }
    if (batchElapsed() >= batchLimit) {
      state.cancelled_batch = true;
      state.cancel_reason = "BATCH_DEADLINE_EXCEEDED";
      persist();
      return { ok: false, reason: "BATCH_DEADLINE_EXCEEDED" };
    }
    if (caseElapsed(hotelId) >= caseLimit) {
      cs.cancelled = true;
      cs.cancel_reason = "CASE_DEADLINE_EXCEEDED";
      persist();
      return { ok: false, reason: "CASE_DEADLINE_EXCEEDED" };
    }
    return {
      ok: true,
      remaining_case_ms: remainingCaseMs(hotelId),
      remaining_batch_ms: remainingBatchMs(),
    };
  }

  function recordSpend(hotelId, credits) {
    const used = Math.max(0, Number(credits) || 0);
    const open = state.outstanding_workflow_reservations.find(
      (r) => r.hotel_id === hotelId && r.status === "OUTSTANDING"
    );
    if (open) {
      return settleOrKeepOutstanding(hotelId, open.id, {
        spentCredits: used,
        keepOutstanding: false,
      });
    }
    const cs = caseState(hotelId);
    cs.spent = Number(cs.spent || 0) + used;
    recomputeBatchTotals();
    persist();
    return { case_spent: cs.spent, batch_spent: state.context_dev_spent, usd_spent: state.usd_spent };
  }

  function applySpendCorrection({
    correction_id,
    corrected_spent,
    outstanding_liability = 0,
    hard_cap = null,
    per_hotel = [],
    reconciliation_summary = null,
    at = null,
  } = {}) {
    if (!correction_id) return { ok: false, reason: "CORRECTION_ID_REQUIRED" };
    if (state.spend_correction?.correction_id === correction_id) {
      return {
        ok: true,
        idempotent: true,
        spent: state.context_dev_spent,
        spend_correction: state.spend_correction,
      };
    }
    const priorSpent = Number(state.context_dev_spent || 0);
    for (const h of per_hotel) {
      const cs = caseState(h.hotel_id);
      if (h.case_id) cs.case_id = h.case_id;
      cs.spent = Number(h.spent || 0);
      cs.journal_outstanding = Number(h.journal_outstanding || 0);
    }
    if (hard_cap != null && Number.isFinite(Number(hard_cap))) {
      state.authorized_credit_hard_cap = Number(hard_cap);
    }
    // Absolute correction must win — do not let recomputeBatchTotals resurrect
    // inflated hotel rows that were not included in per_hotel.
    recomputeBatchTotals();
    state.context_dev_spent = Number(corrected_spent || 0);
    state.spend_correction = {
      correction_id,
      prior_settled_credits: priorSpent,
      corrected_spent: state.context_dev_spent,
      outstanding_liability: Number(outstanding_liability || 0),
      applied_at: at || new Date(nowFn()).toISOString(),
      reconciliation_summary,
      account_wide_excluded: true,
    };
    for (const r of state.outstanding_workflow_reservations) {
      if (r.status === "OUTSTANDING") {
        r.status = "RECONCILED_BY_CORRECTION";
        r.reconciled_at = state.spend_correction.applied_at;
      }
    }
    persist();
    return {
      ok: true,
      idempotent: false,
      spent: state.context_dev_spent,
      prior_spent: priorSpent,
      spend_correction: state.spend_correction,
      remaining: Math.max(
        0,
        (Number(hard_cap) || totalContext) -
          state.context_dev_spent -
          outstandingLiabilityCredits()
      ),
    };
  }

  function blockFurtherContextDev(reason) {
    state.cancelled_batch = true;
    state.cancel_reason = reason || "CONTEXT_DEV_CONSUMPTION_EXCEEDED_RESERVATION";
    persist();
    return { ok: true, cancel_reason: state.cancel_reason };
  }

  /**
   * Open a new elapsed-time window for a founder-authorized resume without resetting spend.
   * Clears only deadline-driven cancellations. Preserves context_dev_spent and case spend.
   */
  function refreshAuthorizedResumeWindow({
    reason = "FOUNDER_AUTHORIZED_LIVE_RESUME",
    hotelIds = null,
  } = {}) {
    const now = nowFn();
    const prior = {
      batch_started_ms: state.batch_started_ms,
      batch_deadline_ms: state.batch_deadline_ms,
      cancelled_batch: state.cancelled_batch,
      cancel_reason: state.cancel_reason,
      context_dev_spent: state.context_dev_spent,
    };
    state.batch_started_ms = now;
    state.batch_deadline_ms = now + batchLimit;
    if (
      state.cancel_reason === "BATCH_DEADLINE_EXCEEDED" ||
      (state.cancelled_batch && !state.cancel_reason)
    ) {
      state.cancelled_batch = false;
      state.cancel_reason = null;
    }
    const ids = Array.isArray(hotelIds) && hotelIds.length ? hotelIds : Object.keys(state.hotels);
    const hotelPriors = {};
    for (const id of ids) {
      const cs = caseState(id);
      hotelPriors[id] = {
        started_ms: cs.started_ms,
        case_deadline_ms: cs.case_deadline_ms,
        cancelled: cs.cancelled,
        cancel_reason: cs.cancel_reason,
        spent: cs.spent,
      };
      cs.started_ms = now;
      cs.case_deadline_ms = now + caseLimit;
      if (cs.cancel_reason === "CASE_DEADLINE_EXCEEDED") {
        cs.cancelled = false;
        cs.cancel_reason = null;
      }
    }
    state.resume_window = {
      refreshed_at: new Date(now).toISOString(),
      reason,
      prior,
      hotel_priors: hotelPriors,
      spend_preserved: Number(state.context_dev_spent || 0),
      batch_limit_ms: batchLimit,
      case_limit_ms: caseLimit,
    };
    persist();
    return { ok: true, resume_window: state.resume_window, snapshot: snapshot() };
  }

  function snapshot() {
    const hardCap = Number(state.authorized_credit_hard_cap) || totalContext;
    return {
      version: state.version,
      pilot_id: state.pilot_id,
      ledger_path: ledgerPath,
      context_dev_spent: state.context_dev_spent,
      usd_spent: state.usd_spent,
      outstanding_liability_credits: outstandingLiabilityCredits(),
      outstanding_liability_usd: usdEnforced
        ? outstandingLiabilityCredits() * usdPerCredit
        : "UNKNOWN",
      remaining_credits: Math.max(
        0,
        hardCap - Number(state.context_dev_spent || 0) - outstandingLiabilityCredits()
      ),
      authorized_credit_hard_cap: state.authorized_credit_hard_cap || hardCap,
      remaining_usd: usdEnforced ? remainingUsd() : "UNKNOWN",
      usd_enforced: usdEnforced,
      batch_elapsed_ms: batchElapsed(),
      cancelled_batch: state.cancelled_batch,
      cancel_reason: state.cancel_reason,
      spend_correction: state.spend_correction || null,
      per_case: { ...state.hotels },
      outstanding_workflow_reservations: state.outstanding_workflow_reservations.filter(
        (r) => r.status === "OUTSTANDING"
      ),
      limits: policy.limits,
    };
  }

  persist();

  return {
    pilotId,
    ledgerPath,
    allowDispatch,
    allowOperationDispatch,
    reserve,
    bindCase,
    getBoundCaseId,
    reconcileFromCase,
    settleOrKeepOutstanding,
    allowProviderDispatch,
    recordSpend,
    applySpendCorrection,
    blockFurtherContextDev,
    refreshAuthorizedResumeWindow,
    remainingBatchContext,
    remainingCaseContext,
    remainingCaseMs,
    remainingBatchMs,
    remainingUsd,
    outstandingLiabilityCredits,
    hasOutstanding,
    snapshot,
    persist,
    releaseRunnerLock,
    runnerLockPath,
    reload: () => {
      state = loadOrInit();
      return snapshot();
    },
    perCaseContextDevMax: perCaseContext,
    sharedPoolMode: policy.shared_pool_mode === true,
  };
}
