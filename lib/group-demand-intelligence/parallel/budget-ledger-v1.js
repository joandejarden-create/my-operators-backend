/**
 * Durable Parallel budget ledger with atomic reservation.
 * Estimated cost ≠ actual billed. Kill switch blocks new reservations.
 *
 * Storage: data/group-demand-intelligence/parallel-budget/ledger.json
 * Lock: mkdir-based (Windows-safe) — prefer Airtable/shared store for multi-instance
 * production; this ledger documents the contract and works for single-writer local.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";
import {
  isGdiParallelKillSwitchOn,
  isGdiParallelEnabled,
} from "./feature-flag.js";

export const PARALLEL_BUDGET_LEDGER_VERSION = "gdi-parallel-budget-ledger-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const LOCK_STALE_MS = 30_000;
const LOCK_RETRY_MS = 25;
const LOCK_MAX_WAIT_MS = 15_000;

/** Documented Parallel list-price estimates (USD) — re-check before live spend. */
export const PARALLEL_PRICE_ESTIMATES_USD = Object.freeze({
  lite: 0.005,
  base: 0.01,
  core: 0.025,
  core2x: 0.05,
  pro: 0.1,
  ultra: 0.3,
  docsNote:
    "Estimates only — read current Parallel platform pricing before paid execution (per successful Task run)",
  asOf: "2026-10-15",
  sourceUrl: "https://docs.parallel.ai/getting-started/pricing",
});

function defaultLedgerPath(env = process.env) {
  if (env.GDI_PARALLEL_BUDGET_LEDGER_PATH) {
    return path.resolve(env.GDI_PARALLEL_BUDGET_LEDGER_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "parallel-budget", "ledger.json");
}

/** Authoritative $11 paid multi-segment / Surfe shared envelope path. */
export function paidMultisegmentAuthorizationLedgerPath(env = process.env) {
  if (env.GDI_PAID_MS_BUDGET_LEDGER_PATH) {
    return path.resolve(env.GDI_PAID_MS_BUDGET_LEDGER_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "paid-multisegment-budget", "authorization-ledger-v1.json");
}

function isAuthorizationEnvelopePath(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  return /paid-multisegment-budget\/authorization-ledger-v1\.json$/i.test(
    normalized
  );
}

function missingAuthorizationLedgerError(ledgerPath) {
  const err = new Error(
    `paid_ms_authorization_ledger_missing: Existing $11 authorization ledger not found at ${ledgerPath}. ` +
      `Paid execution is fail-closed until the ledger is recovered or explicitly reconciled ` +
      `(set allowBootstrapEmpty:true only for intentional empty bootstrap). ` +
      `Do not treat a missing file as a fresh $11 allowance.`
  );
  err.code = "PAID_MS_LEDGER_MISSING";
  err.ledgerPath = ledgerPath;
  return err;
}

function sleepSync(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* short busy-wait for lock */
  }
}

function emptyLedger(budgetUsd) {
  return {
    version: PARALLEL_BUDGET_LEDGER_VERSION,
    currency: "USD",
    budgetUsd: Number(budgetUsd),
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    actualBilledUsd: null,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

export function withParallelBudgetLock(ledgerPath, fn) {
  const lockDir = `${ledgerPath}.lock`;
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const started = Date.now();
  for (;;) {
    try {
      fs.mkdirSync(lockDir);
      fs.writeFileSync(
        path.join(lockDir, "owner.json"),
        JSON.stringify({ pid: process.pid, at: new Date().toISOString() })
      );
      break;
    } catch (err) {
      if (err?.code !== "EEXIST") throw err;
      try {
        const st = fs.statSync(lockDir);
        if (Date.now() - st.mtimeMs > LOCK_STALE_MS) {
          fs.rmSync(lockDir, { recursive: true, force: true });
          continue;
        }
      } catch {
        /* retry */
      }
      if (Date.now() - started > LOCK_MAX_WAIT_MS) {
        const e = new Error("parallel_budget_ledger_lock_timeout");
        e.code = "PARALLEL_BUDGET_LOCK_TIMEOUT";
        throw e;
      }
      sleepSync(LOCK_RETRY_MS);
    }
  }
  try {
    return fn();
  } finally {
    try {
      fs.rmSync(lockDir, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  }
}

/**
 * Read budget ledger.
 *
 * Authorization envelope (`paid-multisegment-budget/authorization-ledger-v1.json`)
 * never silently bootstraps an empty ledger when missing — paid callers must
 * recover/reconcile first (or pass allowBootstrapEmpty:true explicitly).
 *
 * Historical $2 pilot ledger (`parallel-budget/ledger.json`) may still create
 * on first local read unless createIfMissing:false.
 */
export function readParallelBudgetLedger(opts = {}) {
  const ledgerPath = opts.ledgerPath || defaultLedgerPath(opts.env || process.env);
  const budgetUsd = Number(
    opts.budgetUsd ??
      (opts.env || process.env).GDI_PARALLEL_BUDGET_USD ??
      10
  );
  const isAuthEnvelope =
    opts.authorizationEnvelope === true || isAuthorizationEnvelopePath(ledgerPath);
  const allowBootstrapEmpty = opts.allowBootstrapEmpty === true;
  const createIfMissing =
    opts.createIfMissing === true
      ? true
      : opts.createIfMissing === false
        ? false
        : !isAuthEnvelope;

  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  if (!fs.existsSync(ledgerPath)) {
    if (isAuthEnvelope && !allowBootstrapEmpty) {
      throw missingAuthorizationLedgerError(ledgerPath);
    }
    if (!createIfMissing && !allowBootstrapEmpty) {
      const err = new Error(`parallel_budget_ledger_missing:${ledgerPath}`);
      err.code = "PARALLEL_BUDGET_LEDGER_MISSING";
      err.ledgerPath = ledgerPath;
      throw err;
    }
    const fresh = emptyLedger(budgetUsd);
    fs.writeFileSync(ledgerPath, JSON.stringify(fresh, null, 2) + "\n");
    return { ...fresh, path: ledgerPath, bootstrappedEmpty: true };
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  return { ...ledger, path: ledgerPath };
}

function writeUnlocked(ledgerPath, ledger) {
  ledger.updatedAt = new Date().toISOString();
  const tmp = `${ledgerPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(ledger, null, 2) + "\n");
  fs.renameSync(tmp, ledgerPath);
  return ledger;
}

/**
 * Historical unresolved accounting hold (USD).
 * Not incurred spending and not a provider reservation — reduces available only.
 *
 * Missing hold field → 0 only for a valid legacy ledger (no recorded unresolved hold).
 * Invalid, non-finite, or negative hold → fail closed (throws).
 */
export function accountingHoldUsd(ledger) {
  if (!ledger || typeof ledger !== "object") {
    const err = new Error("accounting_hold_ledger_invalid");
    err.code = "ACCOUNTING_HOLD_INVALID";
    throw err;
  }
  if (!Object.prototype.hasOwnProperty.call(ledger, "accountingHoldUsd")) {
    return 0;
  }
  const raw = ledger.accountingHoldUsd;
  if (raw === null || raw === undefined || raw === "") {
    return 0;
  }
  const n = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    const err = new Error(
      `accounting_hold_invalid: hold must be a finite non-negative number (got ${String(raw)})`
    );
    err.code = "ACCOUNTING_HOLD_INVALID";
    err.accountingHoldUsd = raw;
    throw err;
  }
  return n;
}

/**
 * Available for new reservations:
 * authorized − estimated incurred − active reservations − unresolved accounting hold.
 */
export function remainingParallelBudgetUsd(ledger) {
  return (
    Number(ledger.budgetUsd || 0) -
    Number(ledger.reservedUsd || 0) -
    Number(ledger.estimatedSpentUsd || 0) -
    accountingHoldUsd(ledger)
  );
}

/**
 * Convert part of an accounting hold into estimated incurred without double-counting.
 * Idempotent on conversionId. Does not create a provider reservation.
 */
export function convertAccountingHoldToIncurred({
  amountUsd,
  conversionId,
  label = null,
  note = null,
  env = process.env,
  ledgerPath = null,
} = {}) {
  const pathResolved = ledgerPath || defaultLedgerPath(env);
  const amount = Number(amountUsd);
  if (!Number.isFinite(amount) || amount <= 0) {
    const err = new Error("accounting_hold_conversion_amount_invalid");
    err.code = "ACCOUNTING_HOLD_CONVERSION_INVALID";
    throw err;
  }
  if (!conversionId) {
    const err = new Error("accounting_hold_conversion_id_required");
    err.code = "ACCOUNTING_HOLD_CONVERSION_INVALID";
    throw err;
  }

  return withParallelBudgetLock(pathResolved, () => {
    const ledger = readParallelBudgetLedger({ ledgerPath: pathResolved, env });
    const prior = (ledger.holdConversions || []).find(
      (c) => c.conversionId === conversionId
    );
    if (prior) {
      return { ok: true, idempotent: true, conversion: prior, ledger };
    }
    const hold = accountingHoldUsd(ledger);
    if (amount > hold + 1e-9) {
      const err = new Error("accounting_hold_conversion_exceeds_hold");
      err.code = "ACCOUNTING_HOLD_CONVERSION_EXCEEDS";
      err.holdUsd = hold;
      err.attemptedUsd = amount;
      throw err;
    }
    ledger.accountingHoldUsd = Number((hold - amount).toFixed(6));
    ledger.estimatedSpentUsd = Number(
      (Number(ledger.estimatedSpentUsd || 0) + amount).toFixed(6)
    );
    if (!Array.isArray(ledger.holdConversions)) ledger.holdConversions = [];
    const conversion = {
      conversionId,
      amountUsd: amount,
      label: label || null,
      note: note || null,
      convertedAt: new Date().toISOString(),
    };
    ledger.holdConversions.push(conversion);
    if (!ledger.reconciliation) ledger.reconciliation = {};
    ledger.reconciliation.accountingHoldUsd = ledger.accountingHoldUsd;
    ledger.reconciliation.supportedEstimatedIncurredUsd =
      ledger.estimatedSpentUsd;
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, conversion, ledger };
  });
}

/**
 * Release hold that overlays exposure already booked as incurred (or otherwise
 * unsupported as additional). Does not change estimatedSpentUsd.
 * Idempotent on releaseId.
 */
export function releaseAccountingHold({
  amountUsd,
  releaseId,
  reason = null,
  env = process.env,
  ledgerPath = null,
} = {}) {
  const pathResolved = ledgerPath || defaultLedgerPath(env);
  const amount = Number(amountUsd);
  if (!Number.isFinite(amount) || amount <= 0) {
    const err = new Error("accounting_hold_release_amount_invalid");
    err.code = "ACCOUNTING_HOLD_RELEASE_INVALID";
    throw err;
  }
  if (!releaseId) {
    const err = new Error("accounting_hold_release_id_required");
    err.code = "ACCOUNTING_HOLD_RELEASE_INVALID";
    throw err;
  }

  return withParallelBudgetLock(pathResolved, () => {
    const ledger = readParallelBudgetLedger({ ledgerPath: pathResolved, env });
    const prior = (ledger.holdReleases || []).find((r) => r.releaseId === releaseId);
    if (prior) {
      return { ok: true, idempotent: true, release: prior, ledger };
    }
    const hold = accountingHoldUsd(ledger);
    if (amount > hold + 1e-9) {
      const err = new Error("accounting_hold_release_exceeds_hold");
      err.code = "ACCOUNTING_HOLD_RELEASE_EXCEEDS";
      err.holdUsd = hold;
      err.attemptedUsd = amount;
      throw err;
    }
    ledger.accountingHoldUsd = Number((hold - amount).toFixed(6));
    if (!Array.isArray(ledger.holdReleases)) ledger.holdReleases = [];
    const release = {
      releaseId,
      amountUsd: amount,
      reason: reason || null,
      releasedAt: new Date().toISOString(),
    };
    ledger.holdReleases.push(release);
    if (!ledger.reconciliation) ledger.reconciliation = {};
    ledger.reconciliation.accountingHoldUsd = ledger.accountingHoldUsd;
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, release, ledger };
  });
}

/**
 * Increase unresolved accounting hold (additional disputed exposure).
 * Idempotent on holdId. Does not change estimatedSpentUsd.
 */
export function increaseAccountingHold({
  amountUsd,
  holdId,
  reason = null,
  env = process.env,
  ledgerPath = null,
} = {}) {
  const pathResolved = ledgerPath || defaultLedgerPath(env);
  const amount = Number(amountUsd);
  if (!Number.isFinite(amount) || amount <= 0) {
    const err = new Error("accounting_hold_increase_amount_invalid");
    err.code = "ACCOUNTING_HOLD_INCREASE_INVALID";
    throw err;
  }
  if (!holdId) {
    const err = new Error("accounting_hold_increase_id_required");
    err.code = "ACCOUNTING_HOLD_INCREASE_INVALID";
    throw err;
  }

  return withParallelBudgetLock(pathResolved, () => {
    const ledger = readParallelBudgetLedger({ ledgerPath: pathResolved, env });
    const prior = (ledger.holdIncreases || []).find((h) => h.holdId === holdId);
    if (prior) {
      return { ok: true, idempotent: true, increase: prior, ledger };
    }
    const hold = accountingHoldUsd(ledger);
    ledger.accountingHoldUsd = Number((hold + amount).toFixed(6));
    if (!Array.isArray(ledger.holdIncreases)) ledger.holdIncreases = [];
    const increase = {
      holdId,
      amountUsd: amount,
      reason: reason || null,
      increasedAt: new Date().toISOString(),
    };
    ledger.holdIncreases.push(increase);
    if (!ledger.reconciliation) ledger.reconciliation = {};
    ledger.reconciliation.accountingHoldUsd = ledger.accountingHoldUsd;
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, increase, ledger };
  });
}

export function estimateParallelTaskUsd(processor = "base") {
  return (
    PARALLEL_PRICE_ESTIMATES_USD[processor] ?? PARALLEL_PRICE_ESTIMATES_USD.base
  );
}

/**
 * Reserve estimated charge BEFORE submitting a billable Parallel task.
 * Idempotent on reservationId / jobKey when already reserved or spent.
 */
export function reserveParallelBudget({
  label,
  processor = "base",
  estimatedUsd = null,
  reservationId = null,
  jobKey = null,
  hotelId = null,
  maxUsd = null,
  env = process.env,
  ledgerPath = null,
} = {}) {
  if (isGdiParallelKillSwitchOn(env)) {
    const err = new Error("parallel_kill_switch_active");
    err.code = "PARALLEL_KILL_SWITCH";
    throw err;
  }
  if (!isGdiParallelEnabled(env) && env.GDI_PARALLEL_BUDGET_REQUIRE_FLAG !== "0") {
    const err = new Error("parallel_provider_disabled");
    err.code = "PARALLEL_DISABLED";
    throw err;
  }

  const pathResolved = ledgerPath || defaultLedgerPath(env);
  const cost =
    estimatedUsd != null
      ? Number(estimatedUsd)
      : estimateParallelTaskUsd(processor);
  const id =
    reservationId ||
    `rsv_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;

  return withParallelBudgetLock(pathResolved, () => {
    const ledger = readParallelBudgetLedger({ ledgerPath: pathResolved, env });
    const existing = (ledger.entries || []).find(
      (e) =>
        e.reservationId === id ||
        (jobKey && e.jobKey === jobKey && e.status !== "released")
    );
    if (existing) {
      return {
        ok: true,
        idempotent: true,
        reservationId: existing.reservationId,
        estimatedUsd: existing.estimatedUsd,
        ledger,
      };
    }
    const remaining = remainingParallelBudgetUsd(ledger);
    const cap =
      maxUsd != null ? Math.min(Number(maxUsd), remaining) : remaining;
    if (cost > cap + 1e-9) {
      const err = new Error("parallel_budget_exhausted");
      err.code = "PARALLEL_BUDGET_EXHAUSTED";
      err.remainingUsd = remaining;
      err.attemptedUsd = cost;
      throw err;
    }
    ledger.reservedUsd = Number((ledger.reservedUsd + cost).toFixed(6));
    ledger.entries.push({
      reservationId: id,
      jobKey: jobKey || null,
      hotelId: hotelId || null,
      label: label || null,
      processor,
      estimatedUsd: cost,
      status: "reserved",
      reservedAt: new Date().toISOString(),
    });
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, reservationId: id, estimatedUsd: cost, ledger };
  });
}

export function commitParallelBudgetReservation(reservationId, opts = {}) {
  const pathResolved = opts.ledgerPath || defaultLedgerPath(opts.env || process.env);
  return withParallelBudgetLock(pathResolved, () => {
    const ledger = readParallelBudgetLedger({
      ledgerPath: pathResolved,
      env: opts.env,
    });
    const entry = (ledger.entries || []).find(
      (e) => e.reservationId === reservationId
    );
    if (!entry) {
      const err = new Error("parallel_reservation_not_found");
      err.code = "PARALLEL_RESERVATION_NOT_FOUND";
      throw err;
    }
    if (entry.status === "spent") {
      return { ok: true, idempotent: true, ledger };
    }
    if (entry.status === "reserved") {
      ledger.reservedUsd = Number(
        Math.max(0, ledger.reservedUsd - entry.estimatedUsd).toFixed(6)
      );
      ledger.estimatedSpentUsd = Number(
        (ledger.estimatedSpentUsd + entry.estimatedUsd).toFixed(6)
      );
      entry.status = "spent";
      entry.spentAt = new Date().toISOString();
      if (opts.providerRunId) entry.providerRunId = opts.providerRunId;
    }
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, ledger };
  });
}

export function releaseParallelBudgetReservation(reservationId, opts = {}) {
  const pathResolved = opts.ledgerPath || defaultLedgerPath(opts.env || process.env);
  return withParallelBudgetLock(pathResolved, () => {
    const ledger = readParallelBudgetLedger({
      ledgerPath: pathResolved,
      env: opts.env,
    });
    const entry = (ledger.entries || []).find(
      (e) => e.reservationId === reservationId
    );
    if (!entry) return { ok: true, idempotent: true, ledger };
    if (entry.status === "reserved") {
      ledger.reservedUsd = Number(
        Math.max(0, ledger.reservedUsd - entry.estimatedUsd).toFixed(6)
      );
      entry.status = "released";
      entry.releasedAt = new Date().toISOString();
      entry.releaseReason = opts.reason || null;
    }
    writeUnlocked(pathResolved, ledger);
    return { ok: true, ledger };
  });
}
