/**
 * ADP paid-research authorization ledger (separate from GDI envelopes).
 * Fail-closed: missing ledger must not silently create a fresh allowance.
 *
 * Authoritative path:
 *   data/ai-demand-positioning/budget/authorization-ledger-v1.json
 * Override: ADP_BUDGET_LEDGER_PATH
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

export const ADP_BUDGET_LEDGER_VERSION = "adp-authorization-ledger-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const LOCK_STALE_MS = 30_000;
const LOCK_RETRY_MS = 25;
const LOCK_MAX_WAIT_MS = 15_000;

export function adpAuthorizationLedgerPath(env = process.env) {
  if (env.ADP_BUDGET_LEDGER_PATH) {
    return path.resolve(env.ADP_BUDGET_LEDGER_PATH);
  }
  const root = env.ADP_DATA_ROOT
    ? path.resolve(env.ADP_DATA_ROOT)
    : path.join(ROOT, "data/ai-demand-positioning");
  return path.join(root, "budget", "authorization-ledger-v1.json");
}

function sleepSync(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* lock wait */
  }
}

function missingLedgerError(ledgerPath) {
  const err = new Error(
    `adp_authorization_ledger_missing: Existing ADP authorization ledger not found at ${ledgerPath}. ` +
      `Paid ADP execution is fail-closed until the ledger is created explicitly. ` +
      `Do not treat a missing file as a fresh allowance.`
  );
  err.code = "ADP_LEDGER_MISSING";
  err.ledgerPath = ledgerPath;
  return err;
}

export function withAdpBudgetLock(ledgerPath, fn) {
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
        const e = new Error("adp_budget_ledger_lock_timeout");
        e.code = "ADP_BUDGET_LOCK_TIMEOUT";
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

function writeUnlocked(ledgerPath, ledger) {
  ledger.updatedAt = new Date().toISOString();
  const tmp = `${ledgerPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(ledger, null, 2) + "\n");
  fs.renameSync(tmp, ledgerPath);
  return ledger;
}

export function readAdpBudgetLedger(opts = {}) {
  const ledgerPath = opts.ledgerPath || adpAuthorizationLedgerPath(opts.env || process.env);
  const allowBootstrapEmpty = opts.allowBootstrapEmpty === true;
  if (!fs.existsSync(ledgerPath)) {
    if (!allowBootstrapEmpty) throw missingLedgerError(ledgerPath);
    const fresh = {
      version: ADP_BUDGET_LEDGER_VERSION,
      currency: "USD",
      budgetUsd: Number(opts.budgetUsd || 0),
      reservedUsd: 0,
      estimatedSpentUsd: 0,
      actualBilledUsd: null,
      entries: [],
      updatedAt: new Date().toISOString(),
    };
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    fs.writeFileSync(ledgerPath, JSON.stringify(fresh, null, 2) + "\n");
    return { ...fresh, path: ledgerPath, bootstrappedEmpty: true };
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  return { ...ledger, path: ledgerPath };
}

export function remainingAdpBudgetUsd(ledger) {
  return (
    Number(ledger.budgetUsd || 0) -
    Number(ledger.reservedUsd || 0) -
    Number(ledger.estimatedSpentUsd || 0)
  );
}

export function reserveAdpBudget({
  label,
  estimatedUsd,
  reservationId = null,
  jobKey = null,
  hotelId = null,
  propertyId = null,
  env = process.env,
  ledgerPath = null,
} = {}) {
  const pathResolved = ledgerPath || adpAuthorizationLedgerPath(env);
  const cost = Number(estimatedUsd);
  if (!(cost >= 0)) {
    const err = new Error("adp_budget_invalid_estimate");
    err.code = "ADP_BUDGET_INVALID_ESTIMATE";
    throw err;
  }
  const id =
    reservationId ||
    `rsv_adp_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;

  return withAdpBudgetLock(pathResolved, () => {
    const ledger = readAdpBudgetLedger({ ledgerPath: pathResolved, env });
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
    const remaining = remainingAdpBudgetUsd(ledger);
    if (cost > remaining + 1e-9) {
      const err = new Error("adp_budget_exhausted");
      err.code = "ADP_BUDGET_EXHAUSTED";
      err.remainingUsd = remaining;
      err.attemptedUsd = cost;
      throw err;
    }
    ledger.reservedUsd = Number((ledger.reservedUsd + cost).toFixed(6));
    ledger.entries.push({
      reservationId: id,
      jobKey: jobKey || null,
      hotelId: hotelId || null,
      propertyId: propertyId || null,
      label: label || null,
      estimatedUsd: cost,
      status: "reserved",
      reservedAt: new Date().toISOString(),
      actualBilledUsd: null,
    });
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, reservationId: id, estimatedUsd: cost, ledger };
  });
}

export function commitAdpBudgetReservation(reservationId, opts = {}) {
  const pathResolved = opts.ledgerPath || adpAuthorizationLedgerPath(opts.env || process.env);
  return withAdpBudgetLock(pathResolved, () => {
    const ledger = readAdpBudgetLedger({
      ledgerPath: pathResolved,
      env: opts.env,
    });
    const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
    if (!entry) {
      const err = new Error("adp_reservation_not_found");
      err.code = "ADP_RESERVATION_NOT_FOUND";
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
      if (opts.periodId) entry.periodId = opts.periodId;
      if (opts.providerRunId) entry.providerRunId = opts.providerRunId;
      if (opts.actualBilledUsd != null) {
        entry.actualBilledUsd = opts.actualBilledUsd;
        ledger.actualBilledUsd =
          ledger.actualBilledUsd == null
            ? Number(opts.actualBilledUsd)
            : Number(ledger.actualBilledUsd) + Number(opts.actualBilledUsd);
      }
    }
    writeUnlocked(pathResolved, ledger);
    return { ok: true, idempotent: false, ledger };
  });
}

export function releaseAdpBudgetReservation(reservationId, opts = {}) {
  const pathResolved = opts.ledgerPath || adpAuthorizationLedgerPath(opts.env || process.env);
  return withAdpBudgetLock(pathResolved, () => {
    const ledger = readAdpBudgetLedger({
      ledgerPath: pathResolved,
      env: opts.env,
    });
    const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
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
