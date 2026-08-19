/**
 * Idempotency + duplicate-run protection for brand longitudinal measurement.
 */

import fs from "fs";
import path from "path";
import { BRAND_LONGITUDINAL_STORE_ROOT } from "./measurement-period.js";

export const BRAND_LONGITUDINAL_IDEMPOTENCY_VERSION = "brand_longitudinal_idempotency_v1";

const LEDGER_FILENAME = "measurement-ledger.json";

function ledgerPath(storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  return path.join(storeRoot, LEDGER_FILENAME);
}

function readLedger(storeRoot) {
  const p = ledgerPath(storeRoot);
  if (!fs.existsSync(p)) {
    return { version: BRAND_LONGITUDINAL_IDEMPOTENCY_VERSION, locks: {}, completed: [] };
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeLedger(ledger, storeRoot) {
  fs.mkdirSync(storeRoot, { recursive: true });
  fs.writeFileSync(ledgerPath(storeRoot), JSON.stringify(ledger, null, 2), "utf8");
}

/**
 * Build idempotency key for a scheduled measurement cycle.
 */
export function buildMeasurementIdempotencyKey(parts = {}) {
  return [
    parts.cohortId || "",
    parts.cohortVersion || "",
    parts.measurementDate || "",
    parts.datasetNamespace || "",
    parts.cadence || "monthly",
  ].join("|");
}

/**
 * Attempt to acquire measurement period lock.
 * @returns {{ acquired: boolean, reason?: string, existingPeriodId?: string }}
 */
export function acquireMeasurementLock(idempotencyKey, periodId, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  const ledger = readLedger(storeRoot);
  const existing = ledger.locks?.[idempotencyKey];
  if (existing && existing.periodId !== periodId) {
    return {
      acquired: false,
      reason: "NO_SECOND_EXECUTION",
      existingPeriodId: existing.periodId,
    };
  }
  ledger.locks = ledger.locks || {};
  ledger.locks[idempotencyKey] = {
    periodId,
    acquiredAt: new Date().toISOString(),
  };
  writeLedger(ledger, storeRoot);
  return { acquired: true };
}

/**
 * Mark measurement period complete in ledger.
 */
export function completeMeasurementLock(idempotencyKey, periodId, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  const ledger = readLedger(storeRoot);
  ledger.completed = ledger.completed || [];
  if (!ledger.completed.includes(periodId)) {
    ledger.completed.push(periodId);
  }
  if (ledger.locks?.[idempotencyKey]) {
    ledger.locks[idempotencyKey].completedAt = new Date().toISOString();
  }
  writeLedger(ledger, storeRoot);
  return ledger;
}

/**
 * Check if idempotency key already has a completed period.
 */
export function isDuplicateMeasurementCycle(idempotencyKey, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  const ledger = readLedger(storeRoot);
  const lock = ledger.locks?.[idempotencyKey];
  if (!lock) return { duplicate: false };
  if (lock.completedAt) {
    return { duplicate: true, reason: "NO_SECOND_EXECUTION", periodId: lock.periodId };
  }
  return { duplicate: false, inProgress: true, periodId: lock.periodId };
}
