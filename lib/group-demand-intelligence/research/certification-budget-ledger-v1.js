/**
 * Isolated $0.02 ledger for the NAR GAD live certification.
 * Two planner calls maximum. It refuses every earlier research ledger.
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";

export const CERTIFICATION_LEDGER_VERSION = "gdi-autonomous-research-certification-ledger-v1";
export const CERTIFICATION_BUDGET_USD = 0.02;
export const CERTIFICATION_CALL_CEILING_USD = 0.01;
export const CERTIFICATION_MAX_PLANNER_CALLS = 2;

export function assertCertificationLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (/parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation-v1/i.test(normalized)) {
    const err = new Error("certification_ledger_not_isolated");
    err.code = "CERTIFICATION_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith("reports/gdi-autonomous-research-certification-v1/budget-ledger.json")) {
    const err = new Error("certification_ledger_path_refused");
    err.code = "CERTIFICATION_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger() {
  return {
    version: CERTIFICATION_LEDGER_VERSION,
    currency: "USD",
    budgetUsd: CERTIFICATION_BUDGET_USD,
    isolatedFromParallelComparison: true,
    isolatedFromEarlierResearchLedgers: true,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

function writeLedger(ledgerPath, ledger) {
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const tmp = `${ledgerPath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify({ ...ledger, updatedAt: new Date().toISOString() }, null, 2)}\n`);
  fs.rmSync(ledgerPath, { force: true });
  fs.renameSync(tmp, ledgerPath);
}

export function loadOrCreateCertificationLedger(ledgerPath) {
  assertCertificationLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger();
    writeLedger(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (ledger.version !== CERTIFICATION_LEDGER_VERSION || Number(ledger.budgetUsd) !== CERTIFICATION_BUDGET_USD) {
    const err = new Error("certification_ledger_budget_refused");
    err.code = "CERTIFICATION_LEDGER_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

function activeEntries(ledger) {
  return (ledger.entries || []).filter((entry) => entry.status === "spent" || entry.status === "reserved" || entry.status === "failed");
}

export function reserveCertificationCall({ ledgerPath, jobKey, quotedUsd } = {}) {
  const ledger = loadOrCreateCertificationLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted > 0) || quoted - CERTIFICATION_CALL_CEILING_USD > 1e-9) {
    const err = new Error("certification_quote_above_call_ceiling");
    err.code = "CERTIFICATION_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((entry) => entry.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "planner_call_already_used", entry: existing, ledger };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true, ledger };
  }
  if (activeEntries(ledger).length >= CERTIFICATION_MAX_PLANNER_CALLS) {
    const err = new Error("certification_third_call_refused");
    err.code = "CERTIFICATION_THIRD_CALL_REFUSED";
    throw err;
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (committed + quoted - CERTIFICATION_BUDGET_USD > 1e-9) {
    const err = new Error("certification_budget_exceeded");
    err.code = "CERTIFICATION_BUDGET_EXCEEDED";
    throw err;
  }
  const reservationId = `rsv_cert_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
  ledger.entries.push({
    reservationId,
    jobKey,
    quotedUsd: quoted,
    actualUsd: null,
    status: "reserved",
    reservedAt: new Date().toISOString(),
    provider: "openai",
    model: "gpt-4o-mini",
  });
  writeLedger(ledgerPath, ledger);
  return { ok: true, reservationId, ledger };
}

export function commitCertificationCall({ ledgerPath, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreateCertificationLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("certification_reservation_missing");
    err.code = "CERTIFICATION_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true, ledger };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - CERTIFICATION_BUDGET_USD > 1e-9) {
    const err = new Error("certification_actual_above_reservation");
    err.code = "CERTIFICATION_ACTUAL_ABOVE_RESERVATION";
    throw err;
  }
  entry.status = "spent";
  entry.actualUsd = actual;
  entry.usage = usage || null;
  entry.spentAt = new Date().toISOString();
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd)).toFixed(6));
  ledger.estimatedSpentUsd = nextSpent;
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}

export function releaseCertificationCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateCertificationLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true, ledger };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}

export function failCertificationCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateCertificationLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true, ledger };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "planner_call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}
