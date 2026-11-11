/**
 * Isolated ledger for one gpt-4o-mini planner certification.
 * Ceiling $0.01. This file must not point at a Parallel or benchmark ledger.
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";

export const PLANNER_EVALUATION_LEDGER_VERSION = "gdi-planner-evaluation-ledger-v1";
export const PLANNER_EVALUATION_BUDGET_USD = 0.01;

export function assertIsolatedPlannerLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (/parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget/i.test(normalized)) {
    const err = new Error("planner_ledger_not_isolated");
    err.code = "PLANNER_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith("reports/gdi-planner-evaluation-v1/budget-ledger.json")) {
    const err = new Error("planner_ledger_path_refused");
    err.code = "PLANNER_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger() {
  return {
    version: PLANNER_EVALUATION_LEDGER_VERSION,
    currency: "USD",
    budgetUsd: PLANNER_EVALUATION_BUDGET_USD,
    isolatedFromParallelComparison: true,
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

export function loadOrCreatePlannerLedger(ledgerPath) {
  assertIsolatedPlannerLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger();
    writeLedger(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (ledger.version !== PLANNER_EVALUATION_LEDGER_VERSION || Number(ledger.budgetUsd) !== PLANNER_EVALUATION_BUDGET_USD) {
    const err = new Error("planner_ledger_budget_refused");
    err.code = "PLANNER_LEDGER_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

export function reservePlannerCall({ ledgerPath, jobKey, quotedUsd } = {}) {
  const ledger = loadOrCreatePlannerLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted > 0) || quoted - PLANNER_EVALUATION_BUDGET_USD > 1e-9) {
    const err = new Error("planner_quote_above_authorization");
    err.code = "PLANNER_QUOTE_ABOVE_AUTHORIZATION";
    throw err;
  }
  const used = (ledger.entries || []).find((entry) => entry.status === "spent" || entry.status === "reserved" || entry.status === "failed");
  if (used) return { ok: false, reason: "planner_call_already_used", ledger, entry: used };
  const reservationId = `rsv_planner_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  ledger.reservedUsd = quoted;
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

export function commitPlannerCall({ ledgerPath, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreatePlannerLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("planner_reservation_missing");
    err.code = "PLANNER_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true, ledger };
  const actual = Number(actualUsd);
  if (!(actual >= 0) || actual - PLANNER_EVALUATION_BUDGET_USD > 1e-9 || actual - Number(entry.quotedUsd) > 1e-9) {
    const err = new Error("planner_actual_above_reservation");
    err.code = "PLANNER_ACTUAL_ABOVE_RESERVATION";
    throw err;
  }
  entry.status = "spent";
  entry.actualUsd = actual;
  entry.usage = usage || null;
  entry.spentAt = new Date().toISOString();
  ledger.reservedUsd = 0;
  ledger.estimatedSpentUsd = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}

export function releasePlannerCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreatePlannerLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true, ledger };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = 0;
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}

export function failPlannerCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreatePlannerLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true, ledger };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "planner_call_failed";
  ledger.reservedUsd = 0;
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}
