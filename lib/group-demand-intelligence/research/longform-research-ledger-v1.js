/**
 * Isolated ledger for the Principal → Nobu long-form autonomous research certification.
 * Refuses all prior benchmark and certification ledgers.
 */

import fs from "node:fs";
import { randomBytes } from "node:crypto";

export const LONGFORM_LEDGER_VERSION = "gdi-autonomous-longform-research-certification-ledger-v1";
export const LONGFORM_BUDGET_USD = 0.15;
export const LONGFORM_CALL_CEILING_USD = 0.01;
export const LONGFORM_MAX_PLANNER_CALLS = 8;
export const LONGFORM_MAX_RETRIEVALS = 6;
export const LONGFORM_LEDGER_RELATIVE = "reports/gdi-autonomous-longform-research-certification-v1/budget-ledger.json";

const FORBIDDEN = /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation-v1|gdi-autonomous-research-certification-v1\/budget-ledger|gdi-autonomous-deep-research-certification-v1\/budget-ledger/i;

export function assertLongformLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (FORBIDDEN.test(normalized)) {
    const err = new Error("longform_ledger_not_isolated");
    err.code = "LONGFORM_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith(LONGFORM_LEDGER_RELATIVE)) {
    const err = new Error("longform_ledger_path_refused");
    err.code = "LONGFORM_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger() {
  return {
    version: LONGFORM_LEDGER_VERSION,
    currency: "USD",
    budgetUsd: LONGFORM_BUDGET_USD,
    isolatedFromAllPriorResearchLedgers: true,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    plannerCalls: 0,
    retrievals: 0,
    substantiveQuestions: 0,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

function writeLedger(ledgerPath, ledger) {
  fs.mkdirSync(pathDir(ledgerPath), { recursive: true });
  const tmp = `${ledgerPath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify({ ...ledger, updatedAt: new Date().toISOString() }, null, 2)}\n`);
  fs.rmSync(ledgerPath, { force: true });
  fs.renameSync(tmp, ledgerPath);
}

function pathDir(filePath) {
  const parts = String(filePath || "").replace(/\\/g, "/").split("/");
  parts.pop();
  return parts.join("/") || ".";
}

export function loadOrCreateLongformLedger(ledgerPath) {
  assertLongformLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger();
    writeLedger(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (ledger.version !== LONGFORM_LEDGER_VERSION || Number(ledger.budgetUsd) !== LONGFORM_BUDGET_USD) {
    const err = new Error("longform_ledger_budget_refused");
    err.code = "LONGFORM_LEDGER_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

export function reserveLongformCall({ ledgerPath, jobKey, quotedUsd, kind = "planner" } = {}) {
  const ledger = loadOrCreateLongformLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted >= 0) || (quoted > 0 && quoted - LONGFORM_CALL_CEILING_USD > 1e-9)) {
    const err = new Error("longform_quote_above_call_ceiling");
    err.code = "LONGFORM_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((entry) => entry.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used", entry: existing };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true };
  }
  if (kind === "planner" && Number(ledger.plannerCalls || 0) >= LONGFORM_MAX_PLANNER_CALLS) {
    const err = new Error("longform_planner_cap");
    err.code = "LONGFORM_PLANNER_CAP";
    throw err;
  }
  if (kind === "retrieval" && Number(ledger.retrievals || 0) >= LONGFORM_MAX_RETRIEVALS) {
    const err = new Error("longform_retrieval_cap");
    err.code = "LONGFORM_RETRIEVAL_CAP";
    throw err;
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (quoted > 0 && committed + quoted - LONGFORM_BUDGET_USD > 1e-9) {
    const err = new Error("longform_budget_exhausted");
    err.code = "LONGFORM_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_lf_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  if (quoted > 0) {
    ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
    ledger.entries.push({
      reservationId,
      jobKey,
      kind,
      quotedUsd: quoted,
      actualUsd: null,
      status: "reserved",
      reservedAt: new Date().toISOString(),
    });
  } else {
    ledger.entries.push({
      reservationId,
      jobKey,
      kind,
      quotedUsd: 0,
      actualUsd: 0,
      status: "spent",
      spentAt: new Date().toISOString(),
    });
    if (kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
    if (kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  }
  writeLedger(ledgerPath, ledger);
  return { ok: true, reservationId };
}

export function commitLongformCall({ ledgerPath, reservationId, actualUsd, usage, substantiveQuestion = false } = {}) {
  const ledger = loadOrCreateLongformLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("longform_reservation_missing");
    err.code = "LONGFORM_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - LONGFORM_BUDGET_USD > 1e-9) {
    const err = new Error("longform_actual_above_reservation");
    err.code = "LONGFORM_ACTUAL_ABOVE_RESERVATION";
    throw err;
  }
  entry.status = "spent";
  entry.actualUsd = actual;
  entry.usage = usage || null;
  entry.spentAt = new Date().toISOString();
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd)).toFixed(6));
  ledger.estimatedSpentUsd = nextSpent;
  if (entry.kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  if (entry.kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
  if (substantiveQuestion) ledger.substantiveQuestions = Number(ledger.substantiveQuestions || 0) + 1;
  writeLedger(ledgerPath, ledger);
  return { ok: true };
}

export function releaseLongformCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateLongformLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true };
}

export function failLongformCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateLongformLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true };
}
