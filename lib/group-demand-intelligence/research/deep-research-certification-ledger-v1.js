/**
 * Isolated ledger for the extended deep-research certification.
 * Refuses Cohort A, segment, Parallel comparison, and prior planner/cert ledgers.
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";

export const DEEP_RESEARCH_LEDGER_VERSION = "gdi-autonomous-deep-research-certification-ledger-v1";
export const DEEP_RESEARCH_BUDGET_USD = 0.05;
export const DEEP_RESEARCH_CALL_CEILING_USD = 0.01;
export const DEEP_RESEARCH_MAX_PLANNER_CALLS = 3;
export const DEEP_RESEARCH_MAX_RETRIEVALS = 2;
export const DEEP_RESEARCH_LEDGER_RELATIVE = "reports/gdi-autonomous-deep-research-certification-v1/budget-ledger.json";

const FORBIDDEN_LEDGER = /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation-v1|gdi-autonomous-research-certification-v1\/budget-ledger/i;

export function assertDeepResearchLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (FORBIDDEN_LEDGER.test(normalized)) {
    const err = new Error("deep_research_ledger_not_isolated");
    err.code = "DEEP_RESEARCH_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith(DEEP_RESEARCH_LEDGER_RELATIVE)) {
    const err = new Error("deep_research_ledger_path_refused");
    err.code = "DEEP_RESEARCH_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger() {
  return {
    version: DEEP_RESEARCH_LEDGER_VERSION,
    currency: "USD",
    budgetUsd: DEEP_RESEARCH_BUDGET_USD,
    isolatedFromParallelComparison: true,
    isolatedFromEarlierResearchLedgers: true,
    isolatedFromNarCertificationLedger: true,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    plannerCalls: 0,
    retrievals: 0,
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

export function loadOrCreateDeepResearchLedger(ledgerPath) {
  assertDeepResearchLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger();
    writeLedger(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (ledger.version !== DEEP_RESEARCH_LEDGER_VERSION || Number(ledger.budgetUsd) !== DEEP_RESEARCH_BUDGET_USD) {
    const err = new Error("deep_research_ledger_budget_refused");
    err.code = "DEEP_RESEARCH_LEDGER_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

function plannerSpentOrReserved(ledger) {
  return (ledger.entries || []).filter((entry) => entry.kind === "planner" && (entry.status === "spent" || entry.status === "reserved" || entry.status === "failed")).length;
}

export function reserveDeepResearchCall({ ledgerPath, jobKey, quotedUsd, kind = "planner" } = {}) {
  const ledger = loadOrCreateDeepResearchLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted >= 0) || (quoted > 0 && quoted - DEEP_RESEARCH_CALL_CEILING_USD > 1e-9)) {
    const err = new Error("deep_research_quote_above_call_ceiling");
    err.code = "DEEP_RESEARCH_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((entry) => entry.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used", entry: existing, ledger };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true, ledger };
  }
  if (kind === "planner" && plannerSpentOrReserved(ledger) >= DEEP_RESEARCH_MAX_PLANNER_CALLS) {
    const err = new Error("deep_research_planner_cap");
    err.code = "DEEP_RESEARCH_PLANNER_CAP";
    throw err;
  }
  if (kind === "retrieval" && Number(ledger.retrievals || 0) >= DEEP_RESEARCH_MAX_RETRIEVALS) {
    const err = new Error("deep_research_retrieval_cap");
    err.code = "DEEP_RESEARCH_RETRIEVAL_CAP";
    throw err;
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (quoted > 0 && committed + quoted - DEEP_RESEARCH_BUDGET_USD > 1e-9) {
    const err = new Error("deep_research_budget_exhausted");
    err.code = "DEEP_RESEARCH_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_drc_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
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
  return { ok: true, reservationId, ledger };
}

export function commitDeepResearchCall({ ledgerPath, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreateDeepResearchLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("deep_research_reservation_missing");
    err.code = "DEEP_RESEARCH_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true, ledger };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - DEEP_RESEARCH_BUDGET_USD > 1e-9) {
    const err = new Error("deep_research_actual_above_reservation");
    err.code = "DEEP_RESEARCH_ACTUAL_ABOVE_RESERVATION";
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
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}

export function releaseDeepResearchCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateDeepResearchLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true, ledger };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}

export function failDeepResearchCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateDeepResearchLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true, ledger };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true, ledger };
}
