/**
 * Isolated $0.75 ledger for Nobu × professional_planners_dmc segment pilot.
 * Refuses all prior research ledgers.
 */

import fs from "node:fs";
import { randomBytes } from "node:crypto";
import {
  SEGMENT_PILOT_BUDGET_USD,
  SEGMENT_PILOT_CALL_CEILING_USD,
  SEGMENT_PILOT_MAX_PLANNER_CALLS,
  SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS,
  SEGMENT_PILOT_LEDGER_RELATIVE,
  SEGMENT_PILOT_VERSION,
} from "./segment-pilot-professional-planners-dmc-nobu-v1.js";

export const SEGMENT_PILOT_LEDGER_VERSION = `${SEGMENT_PILOT_VERSION}-ledger`;

const FORBIDDEN =
  /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation|gdi-autonomous-research-certification-v1\/budget-ledger|gdi-autonomous-deep-research|gdi-autonomous-longform|cross-organization-followup-savoy|cross-organization-live-recovery|nobu-london-portman-budget/i;

function pathDir(filePath) {
  const parts = String(filePath || "").replace(/\\/g, "/").split("/");
  parts.pop();
  return parts.join("/") || ".";
}

function writeLedger(ledgerPath, ledger) {
  fs.mkdirSync(pathDir(ledgerPath), { recursive: true });
  const tmp = `${ledgerPath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify({ ...ledger, updatedAt: new Date().toISOString() }, null, 2)}\n`);
  fs.rmSync(ledgerPath, { force: true });
  fs.renameSync(tmp, ledgerPath);
}

export function assertSegmentPilotLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (FORBIDDEN.test(normalized)) {
    const err = new Error("segment_pilot_ledger_not_isolated");
    err.code = "SEGMENT_PILOT_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith(SEGMENT_PILOT_LEDGER_RELATIVE)) {
    const err = new Error("segment_pilot_ledger_path_refused");
    err.code = "SEGMENT_PILOT_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger() {
  return {
    version: SEGMENT_PILOT_LEDGER_VERSION,
    currency: "USD",
    budgetUsd: SEGMENT_PILOT_BUDGET_USD,
    isolatedFromAllPriorResearchLedgers: true,
    usesPriorResearchLedgers: false,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    plannerCalls: 0,
    retrievals: 0,
    failedTechnicalAttempts: 0,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

export function loadOrCreateSegmentPilotLedger(ledgerPath) {
  assertSegmentPilotLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger();
    writeLedger(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (Number(ledger.budgetUsd) !== SEGMENT_PILOT_BUDGET_USD) {
    const err = new Error("segment_pilot_budget_refused");
    err.code = "SEGMENT_PILOT_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

export function reserveSegmentPilotCall({ ledgerPath, jobKey, quotedUsd, kind = "planner" } = {}) {
  const ledger = loadOrCreateSegmentPilotLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted >= 0) || (quoted > 0 && quoted - SEGMENT_PILOT_CALL_CEILING_USD > 1e-9)) {
    const err = new Error("segment_pilot_quote_above_call_ceiling");
    err.code = "SEGMENT_PILOT_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((entry) => entry.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used", entry: existing };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true };
  }
  if (kind === "planner" && Number(ledger.plannerCalls || 0) >= SEGMENT_PILOT_MAX_PLANNER_CALLS) {
    const err = new Error("segment_pilot_planner_cap");
    err.code = "SEGMENT_PILOT_PLANNER_CAP";
    throw err;
  }
  if (kind === "retrieval" && Number(ledger.retrievals || 0) >= SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS) {
    const err = new Error("segment_pilot_retrieval_cap");
    err.code = "SEGMENT_PILOT_RETRIEVAL_CAP";
    throw err;
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (quoted > 0 && committed + quoted - SEGMENT_PILOT_BUDGET_USD > 1e-9) {
    const err = new Error("segment_pilot_budget_exhausted");
    err.code = "SEGMENT_PILOT_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_sp_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
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

export function commitSegmentPilotCall({ ledgerPath, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreateSegmentPilotLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("segment_pilot_reservation_missing");
    err.code = "SEGMENT_PILOT_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - SEGMENT_PILOT_BUDGET_USD > 1e-9) {
    const err = new Error("segment_pilot_actual_above_reservation");
    err.code = "SEGMENT_PILOT_ACTUAL_ABOVE_RESERVATION";
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
  return { ok: true };
}

export function releaseSegmentPilotCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateSegmentPilotLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeLedger(ledgerPath, ledger);
  return { ok: true };
}

export function failSegmentPilotCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateSegmentPilotLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  if (entry.kind === "retrieval") {
    ledger.failedTechnicalAttempts = Number(ledger.failedTechnicalAttempts || 0) + 1;
  }
  writeLedger(ledgerPath, ledger);
  return { ok: true };
}

export function recordSegmentPilotFailedTechnical({ ledgerPath }) {
  const ledger = loadOrCreateSegmentPilotLedger(ledgerPath);
  ledger.failedTechnicalAttempts = Number(ledger.failedTechnicalAttempts || 0) + 1;
  writeLedger(ledgerPath, ledger);
  return ledger.failedTechnicalAttempts;
}
