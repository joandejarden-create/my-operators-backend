/**
 * Nontransferable per-hotel ledgers for the $4.50 three-hotel benchmark.
 */

import fs from "node:fs";
import { randomBytes } from "node:crypto";
import {
  THREE_HOTEL_BENCHMARK_HOTELS,
  THREE_HOTEL_BENCHMARK_VERSION,
} from "./three-hotel-benchmark-authorization-v2.js";

const FORBIDDEN =
  /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|gdi-planner-evaluation|gdi-autonomous-research-certification-v1\/budget|gdi-autonomous-deep|gdi-autonomous-longform|cross-organization|segment-pilot-professional|nobu-london-portman-budget/i;

const CALL_CEILING = 0.01;

function pathDir(filePath) {
  const parts = String(filePath || "").replace(/\\/g, "/").split("/");
  parts.pop();
  return parts.join("/") || ".";
}

function writeJson(filePath, data) {
  fs.mkdirSync(pathDir(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify({ ...data, updatedAt: new Date().toISOString() }, null, 2)}\n`);
  fs.rmSync(filePath, { force: true });
  fs.renameSync(tmp, filePath);
}

export function hotelBenchmarkSpec(hotelId) {
  return THREE_HOTEL_BENCHMARK_HOTELS.find((h) => h.hotelId === hotelId) || null;
}

export function assertThreeHotelBenchmarkLedger(ledgerPath, hotelId) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (FORBIDDEN.test(normalized)) {
    const err = new Error("three_hotel_benchmark_ledger_not_isolated");
    err.code = "THREE_HOTEL_LEDGER_NOT_ISOLATED";
    throw err;
  }
  const spec = hotelBenchmarkSpec(hotelId);
  if (!spec) {
    const err = new Error("three_hotel_unknown_hotel");
    err.code = "THREE_HOTEL_UNKNOWN_HOTEL";
    throw err;
  }
  if (!normalized.endsWith(spec.ledgerRelative)) {
    const err = new Error("three_hotel_ledger_path_refused");
    err.code = "THREE_HOTEL_LEDGER_PATH_REFUSED";
    throw err;
  }
}

export function loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelId) {
  assertThreeHotelBenchmarkLedger(ledgerPath, hotelId);
  const spec = hotelBenchmarkSpec(hotelId);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = {
      version: `${THREE_HOTEL_BENCHMARK_VERSION}-ledger`,
      hotelId,
      hotelName: spec.hotelName,
      segmentId: spec.segmentId,
      currency: "USD",
      budgetUsd: spec.ceilingUsd,
      budgetTransferable: false,
      isolatedFromAllPriorResearchLedgers: true,
      reservedUsd: 0,
      estimatedSpentUsd: 0,
      plannerCalls: 0,
      retrievals: 0,
      entries: [],
    };
    writeJson(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (Number(ledger.budgetUsd) !== Number(spec.ceilingUsd) || ledger.hotelId !== hotelId) {
    const err = new Error("three_hotel_ledger_budget_refused");
    err.code = "THREE_HOTEL_LEDGER_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

export function reserveHotelBenchmarkCall({ ledgerPath, hotelId, jobKey, quotedUsd, kind = "planner" } = {}) {
  const ledger = loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelId);
  const quoted = Number(quotedUsd);
  if (!(quoted >= 0) || (quoted > 0 && quoted - CALL_CEILING > 1e-9)) {
    const err = new Error("three_hotel_quote_above_call_ceiling");
    err.code = "THREE_HOTEL_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((e) => e.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used", entry: existing };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true };
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (quoted > 0 && committed + quoted - Number(ledger.budgetUsd) > 1e-9) {
    const err = new Error("three_hotel_budget_exhausted");
    err.code = "THREE_HOTEL_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_th_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  if (quoted > 0) {
    ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
    ledger.entries.push({
      reservationId, jobKey, kind, quotedUsd: quoted, actualUsd: null, status: "reserved",
      reservedAt: new Date().toISOString(),
    });
  } else {
    ledger.entries.push({
      reservationId, jobKey, kind, quotedUsd: 0, actualUsd: 0, status: "spent",
      spentAt: new Date().toISOString(),
    });
    if (kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
    if (kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  }
  writeJson(ledgerPath, ledger);
  return { ok: true, reservationId };
}

export function commitHotelBenchmarkCall({ ledgerPath, hotelId, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelId);
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry) {
    const err = new Error("three_hotel_reservation_missing");
    err.code = "THREE_HOTEL_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - Number(ledger.budgetUsd) > 1e-9) {
    const err = new Error("three_hotel_actual_above_reservation");
    err.code = "THREE_HOTEL_ACTUAL_ABOVE_RESERVATION";
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
  writeJson(ledgerPath, ledger);
  return { ok: true };
}

export function failHotelBenchmarkCall({ ledgerPath, hotelId, reservationId, reason } = {}) {
  const ledger = loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelId);
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeJson(ledgerPath, ledger);
  return { ok: true };
}

export function releaseHotelBenchmarkCall({ ledgerPath, hotelId, reservationId, reason } = {}) {
  const ledger = loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelId);
  const entry = (ledger.entries || []).find((e) => e.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeJson(ledgerPath, ledger);
  return { ok: true };
}
