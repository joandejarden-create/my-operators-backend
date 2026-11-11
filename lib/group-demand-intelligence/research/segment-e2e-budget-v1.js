/**
 * Segment E2E certification budget.
 * Code ceilings are $2 per named hotel and $6 aggregate.
 * This ledger cannot charge the Cohort A authorization.
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { withParallelBudgetLock } from "../parallel/budget-ledger-v1.js";
import { isCohortBHotel } from "./benchmark-cohort-a-registry-v1.js";

export const SEGMENT_E2E_BUDGET_VERSION = "gdi-segment-e2e-budget-v1";
export const SEGMENT_E2E_AGGREGATE_USD = 6;

export const SEGMENT_E2E_HOTELS = Object.freeze({
  recLuxvwwxID7U2B8: Object.freeze({
    censusRecordId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott",
    segmentId: "associations_government",
    ceilingUsd: 2,
  }),
  recnPB3Q02NO8Ma1o: Object.freeze({
    censusRecordId: "recnPB3Q02NO8Ma1o",
    displayName: "Wyndham San Diego Bayside",
    segmentId: "corporate_meetings_training",
    ceilingUsd: 2,
  }),
  recC2kbCaS48jFjFF: Object.freeze({
    censusRecordId: "recC2kbCaS48jFjFF",
    displayName: "Nobu Hotel London Portman Square",
    segmentId: "professional_planners_dmc",
    ceilingUsd: 2,
  }),
});

function fail(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  Object.assign(err, extra);
  return err;
}

export function assertSegmentLedgerSeparated(ledgerPath) {
  const resolved = path.resolve(String(ledgerPath || ""));
  if (/cohort-a-live|cohort-a-ledger|benchmark-provider-budget/i.test(resolved)) {
    throw fail("COHORT_A_LEDGER_FORBIDDEN", "segment_e2e_cannot_use_cohort_a_ledger", {
      ledgerPath: resolved,
    });
  }
  return resolved;
}

function emptyLedger() {
  const hotelCeilingsUsd = {};
  for (const hotel of Object.values(SEGMENT_E2E_HOTELS)) {
    hotelCeilingsUsd[hotel.censusRecordId] = hotel.ceilingUsd;
  }
  return {
    version: SEGMENT_E2E_BUDGET_VERSION,
    currency: "USD",
    aggregateCeilingUsd: SEGMENT_E2E_AGGREGATE_USD,
    hotelCeilingsUsd,
    nontransferable: true,
    cohortADraw: false,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

export function readSegmentE2eLedger({ ledgerPath, createIfMissing = false } = {}) {
  const resolved = assertSegmentLedgerSeparated(ledgerPath);
  if (!fs.existsSync(resolved)) {
    if (!createIfMissing) {
      throw fail("SEGMENT_LEDGER_MISSING", "segment_e2e_ledger_missing", { ledgerPath: resolved });
    }
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    fs.writeFileSync(resolved, JSON.stringify(emptyLedger(), null, 2) + "\n");
  }
  const ledger = JSON.parse(fs.readFileSync(resolved, "utf8"));
  if (ledger.version !== SEGMENT_E2E_BUDGET_VERSION) {
    throw fail("SEGMENT_LEDGER_VERSION", "segment_e2e_ledger_version_mismatch", {
      version: ledger.version,
    });
  }
  return { ...ledger, path: resolved };
}

function writeUnlocked(ledgerPath, ledger) {
  ledger.updatedAt = new Date().toISOString();
  const copy = { ...ledger };
  delete copy.path;
  const tmp = `${ledgerPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(copy, null, 2) + "\n");
  fs.renameSync(tmp, ledgerPath);
}

export function segmentSpendTotals(ledger) {
  const byHotel = {};
  let reservedUsd = 0;
  let actualUsd = 0;
  for (const entry of ledger.entries || []) {
    const hotel = (byHotel[entry.hotelId] ||= { reservedUsd: 0, actualUsd: 0 });
    if (entry.status === "reserved") {
      reservedUsd += Number(entry.reservedUsd || 0);
      hotel.reservedUsd += Number(entry.reservedUsd || 0);
    } else if (entry.status === "reconciled" || entry.status === "reconciled_over_ceiling") {
      actualUsd += Number(entry.actualUsd || 0);
      hotel.actualUsd += Number(entry.actualUsd || 0);
    }
  }
  return {
    reservedUsd: Number(reservedUsd.toFixed(6)),
    actualUsd: Number(actualUsd.toFixed(6)),
    committedUsd: Number((reservedUsd + actualUsd).toFixed(6)),
    byHotel,
  };
}

export function remainingSegmentBudgetUsd(ledger, hotelId) {
  const hotel = SEGMENT_E2E_HOTELS[hotelId];
  const totals = segmentSpendTotals(ledger);
  const hotelCommitted =
    (totals.byHotel[hotelId]?.reservedUsd || 0) + (totals.byHotel[hotelId]?.actualUsd || 0);
  return {
    aggregateRemainingUsd: Number((SEGMENT_E2E_AGGREGATE_USD - totals.committedUsd).toFixed(6)),
    hotelRemainingUsd: hotel ? Number((hotel.ceilingUsd - hotelCommitted).toFixed(6)) : 0,
    ceilingUsd: hotel?.ceilingUsd || 0,
    totals,
  };
}

export function reserveSegmentE2eBudget({
  hotelId,
  provider,
  operation,
  requestFingerprint,
  maxUsd,
  ledgerPath,
  segmentId = null,
  purpose = null,
} = {}) {
  if (isCohortBHotel(hotelId)) {
    throw fail("COHORT_B_NOT_AUTHORIZED", "cohort_b_spend_refused", { hotelId });
  }
  const hotel = SEGMENT_E2E_HOTELS[hotelId];
  if (!hotel) throw fail("HOTEL_NOT_IN_SEGMENT_E2E", "hotel_not_in_segment_e2e", { hotelId });
  if (segmentId && segmentId !== hotel.segmentId) {
    throw fail("SEGMENT_MISMATCH", "segment_budget_is_not_transferable", { segmentId, hotelId });
  }
  if (provider !== "exa" && provider !== "firecrawl") {
    throw fail("BENCHMARK_PROVIDER_UNSUPPORTED", "segment_provider_unsupported", { provider });
  }
  const cost = Number(maxUsd);
  if (!requestFingerprint || !Number.isFinite(cost) || cost <= 0) {
    throw fail("UNBOUNDED_COST", "segment_cost_unbounded", { maxUsd });
  }
  const resolved = assertSegmentLedgerSeparated(ledgerPath);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readSegmentE2eLedger({ ledgerPath: resolved, createIfMissing: true });
    const existing = (ledger.entries || []).find(
      (entry) => entry.requestFingerprint === requestFingerprint && entry.status !== "released"
    );
    if (existing) {
      return {
        ok: true,
        idempotent: true,
        reservationId: existing.reservationId,
        reservedUsd: existing.reservedUsd,
        status: existing.status,
      };
    }
    const remaining = remainingSegmentBudgetUsd(ledger, hotelId);
    if (cost > remaining.hotelRemainingUsd + 1e-9) {
      throw fail("HOTEL_CEILING_EXCEEDED", "segment_hotel_ceiling_exceeded", {
        hotelId,
        ceilingUsd: hotel.ceilingUsd,
        remainingUsd: remaining.hotelRemainingUsd,
        attemptedUsd: cost,
      });
    }
    if (cost > remaining.aggregateRemainingUsd + 1e-9) {
      throw fail("AGGREGATE_CEILING_EXCEEDED", "segment_aggregate_ceiling_exceeded", {
        ceilingUsd: SEGMENT_E2E_AGGREGATE_USD,
        remainingUsd: remaining.aggregateRemainingUsd,
        attemptedUsd: cost,
      });
    }
    const reservationId = `sesv_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
    ledger.entries.push({
      reservationId,
      requestFingerprint,
      hotelId,
      segmentId: hotel.segmentId,
      provider,
      operation: operation || null,
      purpose: purpose || null,
      estimatedUsd: cost,
      reservedUsd: cost,
      actualUsd: null,
      status: "reserved",
      reservedAt: new Date().toISOString(),
      cohortADraw: false,
    });
    writeUnlocked(resolved, ledger);
    return { ok: true, idempotent: false, reservationId, reservedUsd: cost, status: "reserved" };
  });
}

export function reconcileSegmentE2eReservation(reservationId, { ledgerPath, actualUsd } = {}) {
  const actual = Number(actualUsd);
  if (!Number.isFinite(actual) || actual < 0) {
    throw fail("UNBOUNDED_COST", "segment_actual_unbounded", { actualUsd });
  }
  const resolved = assertSegmentLedgerSeparated(ledgerPath);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readSegmentE2eLedger({ ledgerPath: resolved });
    const entry = (ledger.entries || []).find((row) => row.reservationId === reservationId);
    if (!entry) throw fail("RESERVATION_NOT_FOUND", "segment_reservation_not_found", { reservationId });
    if (entry.status === "reconciled" || entry.status === "reconciled_over_ceiling") {
      return { ok: true, idempotent: true, entry };
    }
    entry.actualUsd = Number(actual.toFixed(6));
    entry.reconciledAt = new Date().toISOString();
    entry.status = actual > Number(entry.reservedUsd) + 1e-9 ? "reconciled_over_ceiling" : "reconciled";
    writeUnlocked(resolved, ledger);
    return { ok: true, idempotent: false, entry };
  });
}

export function releaseSegmentReservation(reservationId, { ledgerPath, reason = "released" } = {}) {
  const resolved = assertSegmentLedgerSeparated(ledgerPath);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readSegmentE2eLedger({ ledgerPath: resolved });
    const entry = (ledger.entries || []).find((row) => row.reservationId === reservationId);
    if (!entry || entry.status !== "reserved") return { ok: true, released: false };
    entry.status = "released";
    entry.releasedAt = new Date().toISOString();
    entry.releaseReason = reason;
    writeUnlocked(resolved, ledger);
    return { ok: true, released: true };
  });
}

export function releaseOpenSegmentReservations({ ledgerPath, reason = "released_after_stop" } = {}) {
  const resolved = assertSegmentLedgerSeparated(ledgerPath);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readSegmentE2eLedger({ ledgerPath: resolved });
    let released = 0;
    for (const entry of ledger.entries || []) {
      if (entry.status === "reserved") {
        entry.status = "released";
        entry.releasedAt = new Date().toISOString();
        entry.releaseReason = reason;
        released += 1;
      }
    }
    if (released) writeUnlocked(resolved, ledger);
    return { released };
  });
}
