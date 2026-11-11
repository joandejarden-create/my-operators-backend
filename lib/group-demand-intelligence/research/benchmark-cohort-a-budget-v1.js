/**
 * Cohort A benchmark budget. Separate file from the Parallel ledger.
 * Uses the existing mkdir lock. Ceilings come from code, not from the file.
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";
import { withParallelBudgetLock } from "../parallel/budget-ledger-v1.js";
import {
  COHORT_A_AGGREGATE_USD,
  COHORT_A_BUDGET_VERSION,
  COHORT_A_HOTELS,
  cohortAHotel,
  isCohortBHotel,
} from "./benchmark-cohort-a-registry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function fail(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  Object.assign(err, extra);
  return err;
}

export function benchmarkBudgetLedgerPath(env = process.env) {
  if (env.GDI_BENCHMARK_BUDGET_LEDGER_PATH) {
    return path.resolve(env.GDI_BENCHMARK_BUDGET_LEDGER_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "benchmark-provider-budget", "cohort-a-ledger.json");
}

function emptyLedger() {
  const hotelCeilingsUsd = {};
  for (const hotel of Object.values(COHORT_A_HOTELS)) {
    hotelCeilingsUsd[hotel.censusRecordId] = hotel.ceilingUsd;
  }
  return {
    version: COHORT_A_BUDGET_VERSION,
    currency: "USD",
    aggregateCeilingUsd: COHORT_A_AGGREGATE_USD,
    hotelCeilingsUsd,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

export function readBenchmarkBudgetLedger({ ledgerPath, createIfMissing = false } = {}) {
  const resolved = ledgerPath || benchmarkBudgetLedgerPath();
  if (!fs.existsSync(resolved)) {
    if (!createIfMissing) {
      throw fail(
        "BENCHMARK_BUDGET_LEDGER_MISSING",
        `benchmark_budget_ledger_missing:${resolved}`,
        { ledgerPath: resolved }
      );
    }
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    const fresh = emptyLedger();
    fs.writeFileSync(resolved, JSON.stringify(fresh, null, 2) + "\n");
    return { ...fresh, path: resolved };
  }
  const ledger = JSON.parse(fs.readFileSync(resolved, "utf8"));
  return { ...ledger, path: resolved };
}

function writeUnlocked(ledgerPath, ledger) {
  ledger.updatedAt = new Date().toISOString();
  const tmp = `${ledgerPath}.${process.pid}.${Date.now()}.tmp`;
  const copy = { ...ledger };
  delete copy.path;
  fs.writeFileSync(tmp, JSON.stringify(copy, null, 2) + "\n");
  fs.renameSync(tmp, ledgerPath);
  return copy;
}

export function benchmarkSpendTotals(ledger) {
  const byHotel = {};
  let reservedUsd = 0;
  let actualUsd = 0;
  for (const entry of ledger.entries || []) {
    const hotel = (byHotel[entry.hotelId] ||= { reservedUsd: 0, actualUsd: 0 });
    if (entry.status === "reserved") {
      reservedUsd += Number(entry.reservedUsd || 0);
      hotel.reservedUsd += Number(entry.reservedUsd || 0);
    } else if (
      entry.status === "reconciled" ||
      entry.status === "reconciled_over_ceiling"
    ) {
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

export function remainingBenchmarkBudgetUsd(ledger, hotelId) {
  const totals = benchmarkSpendTotals(ledger);
  const hotel = cohortAHotel(hotelId);
  const hotelCommitted =
    (totals.byHotel[hotelId]?.reservedUsd || 0) +
    (totals.byHotel[hotelId]?.actualUsd || 0);
  return {
    aggregateRemainingUsd: Number(
      (COHORT_A_AGGREGATE_USD - totals.committedUsd).toFixed(6)
    ),
    hotelRemainingUsd: hotel
      ? Number((hotel.ceilingUsd - hotelCommitted).toFixed(6))
      : 0,
    totals,
  };
}

function findEntry(ledger, { reservationId, requestFingerprint }) {
  return (ledger.entries || []).find(
    (entry) =>
      (reservationId && entry.reservationId === reservationId) ||
      (requestFingerprint &&
        entry.requestFingerprint === requestFingerprint &&
        entry.status !== "released")
  );
}

export function reserveBenchmarkBudget({
  hotelId,
  provider,
  arm,
  operation,
  requestFingerprint,
  maxUsd,
  reservationId = null,
  ledgerPath = null,
  env = process.env,
} = {}) {
  if (isCohortBHotel(hotelId)) {
    throw fail("COHORT_B_NOT_AUTHORIZED", "cohort_b_spend_refused", { hotelId });
  }
  const hotel = cohortAHotel(hotelId);
  if (!hotel) {
    throw fail("HOTEL_NOT_IN_COHORT_A", "hotel_not_in_cohort_a", { hotelId });
  }
  if (provider !== "exa" && provider !== "firecrawl") {
    throw fail("BENCHMARK_PROVIDER_UNSUPPORTED", "benchmark_provider_unsupported", {
      provider,
    });
  }
  if (!requestFingerprint) {
    throw fail("FINGERPRINT_REQUIRED", "benchmark_fingerprint_required");
  }
  const cost = Number(maxUsd);
  if (!Number.isFinite(cost) || cost <= 0) {
    throw fail("UNBOUNDED_COST", "benchmark_cost_unbounded", { maxUsd });
  }

  const resolved = ledgerPath || benchmarkBudgetLedgerPath(env);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readBenchmarkBudgetLedger({
      ledgerPath: resolved,
      createIfMissing: true,
    });
    const existing = findEntry(ledger, { reservationId, requestFingerprint });
    if (existing) {
      return {
        ok: true,
        idempotent: true,
        reservationId: existing.reservationId,
        reservedUsd: existing.reservedUsd,
        status: existing.status,
        ledger,
      };
    }
    const remaining = remainingBenchmarkBudgetUsd(ledger, hotel.censusRecordId);
    if (cost > remaining.hotelRemainingUsd + 1e-9) {
      throw fail("HOTEL_CEILING_EXCEEDED", "benchmark_hotel_ceiling_exceeded", {
        hotelId: hotel.censusRecordId,
        ceilingUsd: hotel.ceilingUsd,
        remainingUsd: remaining.hotelRemainingUsd,
        attemptedUsd: cost,
      });
    }
    if (cost > remaining.aggregateRemainingUsd + 1e-9) {
      throw fail("AGGREGATE_CEILING_EXCEEDED", "benchmark_aggregate_ceiling_exceeded", {
        ceilingUsd: COHORT_A_AGGREGATE_USD,
        remainingUsd: remaining.aggregateRemainingUsd,
        attemptedUsd: cost,
      });
    }
    const id =
      reservationId ||
      `brsv_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
    ledger.entries.push({
      reservationId: id,
      requestFingerprint,
      hotelId: hotel.censusRecordId,
      provider,
      arm: arm || null,
      operation: operation || null,
      estimatedUsd: cost,
      reservedUsd: cost,
      actualUsd: null,
      status: "reserved",
      reservedAt: new Date().toISOString(),
    });
    writeUnlocked(resolved, ledger);
    return {
      ok: true,
      idempotent: false,
      reservationId: id,
      reservedUsd: cost,
      status: "reserved",
    };
  });
}

export function reconcileBenchmarkReservation(reservationId, opts = {}) {
  const resolved = opts.ledgerPath || benchmarkBudgetLedgerPath(opts.env || process.env);
  const actual = Number(opts.actualUsd);
  if (!Number.isFinite(actual) || actual < 0) {
    throw fail("UNBOUNDED_COST", "benchmark_actual_unbounded", { actualUsd: opts.actualUsd });
  }
  return withParallelBudgetLock(resolved, () => {
    const ledger = readBenchmarkBudgetLedger({ ledgerPath: resolved });
    const entry = (ledger.entries || []).find((row) => row.reservationId === reservationId);
    if (!entry) {
      throw fail("RESERVATION_NOT_FOUND", "benchmark_reservation_not_found", {
        reservationId,
      });
    }
    if (entry.status === "reconciled" || entry.status === "reconciled_over_ceiling") {
      return { ok: true, idempotent: true, entry, ledger };
    }
    if (entry.status !== "reserved") {
      throw fail("RESERVATION_NOT_OPEN", "benchmark_reservation_not_open", {
        status: entry.status,
      });
    }
    entry.actualUsd = Number(actual.toFixed(6));
    entry.reconciledAt = new Date().toISOString();
    if (actual > Number(entry.reservedUsd) + 1e-9) {
      entry.status = "reconciled_over_ceiling";
      entry.overrunUsd = Number((actual - entry.reservedUsd).toFixed(6));
    } else {
      entry.status = "reconciled";
      entry.releasedUsd = Number((entry.reservedUsd - actual).toFixed(6));
    }
    writeUnlocked(resolved, ledger);
    return { ok: true, idempotent: false, entry };
  });
}

export function releaseBenchmarkReservation(reservationId, opts = {}) {
  const resolved = opts.ledgerPath || benchmarkBudgetLedgerPath(opts.env || process.env);
  return withParallelBudgetLock(resolved, () => {
    const ledger = readBenchmarkBudgetLedger({ ledgerPath: resolved });
    const entry = (ledger.entries || []).find((row) => row.reservationId === reservationId);
    if (!entry) return { ok: true, idempotent: true, ledger };
    if (entry.status === "reserved") {
      entry.status = "released";
      entry.releasedAt = new Date().toISOString();
      entry.releaseReason = opts.reason || null;
      writeUnlocked(resolved, ledger);
    }
    return { ok: true, entry };
  });
}

export function getBenchmarkReservation(reservationId, opts = {}) {
  const ledger = readBenchmarkBudgetLedger({
    ledgerPath: opts.ledgerPath || benchmarkBudgetLedgerPath(opts.env || process.env),
  });
  return (ledger.entries || []).find((row) => row.reservationId === reservationId) || null;
}
