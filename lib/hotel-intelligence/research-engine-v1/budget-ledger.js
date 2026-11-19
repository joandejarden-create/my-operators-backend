/**
 * Per-hotel / per-organization budget + provider-usage ledger.
 * Unknown USD must never be stored or reported as 0.
 *
 * Reservation API (required by execution gateway):
 *   reserve → settle | release
 * Durable operation identity prevents duplicate spend on resume.
 */

import fs from "node:fs";
import path from "node:path";
import { ensureDir } from "../local-store.js";
import { FAILURE_CLASS } from "./contract.js";

export const BUDGET_LEDGER_VERSION = "he-intel-budget-ledger-v1";

function nowIso() {
  return new Date().toISOString();
}

function normalizeUsd(value, statusHint) {
  if (value == null || value === "" || value === "UNKNOWN") {
    return { usd: null, usd_status: "UNKNOWN" };
  }
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return { usd: null, usd_status: "UNKNOWN" };
  }
  return { usd: n, usd_status: statusHint || "REPORTED" };
}

export function createBudgetLedger(opts = {}) {
  const perHotelUsdCap = opts.perHotelUsdCap ?? null; // null = no USD cap (UNKNOWN pricing mode)
  const perOrgUsdCap = opts.perOrgUsdCap ?? null;
  const perHotelCreditCap = opts.perHotelCreditCap ?? null;
  const entries = [];
  const hotelTotals = new Map();
  const orgTotals = new Map();
  /** @type {Map<string, object>} */
  const operations = new Map();

  function hotelBucket(hotelId) {
    const id = String(hotelId || "unknown");
    if (!hotelTotals.has(id)) {
      hotelTotals.set(id, {
        hotel_id: id,
        usd: null,
        usd_status: "UNKNOWN",
        credits: 0,
        reserved_credits: 0,
        reserved_usd: 0,
        calls: 0,
        refused: 0,
        failed: 0,
      });
    }
    return hotelTotals.get(id);
  }

  function orgBucket(orgKey) {
    const id = String(orgKey || "unknown");
    if (!orgTotals.has(id)) {
      orgTotals.set(id, {
        org_key: id,
        usd: null,
        usd_status: "UNKNOWN",
        credits: 0,
        reserved_credits: 0,
        reserved_usd: 0,
        calls: 0,
      });
    }
    return orgTotals.get(id);
  }

  function addUsd(bucket, usd, status) {
    if (usd == null || status === "UNKNOWN") {
      bucket.usd_status = bucket.usd == null ? "UNKNOWN" : bucket.usd_status;
      return;
    }
    bucket.usd = Number(((bucket.usd || 0) + usd).toFixed(6));
    bucket.usd_status = "REPORTED";
  }

  function canSpend({ hotelId, orgKey, costUsd, credits = 0 } = {}) {
    const { usd, usd_status } = normalizeUsd(costUsd);
    const h = hotelBucket(hotelId);
    const pendingCredits = Number(h.reserved_credits || 0);
    const pendingUsd = Number(h.reserved_usd || 0);
    if (
      perHotelCreditCap != null &&
      h.credits + pendingCredits + Number(credits || 0) > perHotelCreditCap
    ) {
      return { ok: false, error: FAILURE_CLASS.BUDGET_EXHAUSTED, scope: "hotel_credits" };
    }
    if (
      usd != null &&
      perHotelUsdCap != null &&
      (h.usd || 0) + pendingUsd + usd > perHotelUsdCap
    ) {
      return { ok: false, error: FAILURE_CLASS.BUDGET_EXHAUSTED, scope: "hotel_usd" };
    }
    if (orgKey && usd != null && perOrgUsdCap != null) {
      const o = orgBucket(orgKey);
      if ((o.usd || 0) + Number(o.reserved_usd || 0) + usd > perOrgUsdCap) {
        return { ok: false, error: FAILURE_CLASS.BUDGET_EXHAUSTED, scope: "org_usd" };
      }
    }
    // If USD required for enforcement but unknown, refuse rather than treat as 0
    if (
      perHotelUsdCap != null &&
      usd_status === "UNKNOWN" &&
      costUsd !== 0 &&
      costUsd != null
    ) {
      return {
        ok: false,
        error: FAILURE_CLASS.BUDGET_EXHAUSTED,
        scope: "usd_unknown_under_cap",
        note: "unknown_cost_cannot_be_zeroed",
      };
    }
    return { ok: true, usd, usd_status };
  }

  function getOperation(operationId) {
    if (!operationId) return null;
    return operations.get(String(operationId)) || null;
  }

  /**
   * Reserve budget before provider execution. Fail-closed on exhaustion.
   * Idempotent for already-RESERVED / SETTLED operation ids.
   */
  function reserve(entry = {}) {
    const operationId = String(entry.operation_id || "").trim();
    if (!operationId) {
      return { ok: false, error: FAILURE_CLASS.INPUT_INVALID, message: "operation_id required" };
    }
    const prior = operations.get(operationId);
    if (prior?.status === "SETTLED") {
      return {
        ok: true,
        already_settled: true,
        reservation_id: operationId,
        operation: prior,
      };
    }
    if (prior?.status === "RESERVED") {
      return {
        ok: true,
        already_reserved: true,
        reservation_id: operationId,
        operation: prior,
      };
    }

    const credits = Number(entry.credits || 0) || 0;
    const gate = canSpend({
      hotelId: entry.hotel_id,
      orgKey: entry.org_key,
      costUsd: entry.cost_usd,
      credits,
    });
    if (!gate.ok) {
      entries.push({
        at: nowIso(),
        provider: entry.provider || null,
        operation: entry.operation || null,
        operation_id: operationId,
        hotel_id: entry.hotel_id || null,
        org_key: entry.org_key || null,
        status: "RESERVE_REFUSED",
        cost_usd: null,
        cost_usd_status: "UNKNOWN",
        credits: 0,
        reason: gate.error,
        scope: gate.scope || null,
        retry: Boolean(entry.retry),
      });
      return gate;
    }

    const { usd, usd_status } = gate;
    const h = hotelBucket(entry.hotel_id);
    h.reserved_credits += credits;
    if (usd != null) h.reserved_usd = Number(((h.reserved_usd || 0) + usd).toFixed(6));
    if (entry.org_key) {
      const o = orgBucket(entry.org_key);
      o.reserved_credits += credits;
      if (usd != null) o.reserved_usd = Number(((o.reserved_usd || 0) + usd).toFixed(6));
    }

    const op = {
      operation_id: operationId,
      status: "RESERVED",
      provider: entry.provider || null,
      operation: entry.operation || null,
      hotel_id: entry.hotel_id || null,
      org_key: entry.org_key || null,
      reserved_usd: usd,
      reserved_usd_status: usd_status,
      reserved_credits: credits,
      retry: Boolean(entry.retry),
      reserved_at: nowIso(),
    };
    operations.set(operationId, op);
    entries.push({
      at: nowIso(),
      ...op,
      ledger_event: "RESERVE",
    });
    return { ok: true, reservation_id: operationId, operation: op };
  }

  /**
   * Reconcile actual usage after provider execution.
   */
  function settle(operationId, actual = {}) {
    const id = String(operationId || "").trim();
    const op = operations.get(id);
    if (!op) {
      return { ok: false, error: "RESERVATION_NOT_FOUND", operation_id: id };
    }
    if (op.status === "SETTLED") {
      return { ok: true, already_settled: true, operation: op };
    }
    if (op.status === "RELEASED") {
      return { ok: false, error: "RESERVATION_RELEASED", operation_id: id };
    }

    const h = hotelBucket(op.hotel_id);
    h.reserved_credits = Math.max(0, h.reserved_credits - (op.reserved_credits || 0));
    h.reserved_usd = Math.max(
      0,
      Number(((h.reserved_usd || 0) - (op.reserved_usd || 0)).toFixed(6))
    );
    if (op.org_key) {
      const o = orgBucket(op.org_key);
      o.reserved_credits = Math.max(0, o.reserved_credits - (op.reserved_credits || 0));
      o.reserved_usd = Math.max(
        0,
        Number(((o.reserved_usd || 0) - (op.reserved_usd || 0)).toFixed(6))
      );
    }

    const { usd, usd_status } = normalizeUsd(actual.cost_usd, actual.cost_usd_status);
    const credits = Number(actual.credits ?? op.reserved_credits ?? 0) || 0;
    const status = actual.status === "FAILED" ? "FAILED" : "OK";

    h.calls += 1;
    h.credits += credits;
    if (status === "FAILED") h.failed += 1;
    if (status === "OK") addUsd(h, usd, usd_status);
    if (op.org_key) {
      const o = orgBucket(op.org_key);
      o.calls += 1;
      o.credits += credits;
      if (status === "OK") addUsd(o, usd, usd_status);
    }

    op.status = "SETTLED";
    op.settled_at = nowIso();
    op.actual_usd = usd;
    op.actual_usd_status = usd_status;
    op.actual_credits = credits;
    op.settle_status = status;
    op.result = actual.result || null;
    op.tokens = actual.tokens ?? null;

    entries.push({
      at: nowIso(),
      provider: op.provider,
      operation: op.operation,
      operation_id: id,
      hotel_id: op.hotel_id,
      org_key: op.org_key,
      status,
      cost_usd: usd,
      cost_usd_status: usd_status,
      credits,
      tokens: op.tokens,
      retry: op.retry,
      ledger_event: "SETTLE",
    });
    return { ok: true, operation: op };
  }

  /**
   * Release a reservation after failure (no spend).
   */
  function release(operationId, meta = {}) {
    const id = String(operationId || "").trim();
    const op = operations.get(id);
    if (!op) {
      return { ok: false, error: "RESERVATION_NOT_FOUND", operation_id: id };
    }
    if (op.status === "SETTLED") {
      return { ok: false, error: "ALREADY_SETTLED", operation_id: id };
    }
    if (op.status === "RELEASED") {
      return { ok: true, already_released: true, operation: op };
    }

    const h = hotelBucket(op.hotel_id);
    h.reserved_credits = Math.max(0, h.reserved_credits - (op.reserved_credits || 0));
    h.reserved_usd = Math.max(
      0,
      Number(((h.reserved_usd || 0) - (op.reserved_usd || 0)).toFixed(6))
    );
    h.failed += 1;
    h.calls += 1;
    if (op.org_key) {
      const o = orgBucket(op.org_key);
      o.reserved_credits = Math.max(0, o.reserved_credits - (op.reserved_credits || 0));
      o.reserved_usd = Math.max(
        0,
        Number(((o.reserved_usd || 0) - (op.reserved_usd || 0)).toFixed(6))
      );
      o.calls += 1;
    }

    op.status = "RELEASED";
    op.released_at = nowIso();
    op.release_reason = meta.reason || null;
    op.release_error = meta.error || null;

    entries.push({
      at: nowIso(),
      provider: op.provider,
      operation: op.operation,
      operation_id: id,
      hotel_id: op.hotel_id,
      org_key: op.org_key,
      status: "FAILED",
      cost_usd: null,
      cost_usd_status: "UNKNOWN",
      credits: 0,
      reason: meta.reason || "RELEASED",
      retry: op.retry,
      ledger_event: "RELEASE",
    });
    return { ok: true, operation: op };
  }

  function recordUsage(entry = {}) {
    const { usd, usd_status } = normalizeUsd(entry.cost_usd, entry.cost_usd_status);
    const credits = Number(entry.credits || 0) || 0;
    const gate = canSpend({
      hotelId: entry.hotel_id,
      orgKey: entry.org_key,
      costUsd: entry.cost_usd,
      credits,
    });
    if (!gate.ok && entry.status !== "REFUSED") {
      return gate;
    }
    const rec = {
      at: nowIso(),
      provider: entry.provider || null,
      operation: entry.operation || null,
      operation_id: entry.operation_id || null,
      hotel_id: entry.hotel_id || null,
      org_key: entry.org_key || null,
      status: entry.status || "OK",
      cost_usd: usd,
      cost_usd_status: usd_status,
      credits,
      tokens: entry.tokens ?? null,
      retry: Boolean(entry.retry),
      reason: entry.reason || null,
      ledger_event: "RECORD_USAGE",
    };
    entries.push(rec);
    const h = hotelBucket(rec.hotel_id);
    h.calls += 1;
    h.credits += credits;
    if (rec.status === "REFUSED") h.refused += 1;
    if (rec.status === "FAILED") h.failed += 1;
    if (rec.status === "OK") addUsd(h, usd, usd_status);
    if (rec.org_key) {
      const o = orgBucket(rec.org_key);
      o.calls += 1;
      o.credits += credits;
      if (rec.status === "OK") addUsd(o, usd, usd_status);
    }
    return { ok: true, entry: rec };
  }

  function snapshot() {
    return {
      version: BUDGET_LEDGER_VERSION,
      caps: {
        per_hotel_usd: perHotelUsdCap,
        per_org_usd: perOrgUsdCap,
        per_hotel_credits: perHotelCreditCap,
      },
      hotels: [...hotelTotals.values()],
      orgs: [...orgTotals.values()],
      entries: [...entries],
      operations: [...operations.values()],
    };
  }

  function persist(filePath) {
    ensureDir(path.dirname(filePath));
    fs.writeFileSync(filePath, `${JSON.stringify(snapshot(), null, 2)}\n`, "utf8");
    return filePath;
  }

  return {
    canSpend,
    reserve,
    settle,
    release,
    getOperation,
    recordUsage,
    snapshot,
    persist,
    version: BUDGET_LEDGER_VERSION,
  };
}
