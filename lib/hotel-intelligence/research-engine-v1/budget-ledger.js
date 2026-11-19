/**
 * Per-hotel / per-organization budget + provider-usage ledger.
 * Unknown USD must never be stored or reported as 0.
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

  function hotelBucket(hotelId) {
    const id = String(hotelId || "unknown");
    if (!hotelTotals.has(id)) {
      hotelTotals.set(id, {
        hotel_id: id,
        usd: null,
        usd_status: "UNKNOWN",
        credits: 0,
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
    if (perHotelCreditCap != null && h.credits + Number(credits || 0) > perHotelCreditCap) {
      return { ok: false, error: FAILURE_CLASS.BUDGET_EXHAUSTED, scope: "hotel_credits" };
    }
    if (usd != null && perHotelUsdCap != null && (h.usd || 0) + usd > perHotelUsdCap) {
      return { ok: false, error: FAILURE_CLASS.BUDGET_EXHAUSTED, scope: "hotel_usd" };
    }
    if (orgKey && usd != null && perOrgUsdCap != null) {
      const o = orgBucket(orgKey);
      if ((o.usd || 0) + usd > perOrgUsdCap) {
        return { ok: false, error: FAILURE_CLASS.BUDGET_EXHAUSTED, scope: "org_usd" };
      }
    }
    // If USD required for enforcement but unknown, refuse rather than treat as 0
    if (perHotelUsdCap != null && usd_status === "UNKNOWN" && costUsd !== 0 && costUsd != null) {
      return {
        ok: false,
        error: FAILURE_CLASS.BUDGET_EXHAUSTED,
        scope: "usd_unknown_under_cap",
        note: "unknown_cost_cannot_be_zeroed",
      };
    }
    return { ok: true, usd, usd_status };
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
      hotel_id: entry.hotel_id || null,
      org_key: entry.org_key || null,
      status: entry.status || "OK",
      cost_usd: usd,
      cost_usd_status: usd_status,
      credits,
      tokens: entry.tokens ?? null,
      retry: Boolean(entry.retry),
      reason: entry.reason || null,
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
    };
  }

  function persist(filePath) {
    ensureDir(path.dirname(filePath));
    fs.writeFileSync(filePath, `${JSON.stringify(snapshot(), null, 2)}\n`, "utf8");
    return filePath;
  }

  return {
    canSpend,
    recordUsage,
    snapshot,
    persist,
    version: BUDGET_LEDGER_VERSION,
  };
}
