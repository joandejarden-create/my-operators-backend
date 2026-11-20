/**
 * HE 5.0 shared $50 pilot budget ledger.
 * Extends M1 shared-budget pattern: reserve → settle | release.
 * Unknown costs cannot be zeroed; concurrent reserves cannot oversubscribe.
 */

import fs from "node:fs";
import path from "node:path";
import { createM1SharedBudgetLedger } from "../research-engine-v1/benchmark-p3b-m1/shared-budget-ledger.js";
import {
  HE50_GLOBAL_USD_CAP,
  HE50_PILOT_RUN_ID,
  HE50_PILOT_VERSION,
  HE50_CATEGORY_CEILINGS,
} from "./contract.js";

export const HE50_BUDGET_LEDGER_VERSION = "he-5-0-pilot-budget-ledger-v1";

/**
 * @param {object} [opts]
 * @param {number} [opts.globalUsdCap]
 * @param {string} [opts.durablePath] — optional JSON persistence path
 */
export function createHe50PilotBudgetLedger(opts = {}) {
  const globalUsdCap = Number(opts.globalUsdCap ?? HE50_GLOBAL_USD_CAP);
  // M1 ledger uses Number(perHotelUsdCap); null→0 would block all spend. Use global cap as
  // per-hotel ceiling so the shared $50 pool is the real constraint (not per-hotel silos).
  const perHotelUsdCap = Number(
    opts.perHotelUsdCap != null ? opts.perHotelUsdCap : globalUsdCap
  );
  const shared = createM1SharedBudgetLedger({
    runUsdCap: globalUsdCap,
    perHotelUsdCap,
  });

  const entries = [];
  const byCategory = new Map();
  const byProvider = new Map();
  const byHotel = new Map();
  let chain = Promise.resolve();

  function bump(map, key, usd) {
    const k = String(key || "unknown");
    const prior = map.get(k) || { key: k, confirmedUsd: 0, reservedUsd: 0, calls: 0 };
    if (usd.kind === "reserve") prior.reservedUsd = Number((prior.reservedUsd + usd.amount).toFixed(6));
    if (usd.kind === "settle") {
      prior.confirmedUsd = Number((prior.confirmedUsd + usd.amount).toFixed(6));
      prior.reservedUsd = Number(Math.max(0, prior.reservedUsd - (usd.released || usd.amount)).toFixed(6));
    }
    if (usd.kind === "release") {
      prior.reservedUsd = Number(Math.max(0, prior.reservedUsd - usd.amount).toFixed(6));
    }
    if (usd.kind === "call") prior.calls += 1;
    map.set(k, prior);
  }

  function syncReserve(entry = {}) {
    const cost = Number(entry.cost_usd ?? entry.estimated_max_usd);
    if (!Number.isFinite(cost) || cost < 0) {
      return {
        ok: false,
        error: "BUDGET_EXHAUSTED",
        scope: "usd_unknown_under_cap",
        note: "unknown_or_unbounded_cost_refused",
      };
    }
    const category = entry.research_category || entry.category || "unclassified";
    const catCeil = HE50_CATEGORY_CEILINGS[category];
    if (catCeil != null) {
      const cat = byCategory.get(category) || { reservedUsd: 0, confirmedUsd: 0 };
      if (cat.reservedUsd + cat.confirmedUsd + cost > catCeil + 1e-9) {
        // Soft category ceiling: allow reallocation only when explicitly opted in
        if (!entry.allowCategoryReallocation) {
          return {
            ok: false,
            error: "BUDGET_EXHAUSTED",
            scope: "category_ceiling",
            category,
            categoryCeiling: catCeil,
            attempted: cost,
            note: "set allowCategoryReallocation=true to use unused global capacity",
          };
        }
      }
    }

    const reserved = shared.reserve({
      ...entry,
      operation_id: entry.operation_id || entry.run_request_id,
      cost_usd: cost,
      cost_usd_status: "ESTIMATED",
      hotel_id: entry.hotel_id || entry.pilot_key,
      provider: entry.provider,
    });

    if (reserved.ok && !reserved.already_settled && !reserved.already_reserved) {
      const row = {
        run_id: opts.runId || HE50_PILOT_RUN_ID,
        operation_id: entry.operation_id || entry.run_request_id,
        hotel_id: entry.hotel_id || entry.pilot_key || null,
        research_category: category,
        provider: entry.provider || null,
        request_type: entry.request_type || null,
        timestamp: new Date().toISOString(),
        estimated_max_usd: cost,
        actual_usd: null,
        status: "RESERVED",
        fields_researched: entry.fields_researched || [],
        verified_findings: [],
        evidence_refs: entry.evidence_refs || [],
        result_class: null,
      };
      entries.push(row);
      bump(byCategory, category, { kind: "reserve", amount: cost });
      bump(byProvider, entry.provider, { kind: "reserve", amount: cost });
      bump(byHotel, entry.hotel_id || entry.pilot_key, { kind: "reserve", amount: cost });
      bump(byProvider, entry.provider, { kind: "call", amount: 0 });
      persist();
    }
    return reserved;
  }

  function reserve(entry = {}) {
    return syncReserve(entry);
  }

  async function reserveAsync(entry = {}) {
    const run = chain.then(() => syncReserve(entry));
    chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  function settle(operationId, actual = {}) {
    const result = shared.settle(operationId, actual);
    if (!result.ok) return result;
    const actualUsd =
      actual.provider_cost_usd != null
        ? Number(actual.provider_cost_usd)
        : actual.cost_usd != null
          ? Number(actual.cost_usd)
          : null;
    const row = entries.find((e) => e.operation_id === operationId && e.status === "RESERVED");
    if (row) {
      row.actual_usd = Number.isFinite(actualUsd) ? actualUsd : null;
      row.status = "SETTLED";
      row.verified_findings = actual.verified_findings || [];
      row.result_class = actual.result_class || "SETTLED";
      row.settled_at = new Date().toISOString();
      const reservedAmt = row.estimated_max_usd || 0;
      const confirmed = Number.isFinite(actualUsd) ? actualUsd : reservedAmt;
      bump(byCategory, row.research_category, {
        kind: "settle",
        amount: confirmed,
        released: reservedAmt,
      });
      bump(byProvider, row.provider, { kind: "settle", amount: confirmed, released: reservedAmt });
      bump(byHotel, row.hotel_id, { kind: "settle", amount: confirmed, released: reservedAmt });
    }
    persist();
    return result;
  }

  function release(operationId, meta = {}) {
    const row = entries.find((e) => e.operation_id === operationId && e.status === "RESERVED");
    const result = shared.release(operationId, meta);
    if (result.ok && row) {
      bump(byCategory, row.research_category, { kind: "release", amount: row.estimated_max_usd });
      bump(byProvider, row.provider, { kind: "release", amount: row.estimated_max_usd });
      bump(byHotel, row.hotel_id, { kind: "release", amount: row.estimated_max_usd });
      row.status = "RELEASED";
      row.result_class = meta.result_class || "RELEASED";
    }
    persist();
    return result;
  }

  /** Free (cost=0) research logging — does not touch paid reservations. */
  function recordFreePass(meta = {}) {
    entries.push({
      run_id: opts.runId || HE50_PILOT_RUN_ID,
      operation_id: meta.operation_id || `free_${Date.now()}`,
      hotel_id: meta.hotel_id || meta.pilot_key || null,
      research_category: meta.research_category || "free_pass",
      provider: meta.provider || "public_web_free",
      request_type: meta.request_type || "free_source",
      timestamp: new Date().toISOString(),
      estimated_max_usd: 0,
      actual_usd: 0,
      status: "FREE",
      fields_researched: meta.fields_researched || [],
      verified_findings: meta.verified_findings || [],
      evidence_refs: meta.evidence_refs || [],
      result_class: meta.result_class || "FREE_PASS",
    });
    persist();
  }

  function persist() {
    if (!opts.durablePath) return;
    const dir = path.dirname(opts.durablePath);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(opts.durablePath, JSON.stringify(snapshot(), null, 2), "utf8");
  }

  function snapshot() {
    const parent = shared.snapshot();
    // Prefer entry-derived accounting: M1 runReservedUsd can still hold settled
    // portions after partial settle, which would double-count if added to confirmed.
    let confirmed = 0;
    let openReserved = 0;
    for (const e of entries) {
      if (e.status === "SETTLED" && Number.isFinite(Number(e.actual_usd))) {
        confirmed = Number((confirmed + Number(e.actual_usd)).toFixed(6));
      } else if (e.status === "RESERVED" && Number.isFinite(Number(e.estimated_max_usd))) {
        openReserved = Number((openReserved + Number(e.estimated_max_usd)).toFixed(6));
      }
    }
    const committed = Number((confirmed + openReserved).toFixed(6));
    const remaining = Number(Math.max(0, globalUsdCap - committed).toFixed(6));
    const verifiedUseful = entries.reduce(
      (n, e) => n + (Array.isArray(e.verified_findings) ? e.verified_findings.length : 0),
      0
    );
    return {
      version: HE50_BUDGET_LEDGER_VERSION,
      pilotVersion: HE50_PILOT_VERSION,
      runId: opts.runId || HE50_PILOT_RUN_ID,
      globalUsdCap,
      categoryCeilings: { ...HE50_CATEGORY_CEILINGS },
      confirmedActualUsd: confirmed,
      reservedUnreconciledUsd: openReserved,
      totalCommittedUsd: committed,
      remainingAuthorizedUsd: remaining,
      byProvider: [...byProvider.values()],
      byHotel: [...byHotel.values()],
      byCategory: [...byCategory.values()],
      verifiedFindingsAttributed: verifiedUseful,
      costPerVerifiedUsefulField:
        verifiedUseful > 0 ? Number((confirmed / verifiedUseful).toFixed(6)) : null,
      entries: [...entries],
      sharedSnapshot: parent,
      note: "Never report reserved estimates as confirmed actual charges.",
    };
  }

  return {
    reserve,
    reserveAsync,
    settle,
    release,
    recordFreePass,
    snapshot,
    getOperation: (...a) => shared.getOperation(...a),
    canSpend: (...a) => shared.canSpend?.(...a),
  };
}
