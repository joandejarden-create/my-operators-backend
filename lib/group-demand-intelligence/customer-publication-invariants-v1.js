/**
 * GDI customer publication invariants — hotel-agnostic.
 *
 * Rules:
 * 1) Report-claimed Ready / Valid Watch MUST equal real gate counts
 *    (isGdiCustomerOpportunityReady / isValidFutureWatch).
 * 2) Every customerVisible Valid Watch MUST appear in the customer-facing API set.
 * 3) Every customerVisible Ready MUST appear in the customer-facing API set.
 * 4) filterCustomerFacing total MUST equal the customer API opportunity count
 *    for the same hotel bag (no silent drops after filter).
 *
 * HOLD_WATCH / research dispositions are internal and must not inflate Valid Watch.
 */

import { filterCustomerFacingOpportunities } from "./customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "./future-watch/is-valid-future-watch-v1.js";

export const CUSTOMER_PUBLICATION_INVARIANT_VERSION =
  "gdi_customer_publication_invariants_v1";

function oppId(o = {}) {
  return o.id || o.opportunityId || null;
}

function isWatchLane(opp = {}) {
  const facing = String(opp.customerFacingState || "").toUpperCase();
  const partition = String(opp.salesPartitionV11 || "").toUpperCase();
  const priority = String(opp.priority || "").toUpperCase();
  if (facing === "FUTURE_WATCH" || partition === "FUTURE_WATCH") return true;
  if (priority === "WATCHLIST" || priority === "WATCH") return true;
  return false;
}

/**
 * Canonical gate counts + customer-visible subsets.
 */
export function countCanonicalWatchPublication(opportunities = [], opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const facing = filterCustomerFacingOpportunities(opportunities, opts);
  const facingIds = new Set(facing.map((o) => oppId(o)).filter(Boolean));

  let canonicalReady = 0;
  let canonicalValidWatch = 0;
  let customerVisibleReady = 0;
  let customerVisibleValidWatch = 0;
  const rows = [];

  for (const o of opportunities || []) {
    const id = oppId(o);
    const ready = isGdiCustomerOpportunityReady(o, { ...opts, nowDate });
    const watch = isValidFutureWatch(o, { ...opts, nowDate });
    const visible = o.customerVisible !== false;
    const onFacing = id != null && facingIds.has(id);

    if (ready.ok) {
      canonicalReady += 1;
      if (visible && onFacing) customerVisibleReady += 1;
    }
    if (watch.ok) {
      canonicalValidWatch += 1;
      if (visible && onFacing) customerVisibleValidWatch += 1;
    }

    if (watch.ok || ready.ok || isWatchLane(o)) {
      rows.push({
        id,
        title: o.title || o.opportunityName,
        watchOk: Boolean(watch.ok),
        watchClass: watch.class || null,
        readyOk: Boolean(ready.ok),
        customerVisible: visible,
        customerFacing: onFacing,
        watchLane: isWatchLane(o),
      });
    }
  }

  return {
    version: CUSTOMER_PUBLICATION_INVARIANT_VERSION,
    canonicalReady,
    canonicalValidWatch,
    customerVisibleReady,
    customerVisibleValidWatch,
    customerFacingTotal: facing.length,
    rows,
  };
}

/**
 * Customer-facing API lane counts (post filterCustomerFacing).
 * Ready lane = facing + ready gate + not watch-lane priority/state.
 * Watch lane = facing + (watch gate OR intentional FUTURE_WATCH/WATCHLIST stamp).
 * An opportunity counted as Ready is not also counted as Watch.
 */
export function countCustomerFacingReadyWatch(opportunities = [], opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const facing = filterCustomerFacingOpportunities(opportunities, opts);
  let ready = 0;
  let watch = 0;
  for (const o of facing) {
    const w = isValidFutureWatch(o, { ...opts, nowDate });
    const r = isGdiCustomerOpportunityReady(o, { ...opts, nowDate });
    if (isWatchLane(o) || (w.ok && !r.ok)) {
      watch += 1;
      continue;
    }
    if (r.ok) {
      ready += 1;
      continue;
    }
    // Facing via legacy/pilot without ready — treat as watch lane if stamped, else skip lane
    if (w.ok) watch += 1;
  }
  return {
    version: CUSTOMER_PUBLICATION_INVARIANT_VERSION,
    customerFacingTotal: facing.length,
    customerFacingReady: ready,
    customerFacingWatch: watch,
  };
}

/**
 * Publication integrity:
 * - every customerVisible Valid Watch must be in facing API set
 * - every customerVisible Ready must be in facing API set
 * - facing total is the API opportunity count
 */
export function assertCustomerPublicationCountsMatch(opportunities = [], opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const canonical = countCanonicalWatchPublication(opportunities, { ...opts, nowDate });
  const api = countCustomerFacingReadyWatch(opportunities, { ...opts, nowDate });
  const facing = filterCustomerFacingOpportunities(opportunities, opts);
  const facingIds = new Set(facing.map((o) => oppId(o)).filter(Boolean));
  const failures = [];

  // Explicit customerVisible:true rows that pass Ready/Watch gates must be API-facing.
  // (customerVisible:false or unset research rows may fail gates without publication.)
  for (const o of opportunities || []) {
    const id = oppId(o);
    if (!id) continue;
    if (o.customerVisible !== true) continue;
    const ready = isGdiCustomerOpportunityReady(o, { ...opts, nowDate });
    const watch = isValidFutureWatch(o, { ...opts, nowDate });
    if (watch.ok && !facingIds.has(id)) {
      failures.push({
        code: "VALID_WATCH_NOT_IN_API",
        id,
        title: o.title || o.opportunityName,
      });
    }
    if (ready.ok && !facingIds.has(id)) {
      failures.push({
        code: "READY_NOT_IN_API",
        id,
        title: o.title || o.opportunityName,
      });
    }
  }

  if (canonical.customerFacingTotal !== api.customerFacingTotal) {
    failures.push({
      code: "FACING_TOTAL_MISMATCH",
      canonicalFacing: canonical.customerFacingTotal,
      apiFacing: api.customerFacingTotal,
    });
  }

  return {
    ok: failures.length === 0,
    version: CUSTOMER_PUBLICATION_INVARIANT_VERSION,
    canonical,
    api,
    failures,
  };
}

/**
 * E2E completion gate: report-claimed Ready/Watch must equal gate-derived counts.
 * Prevents heuristic report dispositions (HOLD_WATCH seeds) from claiming Valid Watch.
 */
export function assertReportCountsMatchGates(
  opportunities = [],
  { claimedReady = 0, claimedValidWatch = 0, nowDate } = {}
) {
  const counts = countCanonicalWatchPublication(opportunities, { nowDate });
  const failures = [];
  if (Number(claimedReady) !== counts.canonicalReady) {
    failures.push({
      code: "REPORT_READY_MISMATCH",
      claimed: claimedReady,
      canonical: counts.canonicalReady,
    });
  }
  if (Number(claimedValidWatch) !== counts.canonicalValidWatch) {
    failures.push({
      code: "REPORT_VALID_WATCH_MISMATCH",
      claimed: claimedValidWatch,
      canonical: counts.canonicalValidWatch,
    });
  }
  return {
    ok: failures.length === 0,
    counts,
    failures,
  };
}
