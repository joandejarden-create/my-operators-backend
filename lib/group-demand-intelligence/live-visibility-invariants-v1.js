/**
 * GDI live visibility invariants V1 (post readiness convergence).
 *
 * NEW opportunities: visible ⇔ strict-ready (and surface-eligible).
 * Legacy: visible ⇔ strict-ready OR explicit temporary legacyVisibilityPreserved.
 */

import { filterCustomerFacingOpportunities } from "./customer-visibility.js";
import { filterSalespersonView } from "./opportunity-factory.js";
import { applyLiveCommercialQuality } from "./live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";
import { isCustomerSurfaceActiveEligible } from "./customer-surface-revalidation-v1.js";

export const CONTROL_API_VISIBLE_EXPECT = Object.freeze({
  BETHESDA: { hpc: "recLuxvwwxID7U2B8", expect: 37 },
  RENAISSANCE: { hpc: "recG66DQJKP2c0UNh", expect: 11 },
  WATERSTONE: { hpc: "recgMYovrrZDJMqzX", expect: 15 },
  HILTON: { hpc: "rec35fExUxCClpOP6", expect: 0 },
});

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

/**
 * Simulate getGdiOpportunities list filters (auth path, default view).
 */
export function simulateGdiListApiOpportunities(opportunities = [], opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  let ops = (opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate })
  );
  if (opts.includeDisqualified !== true) {
    ops = filterSalespersonView(ops);
  }
  ops = filterCustomerFacingOpportunities(ops, opts);
  return ops;
}

/**
 * Invariant: every surface-eligible + (ready OR legacy) opportunity must appear in API list.
 */
export function assertCustomerFacingReturnedByListApi(opportunities = [], opts = {}) {
  const api = simulateGdiListApiOpportunities(opportunities, opts);
  const apiIds = new Set(api.map(oppId).filter(Boolean));
  const violations = [];
  for (const o of opportunities || []) {
    if (!isCustomerSurfaceActiveEligible(o, opts)) continue;
    const ready = isGdiCustomerOpportunityReady(o, opts).ok;
    const legacy = isLegacyVisibilityCompatible(o);
    if (!ready && !legacy) continue;
    const id = oppId(o);
    if (!id) continue;
    if (opts.hotelId && o.hotelId && o.hotelId !== opts.hotelId) {
      violations.push({ id, reason: "HOTEL_BINDING_MISMATCH" });
      continue;
    }
    if (!apiIds.has(id)) {
      violations.push({ id, reason: "READY_OR_LEGACY_NOT_IN_API" });
    }
  }
  return {
    ok: violations.length === 0,
    apiCount: api.length,
    violations,
  };
}

/**
 * Invariant: API-returned count === UI data-model count before presentation filters.
 */
export function assertApiEqualsUiDataModel(apiOpportunities = [], uiDataModel = []) {
  const apiIds = new Set((apiOpportunities || []).map(oppId).filter(Boolean));
  const uiIds = new Set((uiDataModel || []).map(oppId).filter(Boolean));
  const onlyApi = [...apiIds].filter((id) => !uiIds.has(id));
  const onlyUi = [...uiIds].filter((id) => !apiIds.has(id));
  return {
    ok: onlyApi.length === 0 && onlyUi.length === 0 && apiIds.size === uiIds.size,
    apiCount: apiIds.size,
    uiCount: uiIds.size,
    onlyApi,
    onlyUi,
  };
}

export function checkControlApiVisibleCount(hotelKey, apiCount) {
  const cfg = CONTROL_API_VISIBLE_EXPECT[hotelKey];
  if (!cfg) return { ok: false, reason: "UNKNOWN_HOTEL_KEY" };
  return {
    ok: Number(apiCount) === cfg.expect,
    expect: cfg.expect,
    actual: Number(apiCount),
    hpc: cfg.hpc,
  };
}

/**
 * New opportunities must not use the legacy compatibility path.
 */
export function assertNewOpportunityCannotUseLegacy(opp = {}) {
  const stamped = isLegacyVisibilityCompatible(opp);
  return {
    ok: stamped === false,
    reason: stamped ? "NEW_OPP_HAS_LEGACY_STAMP" : null,
  };
}
