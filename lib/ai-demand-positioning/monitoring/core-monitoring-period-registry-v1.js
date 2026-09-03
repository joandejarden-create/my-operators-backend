/**
 * Core ADP monitoring period registry — calendar weeks, prior certified periods, markers.
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../contracts/adp-certified-property-cohort-v1.js";
import { MEASUREMENT_CONTRACT_V1_1 } from "../measurement-assurance/adp-measurement-contract-v1-1-candidate.js";

export const CORE_MONITORING_REGISTRY_VERSION = "core_monitoring_period_registry_v1";

export const ADP_WEEK_2026_09_03 = "adp_week_2026-09-03";
export const CORE_MONITORING_DATE = "2026-09-03";

export const CORE_PERIOD_SEQUENCE_002 = 2;
export const CORE_PERIOD_MARKER_002 = "ADP_OFFICIAL_MONITORING_PERIOD_002";

export const CORE_COST_CAP_USD = 50;
export const CORE_RECOVERY_RESERVE_USD = 1.5;

const BASELINE_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/certification-baseline/ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json"
);

export function loadCertificationBaselineRegistry() {
  if (!existsSync(BASELINE_PATH)) {
    throw new Error(`Certification baseline missing: ${BASELINE_PATH}`);
  }
  return JSON.parse(readFileSync(BASELINE_PATH, "utf8"));
}

/**
 * Prior certified comparable period IDs by property (immutable Period-001).
 * Never treat the current trusted periodId as its own prior after a baseline revision.
 */
export function loadPriorCertifiedCorePeriodIds() {
  const baseline = loadCertificationBaselineRegistry();
  const map = {};
  for (const row of baseline.properties || []) {
    if (!row.propertyId) continue;
    const prior = row.priorPeriodId || row.priorComparablePeriodId || null;
    if (!prior) continue;
    if (row.periodId && prior === row.periodId) {
      throw new Error(
        `BASELINE_REVISION_PRESERVES_PRIOR_RUN_HISTORY: ${row.propertyId} prior equals current ${row.periodId}`
      );
    }
    map[row.propertyId] = prior;
  }
  return map;
}

export function resolvePriorCertifiedCorePeriodId(propertyId) {
  const map = loadPriorCertifiedCorePeriodIds();
  return map[propertyId] || null;
}

export function buildCoreMonitoringCycleIdentity() {
  return {
    calendarWeekId: ADP_WEEK_2026_09_03,
    monitoringDate: CORE_MONITORING_DATE,
    measurementFamily: "CORE",
    periodSequence: CORE_PERIOD_SEQUENCE_002,
    periodMarker: CORE_PERIOD_MARKER_002,
    measurementContractVersion: MEASUREMENT_CONTRACT_V1_1,
    propertyIds: [...ADP_CERTIFIED_PROPERTY_IDS],
    priorPeriodIds: loadPriorCertifiedCorePeriodIds(),
  };
}

/** BAI longitudinal prior — not the federated demo baseline. */
export const BAI_PRIOR_LONGITUDINAL_PERIOD_ID = "aiv_brand_longitudinal_period_20260818_6579d2";

export const BPP_CURRENT_PERIOD_ID = "bpp_second_cycle_2026-09-02T1947";
