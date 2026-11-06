/**
 * ADP_CURRENT_HISTORICAL_VERSION_V1
 *
 * Exactly one designated current certified measurement per hotel per reporting scope.
 * Historical rows remain queryable; older results must not silently override current.
 *
 * Demotion is planned as PATCH ops (Is Current Historical Version → false) on prior
 * Monitoring Periods + Report Snapshots rows for the same property+scope when a new
 * current version is written. Live apply of demotion requires MEASUREMENT_HISTORY auth.
 */

import { HISTORY_TABLES } from "./airtable-history-schema-final-v1.js";
import { CURRENT_VERSION_RULE } from "./adp-e2e-persistence-table-contract-v1.js";

export const ADP_CURRENT_HISTORICAL_VERSION_V1 = "ADP_CURRENT_HISTORICAL_VERSION_V1";

export const REPORTING_SCOPE = Object.freeze({
  CORE_INDEPENDENT_UNPROMPTED_V1_1_1: "CORE_INDEPENDENT_UNPROMPTED_V1_1_1",
});

/**
 * Build demotion plan for prior "current" rows when installing a new current period.
 * Does not mutate Airtable. Mock store can apply via applyDemotionPlanToMockStore.
 */
export function buildCurrentVersionDemotionPlan({
  propertyId,
  newPeriodId,
  newHistoricalVersion = 1,
  newPeriodKey = null,
  newSnapshotId = null,
  existingMonitoringPeriodRows = [],
  existingReportSnapshotRows = [],
  reportingScope = REPORTING_SCOPE.CORE_INDEPENDENT_UNPROMPTED_V1_1_1,
} = {}) {
  const defects = [];
  if (!propertyId || !newPeriodId) {
    defects.push({ code: "MISSING_PROPERTY_OR_PERIOD" });
  }

  const keepPeriodKey =
    newPeriodKey || `${propertyId}|${newPeriodId}|v${newHistoricalVersion}`;

  const demote = [];
  for (const row of existingMonitoringPeriodRows) {
    const fields = row.fields || row;
    if (String(fields["Property ID"] || "") !== propertyId) continue;
    if (fields["Is Current Historical Version"] !== true && fields["Is Current Historical Version"] !== "true") {
      continue;
    }
    const key = fields["Period Key"] || row.idempotencyKey;
    // Keep only the exact new Period Key (same period + new historical version demotes priors)
    if (key && String(key) === keepPeriodKey) continue;
    demote.push({
      table: HISTORY_TABLES.MONITORING_PERIODS,
      recordId: row.id || null,
      idempotencyKey: key,
      action: "DEMOTE_CURRENT_FLAG",
      patch: { "Is Current Historical Version": false },
      priorPeriodId: fields["Period ID"],
    });
  }

  for (const row of existingReportSnapshotRows) {
    const fields = row.fields || row;
    if (String(fields["Property ID"] || "") !== propertyId) continue;
    if (fields["Is Current Historical Version"] !== true && fields["Is Current Historical Version"] !== "true") {
      continue;
    }
    const snapId = fields["Snapshot ID"] || row.idempotencyKey;
    if (newSnapshotId && String(snapId) === String(newSnapshotId)) continue;
    // If no newSnapshotId provided, demote other periods only (legacy behavior)
    if (!newSnapshotId && String(fields["Period ID"] || "") === newPeriodId) continue;
    demote.push({
      table: HISTORY_TABLES.REPORT_SNAPSHOTS,
      recordId: row.id || null,
      idempotencyKey: snapId,
      action: "DEMOTE_CURRENT_FLAG",
      patch: { "Is Current Historical Version": false },
      priorPeriodId: fields["Period ID"],
    });
  }

  return {
    contract: ADP_CURRENT_HISTORICAL_VERSION_V1,
    rule: CURRENT_VERSION_RULE.id,
    reportingScope,
    propertyId,
    newPeriodId,
    newHistoricalVersion,
    demoteCount: demote.length,
    demote,
    defects,
    ok: defects.length === 0,
    note: "Demotion patches must be authorized; never delete historical rows.",
  };
}

/**
 * Apply demotion plan against MockAdpHistoryStore (local only).
 */
export function applyDemotionPlanToMockStore(store, demotionPlan) {
  if (!store?.state?.records) {
    return { ok: false, demoted: 0, error: "NO_MOCK_STORE" };
  }
  let demoted = 0;
  for (const op of demotionPlan?.demote || []) {
    const key = op.idempotencyKey;
    const rec = key ? store.state.records[key] : null;
    if (!rec) {
      // Also search by matching fields
      const hit = Object.values(store.state.records).find(
        (r) =>
          r.table === op.table &&
          r.fields?.["Property ID"] &&
          String(r.fields["Period ID"]) === String(op.priorPeriodId) &&
          (r.fields["Is Current Historical Version"] === true ||
            r.fields["Is Current Historical Version"] === "true")
      );
      if (!hit) continue;
      hit.fields = { ...hit.fields, ...op.patch };
      hit.updatedAt = new Date().toISOString();
      demoted += 1;
      continue;
    }
    rec.fields = { ...rec.fields, ...op.patch };
    rec.updatedAt = new Date().toISOString();
    demoted += 1;
  }
  store._save?.();
  return { ok: true, demoted };
}

/**
 * Assert at most one current Monitoring Periods row per property (mock or list).
 */
export function assertSingleCurrentPerProperty(monitoringPeriodRows, propertyId) {
  const currents = (monitoringPeriodRows || []).filter((r) => {
    const f = r.fields || r;
    return (
      String(f["Property ID"] || "") === propertyId &&
      (f["Is Current Historical Version"] === true ||
        f["Is Current Historical Version"] === "true")
    );
  });
  return {
    ok: currents.length <= 1,
    currentCount: currents.length,
    periodIds: currents.map((r) => (r.fields || r)["Period ID"]),
    defect:
      currents.length > 1
        ? {
            code: "MULTIPLE_CURRENT_HISTORICAL_VERSIONS",
            detail: `${currents.length} current rows for ${propertyId}`,
          }
        : null,
  };
}
