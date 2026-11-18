/**
 * Weekly Change durability V1 (CD-1).
 *
 * Durable state lives on:
 *   1) Filesystem hotel opportunities bag (research SoR for annotations)
 *   2) opportunityPayloadJson commercial keys
 *   3) Existing GDO typed provenance columns (firstDiscovered* / lastMaterialChange*)
 *
 * No Weekly Change history table. Description rewrite alone is not material.
 */

import {
  MATERIAL_FIELDS,
  WEEKLY_DELTA_STATE,
  WEEKLY_DELTA_VERSION,
  classifyWeeklyDelta,
  deriveIsNewThisWeek,
  snapshotBaselineOpportunity,
} from "../weekly-delta.js";

export const GDI_WEEKLY_CHANGE_DURABILITY_V1 = "gdi-weekly-change-durability-v1";

export const WEEKLY_CHANGE_DURABILITY_CONTRACT = Object.freeze({
  version: GDI_WEEKLY_CHANGE_DURABILITY_V1,
  weeklyDeltaEngine: WEEKLY_DELTA_VERSION,
  storage: Object.freeze([
    "filesystem.hotel.opportunities",
    "airtable.gdo.opportunityPayloadJson",
    "airtable.gdo.firstDiscoveredAt",
    "airtable.gdo.firstDiscoveredRunId",
    "airtable.gdo.lastMaterialChangeAt",
    "airtable.gdo.lastMaterialChangeRunId",
  ]),
  materialFields: MATERIAL_FIELDS,
  nonMaterialExplicitly: Object.freeze([
    "description",
    "summaryWhat",
    "whatIsHappening",
    "notes",
    "titleRewriteOnly",
  ]),
  states: WEEKLY_DELTA_STATE,
});

/**
 * Build durable Weekly Change annotation for an opportunity.
 * Safe across process restarts when persisted to FS + payload + typed columns.
 */
export function buildDurableWeeklyChangeAnnotation({
  baselineOpportunity = null,
  currentOpportunity = {},
  runId,
  nowIso = new Date().toISOString(),
  latestCompletedWeeklyRunId = null,
} = {}) {
  const baselineRows = baselineOpportunity
    ? [snapshotBaselineOpportunity(baselineOpportunity)]
    : [];
  const classified = classifyWeeklyDelta(baselineRows, currentOpportunity, {
    runId,
    nowIso,
  });

  const firstSeenRunId =
    classified.firstSeenRunId ||
    currentOpportunity.firstSeenRunId ||
    (classified.weeklyDeltaState === WEEKLY_DELTA_STATE.NEW ? runId : null);
  const firstSeenAt =
    classified.firstSeenAt ||
    currentOpportunity.firstSeenAt ||
    currentOpportunity.firstDiscoveredAt ||
    (classified.weeklyDeltaState === WEEKLY_DELTA_STATE.NEW ? nowIso : null);

  const annotation = {
    weeklyDeltaState: classified.weeklyDeltaState,
    isNewThisWeek: deriveIsNewThisWeek(
      { firstSeenRunId },
      latestCompletedWeeklyRunId || runId,
    ),
    firstSeenRunId,
    firstSeenAt,
    firstDiscoveredAt: firstSeenAt,
    firstDiscoveredRunId: firstSeenRunId,
    lastMaterialChangeAt: classified.lastMaterialChangeAt || null,
    lastMaterialChangeRunId: classified.lastMaterialChangeRunId || null,
    changedFields: classified.changedFields || [],
    durabilityContract: GDI_WEEKLY_CHANGE_DURABILITY_V1,
    durabilityStorage: WEEKLY_CHANGE_DURABILITY_CONTRACT.storage,
  };

  return annotation;
}

/**
 * Prove description-only rewrite does not create NEW / UPDATED.
 */
export function assertDescriptionRewriteIsNonMaterial({
  prior = {},
  rewrittenDescription,
  runId = "durability_test_run",
} = {}) {
  const current = {
    ...prior,
    description: rewrittenDescription,
    summaryWhat: rewrittenDescription,
    whatIsHappening: rewrittenDescription,
    notes: rewrittenDescription,
  };
  const annotation = buildDurableWeeklyChangeAnnotation({
    baselineOpportunity: prior,
    currentOpportunity: current,
    runId,
    latestCompletedWeeklyRunId: runId,
  });
  return {
    ok:
      annotation.weeklyDeltaState === WEEKLY_DELTA_STATE.UNCHANGED &&
      annotation.isNewThisWeek === false,
    weeklyDeltaState: annotation.weeklyDeltaState,
    changedFields: annotation.changedFields,
  };
}

/**
 * Map durable annotation onto typed Airtable provenance columns + payload keys.
 */
export function weeklyChangeAnnotationToAirtableFields(annotation = {}) {
  return {
    firstDiscoveredAt: annotation.firstDiscoveredAt || null,
    firstDiscoveredRunId: annotation.firstDiscoveredRunId || null,
    lastMaterialChangeAt: annotation.lastMaterialChangeAt || null,
    lastMaterialChangeRunId: annotation.lastMaterialChangeRunId || null,
  };
}

export function weeklyChangeAnnotationToPayloadKeys(annotation = {}) {
  return {
    weeklyDeltaState: annotation.weeklyDeltaState || null,
    isNewThisWeek: annotation.isNewThisWeek === true,
    firstSeenRunId: annotation.firstSeenRunId || null,
    firstSeenAt: annotation.firstSeenAt || null,
    firstDiscoveredAt: annotation.firstDiscoveredAt || null,
    firstDiscoveredRunId: annotation.firstDiscoveredRunId || null,
    lastMaterialChangeAt: annotation.lastMaterialChangeAt || null,
    lastMaterialChangeRunId: annotation.lastMaterialChangeRunId || null,
  };
}
