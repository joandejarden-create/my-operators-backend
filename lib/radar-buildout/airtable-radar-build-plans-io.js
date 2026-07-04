/**
 * Airtable read/write for CALA Radar Build Plans.
 */

import {
  RADAR_BUILD_PLANS_FIELDS as F,
  RADAR_BUILD_PLANS_TABLE,
} from "./airtable-radar-build-plans-fields.js";
import {
  getRadarBuildPlansAirtableConfig,
  resolveRadarBuildPlansTableName,
} from "./radar-build-plans-base.js";
import { normalizeRadarBuildPlan } from "./normalize-radar-build-plan.js";
import { fetchAirtableTableFieldNameSet } from "../third-party-operator-basics-airtable-column-aliases.js";

const isDev = process.env.NODE_ENV !== "production";

export const RADAR_BUILD_PLANS_SELECT_FIELDS = Object.values(F);

function escFormula(s) {
  return String(s).replace(/'/g, "\\'");
}

/**
 * @param {object} [query]
 */
export async function fetchRadarBuildPlans(query = {}) {
  const cfg = getRadarBuildPlansAirtableConfig();
  if (!cfg) return { error: "airtable_config_missing" };

  const tableName = await resolveRadarBuildPlansTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);
  const fields = schema
    ? RADAR_BUILD_PLANS_SELECT_FIELDS.filter((name) => schema.has(name))
    : RADAR_BUILD_PLANS_SELECT_FIELDS;

  const conditions = [];
  if (query.country) conditions.push(`{${F.country}} = '${escFormula(query.country)}'`);
  if (query.region) conditions.push(`{${F.region}} = '${escFormula(query.region)}'`);
  if (query.priorityTier) conditions.push(`{${F.priorityTier}} = '${escFormula(query.priorityTier)}'`);
  if (query.buildStatus) conditions.push(`{${F.buildStatus}} = '${escFormula(query.buildStatus)}'`);
  if (query.buildStrategy) conditions.push(`{${F.buildStrategy}} = '${escFormula(query.buildStrategy)}'`);

  const selectOptions = { fields: fields.length ? fields : [F.country] };
  if (conditions.length) selectOptions.filterByFormula = `AND(${conditions.join(", ")})`;

  try {
    const records = await cfg.base(tableName).select(selectOptions).all();
    const plans = records.map((r) => normalizeRadarBuildPlan(r));
    return { tableName, plans, records };
  } catch (err) {
    if (isDev) console.error("[radar-build-plans] fetch error", err?.message || err);
    if (/Could not find table|NOT_FOUND/i.test(String(err?.message))) {
      return { error: "radar_build_plans_table_missing", tableName };
    }
    if (/not authorized|INVALID_PERMISSIONS|AUTHENTICATION/i.test(String(err?.message))) {
      return { error: "radar_build_plans_fetch_failed", tableName, message: err?.message };
    }
    throw err;
  }
}

/**
 * Build Airtable fields from generated plan payload.
 * @param {object} payload
 */
export function buildRadarBuildPlanAirtableFields(payload) {
  const submarkets = payload.submarkets || [];
  return {
    [F.country]: payload.country,
    [F.region]: payload.region,
    [F.buildStrategy]: payload.buildStrategy,
    [F.priorityTier]: payload.priorityTier,
    [F.buildStatus]: payload.buildStatus,
    [F.targetDemandAnchors]: payload.targets?.demandAnchors,
    [F.targetTravelInfrastructure]: payload.targets?.travelInfrastructure,
    [F.targetTotalRadarPoints]: payload.targets?.totalRadarPoints,
    [F.currentDemandAnchors]: payload.current?.demandAnchors,
    [F.currentTravelInfrastructure]: payload.current?.travelInfrastructure,
    [F.currentTotalRadarPoints]: payload.current?.totalRadarPoints,
    [F.submarketsCorridors]: submarkets.join("\n"),
    [F.primaryHotelDemandProfile]: payload.primaryHotelDemandProfile,
    [F.sourceCoveragePct]: payload.coverage?.sourceCoveragePct,
    [F.coordinateCoveragePct]: payload.coverage?.coordinateCoveragePct,
    [F.dataConfidenceMix]: JSON.stringify(payload.coverage?.dataConfidenceMix || {}),
    [F.lastBuildDate]: payload.lastBuildDate || new Date().toISOString().slice(0, 10),
    [F.lastQaDate]: payload.lastQaDate || "",
    [F.nextRecommendedAction]: payload.nextRecommendedAction,
    ...(payload.notes != null && payload.notes !== "" ? { [F.notes]: payload.notes } : {}),
    ...(payload.recommendedBuildSequence != null
      ? { [F.recommendedBuildSequence]: payload.recommendedBuildSequence }
      : {}),
    ...(payload.nextBuildMarket ? { [F.nextBuildMarket]: payload.nextBuildMarket } : {}),
    ...(payload.buildApproachNotes ? { [F.buildApproachNotes]: payload.buildApproachNotes } : {}),
    ...(payload.firstPassTargetDescription
      ? { [F.firstPassTargetDescription]: payload.firstPassTargetDescription }
      : {}),
  };
}

/**
 * @param {object} payload — generated plan
 * @param {{ force?: boolean, existingRecord?: object }} opts
 */
export function mergeBuildPlanWithExisting(payload, opts = {}) {
  const existing = opts.existingRecord;
  if (!existing || opts.force) return payload;
  const merged = { ...payload };
  if (existing.notes && !opts.force) merged.notes = existing.notes;
  return merged;
}
