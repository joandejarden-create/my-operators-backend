/**
 * Validation for GTM Owner Target Airtable writes.
 */
import {
  MAP_GTM_OWNER_TARGET,
  MAP_GTM_TARGET_PROPERTY,
  VAL_GTM_OWNER_TYPE,
  VAL_GTM_PRIORITY_TIER,
  VAL_GTM_OUTREACH_STATUS,
  VAL_GTM_PITCH_STATUS,
  VAL_GTM_CONTACT_PATH,
  VAL_GTM_DATA_SOURCE,
  VAL_GTM_DATA_LICENSE,
  VAL_GTM_VISIBILITY,
  VAL_GTM_ICP_SEGMENT,
  VAL_GTM_DEAL_TRIGGER,
} from "./field-map.js";

function isAllowed(value, allowed) {
  return value == null || value === "" || allowed.includes(value);
}

/**
 * @param {object} fields Airtable field payload keyed by Airtable column names
 */
export function validateOwnerTargetWrite(fields) {
  const failures = [];

  const ownerName = fields[MAP_GTM_OWNER_TARGET.ownerName];
  if (!ownerName || !String(ownerName).trim()) {
    failures.push("Owner Name is required");
  }

  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.ownerType], VAL_GTM_OWNER_TYPE)) {
    failures.push(`Invalid Owner Type: ${fields[MAP_GTM_OWNER_TARGET.ownerType]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.priorityTier], VAL_GTM_PRIORITY_TIER)) {
    failures.push(`Invalid Priority Tier: ${fields[MAP_GTM_OWNER_TARGET.priorityTier]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.outreachStatus], VAL_GTM_OUTREACH_STATUS)) {
    failures.push(`Invalid Outreach Status: ${fields[MAP_GTM_OWNER_TARGET.outreachStatus]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.pitchStatus], VAL_GTM_PITCH_STATUS)) {
    failures.push(`Invalid Dealality Pitch Status: ${fields[MAP_GTM_OWNER_TARGET.pitchStatus]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.contactPath], VAL_GTM_CONTACT_PATH)) {
    failures.push(`Invalid Contact Path: ${fields[MAP_GTM_OWNER_TARGET.contactPath]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.dataSource], VAL_GTM_DATA_SOURCE)) {
    failures.push(`Invalid Data Source: ${fields[MAP_GTM_OWNER_TARGET.dataSource]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.dataLicense], VAL_GTM_DATA_LICENSE)) {
    failures.push(`Invalid Data License: ${fields[MAP_GTM_OWNER_TARGET.dataLicense]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.visibility], VAL_GTM_VISIBILITY)) {
    failures.push(`Invalid Visibility: ${fields[MAP_GTM_OWNER_TARGET.visibility]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.icpSegment], VAL_GTM_ICP_SEGMENT)) {
    failures.push(`Invalid ICP Segment: ${fields[MAP_GTM_OWNER_TARGET.icpSegment]}`);
  }
  if (!isAllowed(fields[MAP_GTM_OWNER_TARGET.dealTrigger], VAL_GTM_DEAL_TRIGGER)) {
    failures.push(`Invalid Deal Trigger: ${fields[MAP_GTM_OWNER_TARGET.dealTrigger]}`);
  }

  return {
    ok: failures.length === 0,
    failures,
  };
}

/**
 * @param {object} fields
 */
export function validatePropertyWrite(fields) {
  const failures = [];
  const buildingName = fields[MAP_GTM_TARGET_PROPERTY.buildingName];
  if (!buildingName || !String(buildingName).trim()) {
    failures.push("Building Name is required");
  }
  const ownerTarget = fields[MAP_GTM_TARGET_PROPERTY.ownerTarget];
  if (!ownerTarget || !Array.isArray(ownerTarget) || !ownerTarget.length) {
    failures.push("Owner Target link is required");
  }
  return { ok: failures.length === 0, failures };
}

/**
 * @param {object} rollup from computeOwnerRollup
 * @param {string} [importBatchId]
 */
export function buildOwnerTargetFieldsFromRollup(rollup, importBatchId) {
  const now = new Date().toISOString();
  const fields = {
    [MAP_GTM_OWNER_TARGET.ownerName]: rollup.ownerName,
    [MAP_GTM_OWNER_TARGET.ownerNameNormalized]: rollup.ownerNameNormalized,
    [MAP_GTM_OWNER_TARGET.ownerType]: rollup.ownerType,
    [MAP_GTM_OWNER_TARGET.priorityTier]: rollup.priorityTier,
    [MAP_GTM_OWNER_TARGET.outreachStatus]: "not_contacted",
    [MAP_GTM_OWNER_TARGET.pitchStatus]: "not_pitched",
    [MAP_GTM_OWNER_TARGET.propertyCount]: rollup.propertyCount,
    [MAP_GTM_OWNER_TARGET.totalRbaSf]: rollup.totalRbaSf,
    [MAP_GTM_OWNER_TARGET.marketsSummary]: rollup.marketsSummary,
    [MAP_GTM_OWNER_TARGET.countriesSummary]: rollup.countriesSummary,
    [MAP_GTM_OWNER_TARGET.sampleProperties]: rollup.sampleProperties,
    [MAP_GTM_OWNER_TARGET.dataSource]: "costar_internal",
    [MAP_GTM_OWNER_TARGET.dataLicense]: "costar_licensed_internal",
    [MAP_GTM_OWNER_TARGET.visibility]: "internal_only",
    [MAP_GTM_OWNER_TARGET.lastCostarSyncAt]: now,
  };
  if (importBatchId) {
    fields[MAP_GTM_OWNER_TARGET.importBatch] = [importBatchId];
  }
  return fields;
}

/**
 * @param {object} row parsed CoStar row
 * @param {string} ownerTargetId
 * @param {string} [importBatchId]
 */
export function buildPropertyFieldsFromRow(row, ownerTargetId, importBatchId) {
  const fields = {
    [MAP_GTM_TARGET_PROPERTY.buildingName]: row.buildingName || "Unnamed Property",
    [MAP_GTM_TARGET_PROPERTY.ownerTarget]: [ownerTargetId],
    [MAP_GTM_TARGET_PROPERTY.trueOwnerRaw]: row.trueOwner,
    [MAP_GTM_TARGET_PROPERTY.costarPropertyId]: row.costarPropertyId || null,
    [MAP_GTM_TARGET_PROPERTY.submarket]: row.submarket || null,
    [MAP_GTM_TARGET_PROPERTY.market]: row.market || null,
    [MAP_GTM_TARGET_PROPERTY.country]: row.country || null,
    [MAP_GTM_TARGET_PROPERTY.city]: row.city || null,
    [MAP_GTM_TARGET_PROPERTY.zipCode]: row.zipCode || null,
    [MAP_GTM_TARGET_PROPERTY.starRating]: row.starRating,
    [MAP_GTM_TARGET_PROPERTY.rbaGlaSf]: row.rbaGlaSf,
    [MAP_GTM_TARGET_PROPERTY.yearBuilt]: row.yearBuilt,
    [MAP_GTM_TARGET_PROPERTY.yearRenovated]: row.yearRenovated,
    [MAP_GTM_TARGET_PROPERTY.brandAffiliation]: row.brandAffiliation || null,
    [MAP_GTM_TARGET_PROPERTY.propertyType]: row.propertyType || "Hospitality",
    [MAP_GTM_TARGET_PROPERTY.builtRenovText]: row.builtRenovText || null,
    [MAP_GTM_TARGET_PROPERTY.sourceRowKey]: row.sourceRowKey,
  };
  if (importBatchId) {
    fields[MAP_GTM_TARGET_PROPERTY.importBatch] = [importBatchId];
  }
  return fields;
}

/**
 * Merge rollup metrics into an existing owner record without overwriting outreach fields.
 * @param {object} rollup
 */
export function buildOwnerTargetUpdateFieldsFromRollup(rollup) {
  return {
    [MAP_GTM_OWNER_TARGET.ownerName]: rollup.ownerName,
    [MAP_GTM_OWNER_TARGET.ownerNameNormalized]: rollup.ownerNameNormalized,
    [MAP_GTM_OWNER_TARGET.ownerType]: rollup.ownerType,
    [MAP_GTM_OWNER_TARGET.priorityTier]: rollup.priorityTier,
    [MAP_GTM_OWNER_TARGET.propertyCount]: rollup.propertyCount,
    [MAP_GTM_OWNER_TARGET.totalRbaSf]: rollup.totalRbaSf,
    [MAP_GTM_OWNER_TARGET.marketsSummary]: rollup.marketsSummary,
    [MAP_GTM_OWNER_TARGET.countriesSummary]: rollup.countriesSummary,
    [MAP_GTM_OWNER_TARGET.sampleProperties]: rollup.sampleProperties,
    [MAP_GTM_OWNER_TARGET.lastCostarSyncAt]: new Date().toISOString(),
  };
}
