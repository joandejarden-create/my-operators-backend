/**
 * Market Alerts intelligence orchestrator (deterministic V1.1).
 */
import Airtable from "airtable";
import { MAP_INTEL, audienceActionableField } from "../api/lib/market-alerts-intelligence-map.js";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";
import { inferMarketAlertEvent } from "./market-alerts-event-infer.js";
import { extractMarketAlertEntities } from "./market-alerts-entity-extract.js";
import { applyProjectDirectionToAudience, resolveAudienceIntelligence } from "./market-alerts-audience-rules.js";
import { buildAudienceTemplates } from "./market-alerts-templates.js";
import { applyQualificationGate } from "./market-alerts-qualification-gate.js";
import { inferSignalTiming } from "./market-alerts-signal-timing.js";
import { buildProjectLabel } from "./market-alerts-project-label.js";
import { inferProjectDirection } from "./market-alerts-project-direction.js";
import {
  buildRelatedSummary,
  buildWatchingList,
  findRelatedAlertsByEntityKey,
} from "./market-alerts-correlation.js";
import { assessContentQuality } from "./market-alerts-content-quality.js";
import { computeActionableFlags, isActionableForAudience } from "./market-alerts-actionability.js";
import { buildGenericAudienceCopy } from "./market-alerts-generic-copy.js";

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return null;
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
}

function audienceFieldPatch(audienceKey, audienceSlice, templates, actionableFlags) {
  const prefix =
    audienceKey === "owner" ? "Owner" : audienceKey === "brand" ? "Brand" : "Operator";
  const worthKey = MAP_INTEL[`worthReviewing${prefix}`];
  const signalKey = MAP_INTEL[`signalType${prefix}`];
  const stageKey = MAP_INTEL[`decisionStage${prefix}`];
  const whyKey = MAP_INTEL[`whyItMatters${prefix}`];
  const actionKey = MAP_INTEL[`recommendedAction${prefix}`];
  const actionableKey = MAP_INTEL[`actionable${prefix}`];

  const patch = {
    [worthKey]: !!audienceSlice.worthReviewing,
    [signalKey]: audienceSlice.worthReviewing ? audienceSlice.signalType || null : null,
    [stageKey]: audienceSlice.worthReviewing ? audienceSlice.decisionStage || null : null,
    [whyKey]: audienceSlice.worthReviewing ? templates.whyItMatters || null : null,
    [actionKey]: audienceSlice.worthReviewing ? templates.recommendedAction || null : null,
    [actionableKey]: !!actionableFlags?.[audienceKey],
  };
  return patch;
}

/**
 * Build intelligence field patch from alert content.
 * @param {{ title?: string, summary?: string, relatedCandidates?: Array<object>, alertId?: string }} input
 */
export function computeMarketAlertIntelligence(input = {}) {
  const title = input.title || "";
  const summary = input.summary || "";
  const sourceName = input.sourceName || input.source || "";

  const contentQuality = assessContentQuality({ title, summary });
  if (contentQuality.ignore) {
    return {
      fields: {
        [MAP_INTEL.intelligenceStatus]: "Ready",
        [MAP_INTEL.intelligenceTreatment]: "IGNORE",
        [MAP_INTEL.intelligenceUpdatedAt]: new Date().toISOString(),
        [MAP_INTEL.worthReviewingOwner]: false,
        [MAP_INTEL.worthReviewingBrand]: false,
        [MAP_INTEL.worthReviewingOperator]: false,
        [MAP_INTEL.actionableOwner]: false,
        [MAP_INTEL.actionableBrand]: false,
        [MAP_INTEL.actionableOperator]: false,
      },
      meta: {
        treatment: "IGNORE",
        contentQualityReason: contentQuality.reason || null,
      },
    };
  }

  const rawEvent = inferMarketAlertEvent({ title, summary });
  let entities = extractMarketAlertEntities({
    title,
    summary,
    eventType: rawEvent.eventType,
    sourceName,
  });
  let audience = resolveAudienceIntelligence({
    eventType: rawEvent.eventType,
    title,
    summary,
    entities,
  });

  const gated = applyQualificationGate({
    title,
    summary,
    sourceName,
    event: rawEvent,
    entities,
    audience,
  });

  const eventType = gated.eventType;
  const whatChanged = gated.whatChanged ?? (eventType ? rawEvent.whatChanged : null);
  entities = gated.entities;
  audience = gated.audience;

  const projectDirection = inferProjectDirection({
    eventType,
    title,
    summary,
  });
  audience = applyProjectDirectionToAudience(audience, projectDirection);

  const projectLabel = buildProjectLabel({
    eventType,
    rooms: entities.rooms,
    hotelProject: entities.hotelProject,
    title,
    summary,
  });
  entities = { ...entities, projectLabel: projectLabel || null };

  const signalTiming = inferSignalTiming({
    eventType,
    title,
    summary,
    entities,
  });

  const ownerTpl = buildAudienceTemplates("owner", {
    eventType,
    whatChanged,
    entities,
    signalType: audience.owner.signalType,
    decisionStage: audience.owner.decisionStage,
    projectDirection,
  });
  const brandTpl = buildAudienceTemplates("brand", {
    eventType,
    whatChanged,
    entities,
    signalType: audience.brand.signalType,
    decisionStage: audience.brand.decisionStage,
    projectDirection,
  });
  const operatorTpl = buildAudienceTemplates("operator", {
    eventType,
    whatChanged,
    entities,
    signalType: audience.operator.signalType,
    decisionStage: audience.operator.decisionStage,
    projectDirection,
  });

  const related = findRelatedAlertsByEntityKey(
    entities.entityKey,
    input.relatedCandidates || [],
    { excludeId: input.alertId }
  );
  const relatedSummary = buildRelatedSummary(related);
  const watching = buildWatchingList(entities);

  const actionableFlags = computeActionableFlags({
    treatment: audience.treatment,
    owner: audience.owner,
    brand: audience.brand,
    operator: audience.operator,
    signalTiming,
    projectDirection: projectDirection || null,
    eventType,
  });

  /** @type {Record<string, any>} */
  const fields = {
    [MAP_INTEL.intelligenceStatus]: "Ready",
    [MAP_INTEL.intelligenceTreatment]: audience.treatment,
    [MAP_INTEL.intelligenceUpdatedAt]: new Date().toISOString(),
    [MAP_INTEL.signalTiming]: signalTiming || null,
    [MAP_INTEL.projectDirection]: projectDirection || null,
    ...audienceFieldPatch("owner", audience.owner, ownerTpl, actionableFlags),
    ...audienceFieldPatch("brand", audience.brand, brandTpl, actionableFlags),
    ...audienceFieldPatch("operator", audience.operator, operatorTpl, actionableFlags),
  };

  // Clear nullable intelligence fields when downgraded
  fields[MAP_INTEL.eventType] = eventType || null;
  fields[MAP_INTEL.whatChanged] = whatChanged || null;
  fields[MAP_INTEL.hotelProject] = entities.hotelProject || null;
  fields[MAP_INTEL.ownerDeveloper] = entities.ownerDeveloper || null;
  fields[MAP_INTEL.brandInvolved] = entities.brandInvolved || null;
  fields[MAP_INTEL.operatorInvolved] = entities.operatorInvolved || null;
  fields[MAP_INTEL.rooms] = entities.rooms ?? null;
  fields[MAP_INTEL.assetProjectStage] = entities.assetProjectStage || null;
  fields[MAP_INTEL.entityKey] = entities.entityKey || null;

  return {
    fields,
    meta: {
      event: { ...rawEvent, eventType, whatChanged },
      entities,
      audience,
      gate: gated.stats,
      related,
      relatedSummary,
      watching,
      treatment: audience.treatment,
      signalTiming,
      projectDirection: projectDirection || null,
      projectLabel: projectLabel || null,
      actionable: actionableFlags,
    },
  };
}

/**
 * Filter patch to only fields that exist on the table (guard missing schema).
 * @param {Record<string, any>} fields
 * @param {Set<string>|null} knownFieldNames
 */
export function filterIntelligenceFieldsToKnown(fields, knownFieldNames) {
  if (!knownFieldNames) return { fields, skipped: [] };
  const out = {};
  const skipped = [];
  for (const [k, v] of Object.entries(fields)) {
    if (knownFieldNames.has(k)) out[k] = v;
    else skipped.push(k);
  }
  return { fields: out, skipped };
}

/**
 * Apply intelligence patch to an existing MarketAlerts row.
 * @param {string} recordId
 * @param {Record<string, any>} fieldsPatch
 * @param {{ tableName?: string, knownFieldNames?: Set<string>|null, dryRun?: boolean }} [opts]
 */
export async function applyIntelligencePatch(recordId, fieldsPatch, opts = {}) {
  const tableName = opts.tableName || process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const { fields, skipped } = filterIntelligenceFieldsToKnown(
    fieldsPatch,
    opts.knownFieldNames || null
  );

  if (skipped.length && process.env.NODE_ENV !== "production") {
    console.warn(
      "[market-alerts-intelligence] skipping unknown fields:",
      skipped.join(", ")
    );
  }

  if (!Object.keys(fields).length) {
    return {
      ok: false,
      skipped: true,
      reason: "no_writable_fields",
      skippedFields: skipped,
      validation: { ok: false, errors: ["No intelligence fields available to write"] },
      sanitizedPreview: {},
      fieldMapping: MAP_INTEL,
    };
  }

  if (opts.dryRun) {
    return {
      ok: true,
      dryRun: true,
      recordId,
      sanitizedPreview: fields,
      skippedFields: skipped,
      fieldMapping: MAP_INTEL,
      validation: { ok: true, errors: [] },
    };
  }

  const base = getBase();
  if (!base) {
    return {
      ok: false,
      error: "Airtable not configured",
      validation: { ok: false, errors: ["Airtable not configured"] },
      fieldMapping: MAP_INTEL,
    };
  }

  try {
    await base(tableName).update(recordId, fields, { typecast: true });
    return {
      ok: true,
      recordId,
      sanitizedPreview: fields,
      skippedFields: skipped,
      fieldMapping: MAP_INTEL,
      validation: { ok: true, errors: [] },
    };
  } catch (err) {
    console.error("[market-alerts-intelligence] applyIntelligencePatch failed:", err.message || err);
    return {
      ok: false,
      recordId,
      error: err.message || String(err),
      sanitizedPreview: fields,
      fieldMapping: MAP_INTEL,
      validation: { ok: true, errors: [] },
      errorHandling: "api_error",
    };
  }
}

/**
 * Enrich a newly created alert. Failures set Error status when possible; never throw to caller sync.
 * @param {string} recordId
 * @param {Record<string, any>} alertFields Airtable field map (Title/Summary…)
 * @param {{ tableName?: string, knownFieldNames?: Set<string>|null }} [opts]
 */
export async function enrichMarketAlertIntelligence(recordId, alertFields, opts = {}) {
  try {
    const computed = computeMarketAlertIntelligence({
      title: alertFields[MAP_ALERT.title] || alertFields.Title || "",
      summary: alertFields[MAP_ALERT.summary] || alertFields.Summary || "",
      alertId: recordId,
    });
    return await applyIntelligencePatch(recordId, computed.fields, opts);
  } catch (err) {
    console.error("[market-alerts-intelligence] enrich failed:", err.message || err);
    try {
      await applyIntelligencePatch(
        recordId,
        {
          [MAP_INTEL.intelligenceStatus]: "Error",
          [MAP_INTEL.intelligenceUpdatedAt]: new Date().toISOString(),
        },
        opts
      );
    } catch (inner) {
      console.error(
        "[market-alerts-intelligence] failed to set Error status:",
        inner.message || inner
      );
    }
    return { ok: false, recordId, error: err.message || String(err) };
  }
}

/**
 * Map stored Airtable fields → nested intelligence object for one audience.
 * @param {Record<string, any>} fields
 * @param {'owner'|'brand'|'operator'|'all'|null} audience
 */
export function intelligencePayloadForAudience(fields, audience) {
  if (!fields) return null;

  const entities = {
    hotelProject: fields[MAP_INTEL.hotelProject] || null,
    ownerDeveloper: fields[MAP_INTEL.ownerDeveloper] || null,
    brandInvolved: fields[MAP_INTEL.brandInvolved] || null,
    operatorInvolved: fields[MAP_INTEL.operatorInvolved] || null,
    rooms: fields[MAP_INTEL.rooms] ?? null,
    assetProjectStage: fields[MAP_INTEL.assetProjectStage] || null,
    entityKey: fields[MAP_INTEL.entityKey] || null,
  };

  const watching = buildWatchingList(entities);
  const base = {
    eventType: fields[MAP_INTEL.eventType] || null,
    whatChanged: fields[MAP_INTEL.whatChanged] || null,
    treatment: fields[MAP_INTEL.intelligenceTreatment] || null,
    status: fields[MAP_INTEL.intelligenceStatus] || null,
    entities,
    watching,
    relatedSummary: null,
  };

  if (audience === "all") {
    const owner = intelligencePayloadForAudience(fields, "owner");
    const brand = intelligencePayloadForAudience(fields, "brand");
    const operator = intelligencePayloadForAudience(fields, "operator");
    const actionable = !!(owner?.actionable || brand?.actionable || operator?.actionable);
    const worthReviewing = !!(
      owner?.worthReviewing ||
      brand?.worthReviewing ||
      operator?.worthReviewing
    );
    const copy = buildGenericAudienceCopy({
      actionable,
      worthReviewing,
      eventType: base.eventType,
      signalTypes: [owner?.signalType, brand?.signalType, operator?.signalType],
    });
    return {
      ...base,
      audience: "all",
      worthReviewing,
      actionable,
      signalType: copy.signalType,
      stage: owner?.stage || brand?.stage || operator?.stage || null,
      signalTiming: owner?.signalTiming || brand?.signalTiming || operator?.signalTiming || null,
      projectDirection:
        owner?.projectDirection || brand?.projectDirection || operator?.projectDirection || null,
      timingDirection:
        owner?.timingDirection || brand?.timingDirection || operator?.timingDirection || null,
      whyItMatters: copy.whyItMatters,
      recommendedAction: copy.recommendedAction,
    };
  }

  if (!audience || !["owner", "brand", "operator"].includes(audience)) {
    return intelligencePayloadForAudience(fields, "all");
  }

  const worthKey =
    audience === "owner"
      ? MAP_INTEL.worthReviewingOwner
      : audience === "brand"
        ? MAP_INTEL.worthReviewingBrand
        : MAP_INTEL.worthReviewingOperator;
  const signalKey =
    audience === "owner"
      ? MAP_INTEL.signalTypeOwner
      : audience === "brand"
        ? MAP_INTEL.signalTypeBrand
        : MAP_INTEL.signalTypeOperator;
  const stageKey =
    audience === "owner"
      ? MAP_INTEL.decisionStageOwner
      : audience === "brand"
        ? MAP_INTEL.decisionStageBrand
        : MAP_INTEL.decisionStageOperator;
  const whyKey =
    audience === "owner"
      ? MAP_INTEL.whyItMattersOwner
      : audience === "brand"
        ? MAP_INTEL.whyItMattersBrand
        : MAP_INTEL.whyItMattersOperator;
  const actionKey =
    audience === "owner"
      ? MAP_INTEL.recommendedActionOwner
      : audience === "brand"
        ? MAP_INTEL.recommendedActionBrand
        : MAP_INTEL.recommendedActionOperator;
  const actionableKey = audienceActionableField(audience);

  const title = fields.Title || fields["Title"] || "";
  const summary = fields.Summary || fields["Summary"] || "";
  const storedTiming = fields[MAP_INTEL.signalTiming] || null;
  const storedDirection = fields[MAP_INTEL.projectDirection] || null;
  const signalTiming =
    storedTiming ||
    inferSignalTiming({
      eventType: base.eventType,
      title,
      summary,
      entities,
    });
  const projectDirection =
    storedDirection ||
    inferProjectDirection({
      eventType: base.eventType,
      title,
      summary,
    });

  const worthReviewing = !!fields[worthKey];
  const signalType = fields[signalKey] || null;
  const stage = fields[stageKey] || null;

  let actionable = actionableKey ? !!fields[actionableKey] : false;
  if (!actionableKey || fields[actionableKey] == null) {
    actionable = isActionableForAudience({
      audience,
      worthReviewing,
      signalType,
      decisionStage: stage,
      signalTiming,
      projectDirection,
      eventType: base.eventType,
      treatment: base.treatment,
    });
  }

  const timingDirection =
    signalTiming && projectDirection
      ? `${signalTiming} · ${projectDirection}`
      : signalTiming || null;

  return {
    ...base,
    audience,
    worthReviewing,
    actionable,
    signalType,
    stage,
    signalTiming,
    projectDirection: projectDirection || null,
    timingDirection,
    whyItMatters: fields[whyKey] || null,
    recommendedAction: fields[actionKey] || null,
  };
}

/**
 * Resolve product audience from legacy role flags.
 * @param {{ isOwner?: boolean, isBrand?: boolean, isOperator?: boolean, isOwnerOperator?: boolean, isAdmin?: boolean }|null} flags
 * @returns {'owner'|'brand'|'operator'|null}
 */
export function marketAlertsAudienceFromFlags(flags) {
  if (!flags) return null;
  if (flags.isOwnerOperator || (flags.isOwner && flags.isOperator)) return "owner";
  if (flags.isOwner) return "owner";
  if (flags.isBrand) return "brand";
  if (flags.isOperator) return "operator";
  return null;
}
