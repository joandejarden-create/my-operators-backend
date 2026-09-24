/**
 * Research Coverage Airtable stores — upsert Targets / Runs / Target Runs.
 */

import Airtable from "airtable";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import { airtableRequestTimeoutMs } from "../../http/with-timeout.js";
import {
  RESEARCH_TARGETS_TABLE_NAME,
  RESEARCH_RUNS_TABLE_NAME,
  RESEARCH_TARGET_RUNS_TABLE_NAME,
  RESEARCH_COVERAGE_VERSION,
  MAP_RESEARCH_TARGET,
  MAP_RESEARCH_RUN,
  MAP_RESEARCH_TARGET_RUN,
  VAL_TARGET_TYPE,
  VAL_TARGET_STATUS,
  VAL_TARGET_PRIORITY,
  VAL_RESEARCH_CADENCE,
  VAL_RUN_TYPE,
  VAL_RUN_STATUS,
  VAL_EXECUTION_STATUS,
  VAL_RESULT_TYPE,
} from "./airtable-field-map.js";
import { buildResearchTarget, buildResearchRun, buildTargetRun } from "./entities.js";

function token() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

export function isResearchCoverageAirtableConfigured() {
  return Boolean(token() && getGdiOpportunitiesAirtableBaseId());
}

export function getRcBase() {
  const apiKey = token();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("gdi_research_coverage_airtable_not_configured");
    err.code = "gdi_research_coverage_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_research_coverage" });
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
}

function omitEmpty(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
}

function asSelect(value, allowed) {
  const v = value == null ? "" : String(value).trim();
  if (!v) return undefined;
  if (Array.isArray(allowed) && allowed.length && !allowed.includes(v)) return undefined;
  return v;
}

function joinLines(arr) {
  if (!Array.isArray(arr) || !arr.length) return undefined;
  return arr.map((x) => String(x || "").trim()).filter(Boolean).join("\n");
}

function jsonField(obj) {
  if (obj == null) return undefined;
  try {
    return JSON.stringify(obj);
  } catch {
    return undefined;
  }
}

async function findByField(tableName, fieldName, value) {
  const base = getRcBase();
  const formula = `{${fieldName}} = "${String(value).replace(/"/g, '\\"')}"`;
  const rows = await base(tableName)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

export function targetToFields(entity, links = {}) {
  const T = MAP_RESEARCH_TARGET;
  return omitEmpty({
    [T.targetId]: entity.targetId,
    [T.hotelId]: entity.hotelId,
    [T.hotelName]: entity.hotelName,
    [T.targetType]: asSelect(entity.targetType, VAL_TARGET_TYPE),
    [T.entityType]: entity.entityType,
    [T.canonicalName]: entity.canonicalName,
    [T.entityId]: entity.entityId,
    [T.programId]: entity.programId,
    [T.venueId]: entity.venueId,
    [T.organizationId]: entity.organizationId,
    [T.seriesId]: entity.seriesId,
    [T.demandGeneratorLink]: links.generatorRecordId ? [links.generatorRecordId] : undefined,
    [T.demandProgramLink]: links.programRecordId ? [links.programRecordId] : undefined,
    [T.privateEventVenueLink]: links.venueRecordId ? [links.venueRecordId] : undefined,
    [T.officialDomain]: entity.officialDomain,
    [T.primarySourceUrl]: entity.primarySourceUrl,
    [T.researchPlaybook]: entity.researchPlaybook,
    [T.priority]: asSelect(entity.priority, VAL_TARGET_PRIORITY),
    [T.status]: asSelect(entity.status, VAL_TARGET_STATUS),
    [T.reasonMonitored]: entity.reasonMonitored,
    [T.researchCadence]: asSelect(entity.researchCadence, VAL_RESEARCH_CADENCE),
    [T.nextResearchAt]: entity.nextResearchAt,
    [T.lastResearchedAt]: entity.lastResearchedAt,
    [T.lastSuccessfulResearchAt]: entity.lastSuccessfulResearchAt,
    [T.lastMaterialChangeAt]: entity.lastMaterialChangeAt,
    [T.lastResult]: entity.lastResult,
    [T.lastRunId]: entity.lastRunId,
    [T.consecutiveNoChangeRuns]: entity.consecutiveNoChangeRuns,
    [T.sourceFamilies]: joinLines(entity.sourceFamilies),
    [T.firstSeenAt]: entity.firstSeenAt,
    [T.createdAt]: entity.createdAt,
    [T.updatedAt]: entity.updatedAt,
    [T.confidence]: entity.confidence,
    [T.signalsFound]: entity.signalsFound,
    [T.opportunitiesCreated]: entity.opportunitiesCreated,
    [T.opportunitiesUpdated]: entity.opportunitiesUpdated,
    [T.successfulRuns]: entity.successfulRuns,
    [T.noChangeRuns]: entity.noChangeRuns,
    [T.schemaVersion]: RESEARCH_COVERAGE_VERSION,
    [T.payloadJson]: jsonField(entity.payload || entity),
  });
}

export function runToFields(entity) {
  const R = MAP_RESEARCH_RUN;
  return omitEmpty({
    [R.runId]: entity.runId,
    [R.hotelId]: entity.hotelId,
    [R.hotelName]: entity.hotelName,
    [R.runType]: asSelect(entity.runType, VAL_RUN_TYPE),
    [R.startedAt]: entity.startedAt,
    [R.completedAt]: entity.completedAt,
    [R.status]: asSelect(entity.status, VAL_RUN_STATUS),
    [R.targetsDue]: entity.targetsDue,
    [R.targetsAttempted]: entity.targetsAttempted,
    [R.targetsCompleted]: entity.targetsCompleted,
    [R.targetsMissed]: entity.targetsMissed,
    [R.targetsFailed]: entity.targetsFailed,
    [R.targetsNoChange]: entity.targetsNoChange,
    [R.newSignals]: entity.newSignals,
    [R.updatedSignals]: entity.updatedSignals,
    [R.newOpportunities]: entity.newOpportunities,
    [R.updatedOpportunities]: entity.updatedOpportunities,
    [R.queries]: entity.queries,
    [R.fetches]: entity.fetches,
    [R.estimatedCost]: entity.estimatedCost,
    [R.codeVersion]: entity.codeVersion,
    [R.gitSha]: entity.gitSha,
    [R.researchVersion]: entity.researchVersion,
    [R.notes]: entity.notes,
    [R.schemaVersion]: RESEARCH_COVERAGE_VERSION,
    [R.payloadJson]: jsonField(entity.payload || entity),
  });
}

export function targetRunToFields(entity) {
  const TR = MAP_RESEARCH_TARGET_RUN;
  return omitEmpty({
    [TR.targetRunId]: entity.targetRunId,
    [TR.researchRunLink]: entity.researchRunRecordId
      ? [entity.researchRunRecordId]
      : undefined,
    [TR.researchTargetLink]: entity.researchTargetRecordId
      ? [entity.researchTargetRecordId]
      : undefined,
    [TR.hotelId]: entity.hotelId,
    [TR.targetId]: entity.targetId,
    [TR.runId]: entity.runId,
    [TR.scheduledAt]: entity.scheduledAt,
    [TR.startedAt]: entity.startedAt,
    [TR.completedAt]: entity.completedAt,
    [TR.executionStatus]: asSelect(entity.executionStatus, VAL_EXECUTION_STATUS),
    [TR.resultType]: asSelect(entity.resultType, VAL_RESULT_TYPE),
    [TR.queriesUsed]: entity.queriesUsed,
    [TR.fetchesUsed]: entity.fetchesUsed,
    [TR.sourceCount]: entity.sourceCount,
    [TR.officialSourceCount]: entity.officialSourceCount,
    [TR.materialChange]: entity.materialChange === true ? true : undefined,
    [TR.failureReason]: entity.failureReason,
    [TR.newSignalCount]: entity.newSignalCount,
    [TR.updatedSignalCount]: entity.updatedSignalCount,
    [TR.newOpportunityCount]: entity.newOpportunityCount,
    [TR.updatedOpportunityCount]: entity.updatedOpportunityCount,
    [TR.lastResultSummary]: entity.lastResultSummary,
    [TR.sourceUrls]: joinLines(entity.sourceUrls),
    [TR.schemaVersion]: RESEARCH_COVERAGE_VERSION,
    [TR.payloadJson]: jsonField(entity.payload || entity),
  });
}

function recordToTarget(rec) {
  const f = rec.fields || {};
  const T = MAP_RESEARCH_TARGET;
  return buildResearchTarget({
    targetId: f[T.targetId],
    hotelId: f[T.hotelId],
    hotelName: f[T.hotelName],
    targetType: f[T.targetType],
    entityType: f[T.entityType],
    canonicalName: f[T.canonicalName],
    entityId: f[T.entityId],
    programId: f[T.programId],
    venueId: f[T.venueId],
    organizationId: f[T.organizationId],
    seriesId: f[T.seriesId],
    demandGeneratorId: f[T.entityId],
    officialDomain: f[T.officialDomain],
    primarySourceUrl: f[T.primarySourceUrl],
    researchPlaybook: f[T.researchPlaybook],
    priority: f[T.priority],
    status: f[T.status],
    reasonMonitored: f[T.reasonMonitored],
    researchCadence: f[T.researchCadence],
    nextResearchAt: f[T.nextResearchAt],
    lastResearchedAt: f[T.lastResearchedAt],
    lastSuccessfulResearchAt: f[T.lastSuccessfulResearchAt],
    lastMaterialChangeAt: f[T.lastMaterialChangeAt],
    lastResult: f[T.lastResult],
    lastRunId: f[T.lastRunId],
    consecutiveNoChangeRuns: f[T.consecutiveNoChangeRuns],
    sourceFamilies: String(f[T.sourceFamilies] || "")
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean),
    firstSeenAt: f[T.firstSeenAt],
    createdAt: f[T.createdAt],
    updatedAt: f[T.updatedAt],
    confidence: f[T.confidence],
    signalsFound: f[T.signalsFound],
    opportunitiesCreated: f[T.opportunitiesCreated],
    opportunitiesUpdated: f[T.opportunitiesUpdated],
    successfulRuns: f[T.successfulRuns],
    noChangeRuns: f[T.noChangeRuns],
    airtableRecordId: rec.id,
  });
}

export async function upsertResearchTarget(raw, { dryRun = true, links = {} } = {}) {
  const entity = buildResearchTarget(raw);
  const fields = targetToFields(entity, links);
  if (dryRun) return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  const existing = await findByField(
    RESEARCH_TARGETS_TABLE_NAME,
    MAP_RESEARCH_TARGET.targetId,
    entity.targetId
  );
  const base = getRcBase();
  if (existing) {
    const updated = await base(RESEARCH_TARGETS_TABLE_NAME).update(existing.id, fields);
    return { dryRun: false, entity: { ...entity, airtableRecordId: updated.id }, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(RESEARCH_TARGETS_TABLE_NAME).create(fields);
  return { dryRun: false, entity: { ...entity, airtableRecordId: created.id }, recordId: created.id, action: "CREATE" };
}

export async function upsertResearchRun(raw, { dryRun = true } = {}) {
  const entity = buildResearchRun(raw);
  const fields = runToFields(entity);
  if (dryRun) return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  const existing = await findByField(
    RESEARCH_RUNS_TABLE_NAME,
    MAP_RESEARCH_RUN.runId,
    entity.runId
  );
  const base = getRcBase();
  if (existing) {
    const updated = await base(RESEARCH_RUNS_TABLE_NAME).update(existing.id, fields);
    return { dryRun: false, entity: { ...entity, airtableRecordId: updated.id }, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(RESEARCH_RUNS_TABLE_NAME).create(fields);
  return { dryRun: false, entity: { ...entity, airtableRecordId: created.id }, recordId: created.id, action: "CREATE" };
}

export async function upsertTargetRun(raw, { dryRun = true } = {}) {
  const entity = buildTargetRun(raw);
  const fields = targetRunToFields(entity);
  if (dryRun) return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  const existing = await findByField(
    RESEARCH_TARGET_RUNS_TABLE_NAME,
    MAP_RESEARCH_TARGET_RUN.targetRunId,
    entity.targetRunId
  );
  const base = getRcBase();
  if (existing) {
    const updated = await base(RESEARCH_TARGET_RUNS_TABLE_NAME).update(existing.id, fields);
    return { dryRun: false, entity: { ...entity, airtableRecordId: updated.id }, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(RESEARCH_TARGET_RUNS_TABLE_NAME).create(fields);
  return { dryRun: false, entity: { ...entity, airtableRecordId: created.id }, recordId: created.id, action: "CREATE" };
}

export async function listTargetsForHotel(hotelId, { maxRecords = 500 } = {}) {
  const base = getRcBase();
  const formula = `{${MAP_RESEARCH_TARGET.hotelId}} = "${String(hotelId).replace(/"/g, '\\"')}"`;
  const out = [];
  await base(RESEARCH_TARGETS_TABLE_NAME)
    .select({ filterByFormula: formula, pageSize: 100, maxRecords })
    .eachPage((records, next) => {
      for (const r of records) out.push(recordToTarget(r));
      next();
    });
  return out;
}

export async function listAllRecords(tableName, { maxRecords = 1000 } = {}) {
  const base = getRcBase();
  const out = [];
  await base(tableName)
    .select({ pageSize: 100, maxRecords })
    .eachPage((records, next) => {
      out.push(...records);
      next();
    });
  return out;
}

export { RESEARCH_TARGETS_TABLE_NAME, RESEARCH_RUNS_TABLE_NAME, RESEARCH_TARGET_RUNS_TABLE_NAME };
