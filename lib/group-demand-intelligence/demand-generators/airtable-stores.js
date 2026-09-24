/**
 * Demand Generator Airtable stores — upsert by stable IDs.
 * Linked-record fields populated when table IDs / record ids available.
 */

import {
  getDgBase,
  omitEmpty,
  asSelect,
  joinLines,
  jsonField,
  DG_SCHEMA_VERSION,
} from "./airtable-client.js";
import {
  DEMAND_GENERATORS_TABLE_NAME,
  DEMAND_PROGRAMS_TABLE_NAME,
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  MAP_DEMAND_GENERATOR,
  MAP_DEMAND_PROGRAM,
  MAP_HOTEL_GENERATOR_FIT,
  MAP_DEMAND_GENERATOR_SIGNAL,
  VAL_ORGANIZATION_TYPE,
  VAL_GENERATOR_STATUS,
  VAL_PROGRAM_TYPE,
  VAL_RECURRENCE_STATUS,
  VAL_GENERATOR_PRIORITY,
  VAL_RELEVANCE_CLASS,
  VAL_SIGNAL_STATUS,
  VAL_TRIGGER_TYPE,
  VAL_SOURCE_AUTHORITY,
  VAL_FIT_BAND,
} from "./airtable-field-map.js";
import { buildDemandGeneratorEntity, buildProgramEntity } from "./entities.js";
import { buildHotelDemandGeneratorFit } from "./fit-relevance.js";
import { buildDemandGeneratorSignal } from "./qualify-promote.js";

async function findByField(tableName, fieldName, value) {
  const base = getDgBase();
  const formula = `{${fieldName}} = "${String(value).replace(/"/g, '\\"')}"`;
  const rows = await base(tableName)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

export function generatorToFields(entity, { generatorRecordId } = {}) {
  const G = MAP_DEMAND_GENERATOR;
  return omitEmpty({
    [G.demandGeneratorId]: entity.demandGeneratorId,
    [G.organizationName]: entity.organizationName,
    [G.organizationAliases]: joinLines(entity.organizationAliases),
    [G.organizationType]: asSelect(entity.organizationType, VAL_ORGANIZATION_TYPE),
    [G.officialDomain]: entity.officialDomain,
    [G.website]: entity.website,
    [G.headquartersLocation]: entity.headquartersLocation,
    [G.primaryMarkets]: joinLines(entity.primaryMarkets),
    [G.industry]: entity.industry,
    [G.generatorStatus]: asSelect(entity.generatorStatus, VAL_GENERATOR_STATUS),
    [G.sourceUrls]: joinLines(entity.sourceUrls),
    [G.sourceAuthority]: asSelect(entity.sourceAuthority, VAL_SOURCE_AUTHORITY),
    [G.firstSeenAt]: entity.firstSeenAt,
    [G.lastSeenAt]: entity.lastSeenAt,
    [G.lastVerifiedAt]: entity.lastVerifiedAt,
    [G.confidence]: entity.confidence,
    [G.nextResearchAt]: entity.nextResearchAt,
    [G.researchReason]: entity.researchReason,
    [G.lastResearchResult]: entity.lastResearchResult,
    [G.lastMaterialSignalAt]: entity.lastMaterialSignalAt,
    [G.schemaVersion]: DG_SCHEMA_VERSION,
    [G.payloadJson]: jsonField(entity.payload || entity),
  });
}

export function programToFields(entity, { generatorRecordId } = {}) {
  const P = MAP_DEMAND_PROGRAM;
  return omitEmpty({
    [P.programId]: entity.programId,
    [P.seriesId]: entity.seriesId,
    [P.demandGeneratorId]: entity.demandGeneratorId,
    [P.generatorLink]: generatorRecordId ? [generatorRecordId] : undefined,
    [P.programName]: entity.programName,
    [P.programAliases]: joinLines(entity.programAliases),
    [P.programType]: asSelect(entity.programType, VAL_PROGRAM_TYPE),
    [P.market]: entity.market,
    [P.typicalTiming]: entity.typicalTiming,
    [P.recurrenceStatus]: asSelect(entity.recurrenceStatus, VAL_RECURRENCE_STATUS),
    [P.recurrenceFrequency]: entity.recurrenceFrequency,
    [P.historicalCyclesJson]: jsonField(entity.historicalCycles),
    [P.confirmedFutureCycleJson]: jsonField(entity.confirmedFutureCycle),
    [P.sourceUrls]: joinLines(entity.sourceUrls),
    [P.firstSeenAt]: entity.firstSeenAt,
    [P.lastSeenAt]: entity.lastSeenAt,
    [P.lastVerifiedAt]: entity.lastVerifiedAt,
    [P.schemaVersion]: DG_SCHEMA_VERSION,
    [P.payloadJson]: jsonField(entity),
  });
}

export function fitToFields(entity, { generatorRecordId } = {}) {
  const F = MAP_HOTEL_GENERATOR_FIT;
  return omitEmpty({
    [F.fitId]: entity.fitId,
    [F.hotelId]: entity.hotelId,
    [F.hotelName]: entity.hotelName,
    [F.demandGeneratorId]: entity.demandGeneratorId,
    [F.generatorLink]: generatorRecordId ? [generatorRecordId] : undefined,
    [F.organizationName]: entity.organizationName,
    [F.marketRelevance]: asSelect(entity.marketRelevance, VAL_FIT_BAND),
    [F.productFit]: asSelect(entity.productFit, VAL_FIT_BAND),
    [F.travelDemandPotential]: asSelect(entity.travelDemandPotential, VAL_FIT_BAND),
    [F.recurrencePotential]: entity.recurrencePotential,
    [F.buyerAccessibility]: asSelect(entity.buyerAccessibility, VAL_FIT_BAND),
    [F.generatorPriority]: asSelect(entity.generatorPriority, VAL_GENERATOR_PRIORITY),
    [F.relevanceClass]: asSelect(entity.relevanceClass, VAL_RELEVANCE_CLASS),
    [F.fitRationale]: entity.fitRationale,
    [F.factorsJson]: jsonField(entity.factors),
    [F.lastEvaluatedAt]: entity.lastEvaluatedAt,
    [F.firstEvaluatedAt]: entity.firstEvaluatedAt,
    [F.schemaVersion]: DG_SCHEMA_VERSION,
    [F.payloadJson]: jsonField(entity),
  });
}

export function signalToFields(
  entity,
  { generatorRecordId, programRecordId, fitRecordId, opportunityRecordId } = {}
) {
  const S = MAP_DEMAND_GENERATOR_SIGNAL;
  return omitEmpty({
    [S.signalId]: entity.signalId,
    [S.demandGeneratorId]: entity.demandGeneratorId,
    [S.generatorLink]: generatorRecordId ? [generatorRecordId] : undefined,
    [S.programId]: entity.programId,
    [S.programLink]: programRecordId ? [programRecordId] : undefined,
    [S.seriesId]: entity.seriesId,
    [S.cycleId]: entity.cycleId,
    [S.hotelId]: entity.hotelId,
    [S.hotelGeneratorFitId]: entity.hotelGeneratorFitId,
    [S.fitLink]: fitRecordId ? [fitRecordId] : undefined,
    [S.triggerType]: asSelect(entity.triggerType, VAL_TRIGGER_TYPE),
    [S.title]: entity.title,
    [S.eventStartDate]: entity.eventStartDate,
    [S.eventEndDate]: entity.eventEndDate,
    [S.market]: entity.market,
    [S.hotelDemandThesis]: entity.hotelDemandThesis,
    [S.sourceUrl]: entity.sourceUrl,
    [S.sourceUrls]: joinLines(entity.sourceUrls),
    [S.signalStatus]: asSelect(entity.signalStatus, VAL_SIGNAL_STATUS),
    [S.recommendedAction]: entity.recommendedAction,
    [S.gdiOpportunityId]: entity.gdiOpportunityId,
    [S.gdiOpportunityLink]: opportunityRecordId ? [opportunityRecordId] : undefined,
    [S.failReasonsJson]: jsonField(entity.failReasons),
    [S.firstSeenAt]: entity.firstSeenAt,
    [S.lastSeenAt]: entity.lastSeenAt,
    [S.schemaVersion]: DG_SCHEMA_VERSION,
    [S.payloadJson]: jsonField(entity),
  });
}

export async function upsertDemandGenerator(raw, { dryRun = true } = {}) {
  const entity = buildDemandGeneratorEntity(raw);
  const fields = generatorToFields(entity);
  if (dryRun) {
    return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  }
  const existing = await findByField(
    DEMAND_GENERATORS_TABLE_NAME,
    MAP_DEMAND_GENERATOR.demandGeneratorId,
    entity.demandGeneratorId
  );
  const base = getDgBase();
  if (existing) {
    const updated = await base(DEMAND_GENERATORS_TABLE_NAME).update(existing.id, fields);
    return { dryRun: false, entity, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(DEMAND_GENERATORS_TABLE_NAME).create(fields);
  return { dryRun: false, entity, recordId: created.id, action: "CREATE" };
}

export async function upsertDemandProgram(
  raw,
  { dryRun = true, generatorRecordId } = {}
) {
  const entity = buildProgramEntity(raw);
  const fields = programToFields(entity, { generatorRecordId });
  if (dryRun) {
    return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  }
  const existing = await findByField(
    DEMAND_PROGRAMS_TABLE_NAME,
    MAP_DEMAND_PROGRAM.programId,
    entity.programId
  );
  const base = getDgBase();
  if (existing) {
    const updated = await base(DEMAND_PROGRAMS_TABLE_NAME).update(existing.id, fields);
    return { dryRun: false, entity, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(DEMAND_PROGRAMS_TABLE_NAME).create(fields);
  return { dryRun: false, entity, recordId: created.id, action: "CREATE" };
}

export async function upsertHotelGeneratorFit(
  raw,
  { dryRun = true, generatorRecordId } = {}
) {
  const entity = buildHotelDemandGeneratorFit(raw);
  const fields = fitToFields(entity, { generatorRecordId });
  if (dryRun) {
    return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  }
  const existing = await findByField(
    HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
    MAP_HOTEL_GENERATOR_FIT.fitId,
    entity.fitId
  );
  const base = getDgBase();
  if (existing) {
    const updated = await base(HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME).update(
      existing.id,
      fields
    );
    return { dryRun: false, entity, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME).create(fields);
  return { dryRun: false, entity, recordId: created.id, action: "CREATE" };
}

export async function upsertDemandGeneratorSignal(
  raw,
  { dryRun = true, generatorRecordId, programRecordId, fitRecordId } = {}
) {
  const entity = buildDemandGeneratorSignal(raw);
  const fields = signalToFields(entity, {
    generatorRecordId,
    programRecordId,
    fitRecordId,
  });
  if (dryRun) {
    return { dryRun: true, entity, fields, action: "UPSERT_PREVIEW" };
  }
  const existing = await findByField(
    DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
    MAP_DEMAND_GENERATOR_SIGNAL.signalId,
    entity.signalId
  );
  const base = getDgBase();
  if (existing) {
    const updated = await base(DEMAND_GENERATOR_SIGNALS_TABLE_NAME).update(
      existing.id,
      fields
    );
    return { dryRun: false, entity, recordId: updated.id, action: "UPDATE" };
  }
  const created = await base(DEMAND_GENERATOR_SIGNALS_TABLE_NAME).create(fields);
  return { dryRun: false, entity, recordId: created.id, action: "CREATE" };
}
