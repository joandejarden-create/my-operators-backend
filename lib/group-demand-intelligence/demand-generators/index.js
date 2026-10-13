/**
 * GDI Demand Generator Intelligence V1 — public surface.
 */

export * from "./constants.js";
export {
  computeDemandGeneratorId,
  computeProgramId,
  computeSeriesId,
  domainFromUrl,
  normalizeOrganizationType,
  normalizeProgramType,
  normalizeRecurrenceStatus,
  assertNoInventedFuture,
  buildDemandGeneratorEntity,
  buildProgramEntity,
} from "./entities.js";
export {
  computeHotelGeneratorFitId,
  evaluateGeneratorRelevance,
  buildHotelDemandGeneratorFit,
  suggestNextResearchAt,
  organizationTypesForHotelProfile,
} from "./fit-relevance.js";
export {
  computeSignalId,
  qualifyGeneratorSignal,
  buildDemandGeneratorSignal,
  buildGeneratorDrivenOpportunityDraft,
} from "./qualify-promote.js";
export {
  buildGeneratorDiscoveryQueries,
  buildGeneratorGuidedQueries,
  compareSearchEfficiency,
  measureIncrementalAtBats,
} from "./discovery.js";
export {
  MAP_DEMAND_GENERATOR,
  MAP_DEMAND_PROGRAM,
  MAP_HOTEL_GENERATOR_FIT,
  MAP_DEMAND_GENERATOR_SIGNAL,
  MAP_GDI_DG_LINK,
} from "./airtable-field-map.js";
export {
  upsertDemandGenerator,
  upsertDemandProgram,
  upsertHotelGeneratorFit,
  upsertDemandGeneratorSignal,
  generatorToFields,
  programToFields,
  fitToFields,
  signalToFields,
} from "./airtable-stores.js";
export { auditDemandGeneratorHotelSpecificLogic } from "./hotel-specific-audit.js";
export {
  isDgAirtableConfigured,
  getDgAirtableBaseId,
  getDgBase,
} from "./airtable-client.js";
export {
  classifyWatchFailureReasons,
  buildLodgingEvidenceQueries,
  extractLodgingEvidenceFromText,
  mergeEvidenceFindings,
  evidenceToQualificationHints,
  classifyAgainstExistingGdi,
  isOfficialishUrl,
  rankHarvestTargets,
  WATCH_FAIL_REASON,
  LODGING_EVIDENCE_STRENGTH,
  HARVEST_CLASSIFICATION,
} from "./lodging-evidence.js";
