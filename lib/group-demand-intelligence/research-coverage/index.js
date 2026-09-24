/**
 * GDI Research Coverage & Target Registry V1 — public API.
 */

export * from "./constants.js";
export * from "./airtable-field-map.js";
export * from "./entities.js";
export * from "./coverage-engine.js";
export * from "./backfill-from-existing.js";
export * from "./portable-seed-templates.js";
export {
  ONBOARD_SEED_VERSION,
  buildGenericHotelSeedInput,
  proposeHotelOnboardSeed,
  compareOnboardSeedProposals,
  applyHotelOnboardSeed,
  assessWeeklyReadinessFromSeed,
} from "./onboard-hotel-research-graph.js";
export {
  isResearchCoverageAirtableConfigured,
  getRcBase,
  upsertResearchTarget,
  upsertResearchRun,
  upsertTargetRun,
  listTargetsForHotel,
  listAllRecords,
  targetToFields,
  runToFields,
  targetRunToFields,
} from "./airtable-stores.js";
