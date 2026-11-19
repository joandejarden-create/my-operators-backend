/**
 * Hotel Intelligence Research Engine V1 — public exports.
 */

export {
  ENGINE_CONTRACT_ID,
  ENGINE_CONTRACT_VERSION,
  PUBLICATION_STATE,
  FAILURE_CLASS,
  P1_POLICY,
  writeGuaranteesP1,
} from "./contract.js";

export {
  ENGINE_STAGE,
  ENGINE_STAGE_GRAPH,
  ENGINE_TO_WORKFLOW_STAGE,
  STAGE_IO_SCHEMAS,
  assertSameStageGraphForAllHotels,
} from "./stages.js";

export {
  loadPilotManifest,
  resolvePilotHotel,
  listCanonicalPilotHpcIds,
  PILOT_MANIFEST_REL,
} from "./pilot-manifest.js";

export {
  orgDedupeKey,
  personDedupeKey,
  relationshipDedupeKey,
  channelDedupeKey,
  assertNoDuplicateKeys,
} from "./entity-keys.js";

export {
  emptyStagedPackage,
  normalizeOrganization,
  normalizeRelationship,
  normalizePerson,
  normalizeChannel,
  normalizeEvidence,
} from "./output-schemas.js";

export {
  PROVIDER_IDS,
  PAID_PROVIDERS,
  createProviderGateway,
  createMockProviderAdapter,
} from "./provider-interface.js";

export { createBudgetLedger, BUDGET_LEDGER_VERSION } from "./budget-ledger.js";

export {
  APPLY_MODE,
  resolveApplyMode,
  assertNoAirtableMutations,
} from "./apply-policy.js";

export { projectHotelExplorerContactViewModel } from "./he-contact-projection.js";

export {
  KGPV_HPC_ID,
  GOLDEN_DEMO_HOTEL_IDS,
  isGoldenDemoHotel,
  assertNotGoldenDemoAsProduction,
  assertResearchCompleteClaim,
} from "./golden-demo-isolation.js";

export {
  runHotelIntelligenceResearchEngine,
  runPilotCohortOffline,
} from "./supervisor.js";
