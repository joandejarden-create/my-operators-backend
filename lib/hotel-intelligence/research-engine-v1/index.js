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
  buildOperationId,
  createExecutionGateway,
  EXECUTION_GATEWAY_VERSION,
} from "./provider-interface.js";

export { createBudgetLedger, BUDGET_LEDGER_VERSION } from "./budget-ledger.js";

export {
  sealProviderAdapters,
} from "./execution-gateway.js";

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
  EXECUTION_MODE,
} from "./supervisor.js";

export {
  GRAPH_CONTRACT_ID,
  GRAPH_CONTRACT_VERSION,
  GRAPH_SCHEMA_VERSION,
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_BASE_ID_FORBIDDEN,
  HPC_TABLE_ID,
  HPC_TABLE_NAME,
  HI_GRAPH_TABLES,
  P2_POLICY,
  PUBLICATION_LAYER,
  RECORD_PUBLICATION_STATE,
  EVIDENCE_ENTITY_TYPE_EXTENSIONS,
  GRAPH_EVIDENCE_ENTITY_TYPES,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
  writeGuaranteesP2,
} from "./airtable-graph-contract-v1.js";

export {
  planGraphUpserts,
  resolveGraphDataApply,
  emptyGraphIndex,
} from "./airtable-graph-upsert-planner.js";

export {
  createSyntheticGraphStore,
  reconcileReadAfterWrite,
  assertNoCustomerPublication,
} from "./airtable-graph-store.js";

export {
  validateHotelOrgRelationshipRefs,
  buildRelationshipAirtableFields,
  assertWriteBase,
  REFERENTIAL_INTEGRITY_VERSION,
} from "./airtable-graph-referential-integrity.js";

export {
  BENCHMARK_CONTRACT_ID,
  BENCHMARK_CONTRACT_VERSION,
  BENCHMARK_PHASE,
  BENCHMARK_MODE,
  BENCHMARK_ARM,
  P3A_POLICY,
  writeGuaranteesP3A,
  loadP3ASixHotelCohort,
  buildProviderCapabilityMatrix,
  runP3ABenchmarkDryRun,
  proposedMinimumPaidBenchmarkBudget,
  buildP3BExecutionPlan,
} from "./benchmark-p3a/index.js";

export {
  P3B_M1_CONTRACT_ID,
  P3B_M1_CONTRACT_VERSION,
  P3B_M1_PHASE,
  P3B_M1_POLICY,
  writeGuaranteesP3BM1,
  resolveP3BM1FounderPaidAuthorization,
  loadP3BM1PilotCohort,
  runP3BM1ReadinessValidation,
  buildP3BM1PlannedOperations,
  buildProposedExecutionCommand,
  probeConfiguredProviderAvailability,
} from "./benchmark-p3b-m1/index.js";

export {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
  OWNERSHIP_DRAWER_QUESTIONS,
  OWNERSHIP_ROLE,
  OWNERSHIP_SUCCESS_TIER,
  PRIMARY_OWNERSHIP_GOALS,
  classifyOwnershipResearchSuccess,
  scoreUltimateOwnershipDimension,
  writeOwnershipObjectiveGuarantees,
} from "./ownership-research-objective.js";
