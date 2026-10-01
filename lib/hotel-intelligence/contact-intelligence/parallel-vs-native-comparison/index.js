export {
  COMPARISON_ID,
  COMPARISON_LABEL,
  PARALLEL_ADAPTER_ROOT,
  ARM_IDS,
  PARALLEL_PROCESSOR_FREEZE,
  SPENDING_CAPS,
  COHORT_META,
  BUSINESS_OBJECTIVE,
  RELATIONSHIP_ENUM,
} from "./comparison-config.js";

export {
  loadCohortFreeze,
  buildSharedHotelInput,
  buildNativeCaseInput,
  buildParallelHotelSeed,
  auditInputLeakage,
  buildCohortSharedInputs,
  documentCodeTuningExposure,
} from "./shared-hotel-inputs.js";

export {
  OUTPUT_CONTRACT_VERSION,
  createEmptyReviewRow,
  normalizeNativeResearchToContract,
  normalizeParallelArtifactToContract,
  assertNoOwnershipPromotion,
  scoreboardMetrics,
} from "./common-output-contract.js";

export {
  buildComparisonResearchObjectiveText,
  compileComparisonParallelTask,
} from "./parallel-instructions.js";

export {
  buildComparisonLeanOutputJsonSchema,
  applyLeanComparisonOutputSchema,
  COMPARISON_OUTPUT_SCHEMA_VERSION,
} from "./lean-output-schema.js";
