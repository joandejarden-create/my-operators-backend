/**
 * Dealality Full E2E Operating System — public surface.
 */

export {
  OS_VERSION,
  EXECUTION_STATE,
  RUN_MODE,
  PRODUCT,
  PUBLICATION_LIFECYCLE,
  OVERALL_RUN_STATUS,
  mapGdiEvaluatorStatus,
  isTerminalExecutionState,
  isWaitingState,
} from "./execution-states.js";

export {
  STAGE_RESULT_SCHEMA_VERSION,
  buildStageResult,
  detectCountMismatch,
  assertHandoffIntegrity,
  hashArtifactRef,
} from "./stage-result.js";

export {
  GDI_FULL_E2E_WORKFLOW_ID,
  GDI_FULL_E2E_WORKFLOW_VERSION,
  GDI_REQUIRED_SEGMENTS,
  getGdiWorkflowDefinition,
  validateGdiRequiredSegments,
} from "./gdi-full-e2e-workflow-v1.js";

export {
  ADP_FULL_E2E_WORKFLOW_ID,
  ADP_FULL_E2E_WORKFLOW_VERSION,
  ADP_CERTIFICATION_AXES,
  getAdpWorkflowDefinition,
  assessAdpReportCertification,
} from "./adp-full-e2e-workflow-v1.js";

export {
  ADP_SECTION_COMPLETENESS_V1,
  ADP_PRODUCT_SECTIONS,
  SECTION_STATUS,
  assessAdpSectionCompleteness,
  summarizeSectionPortfolio,
} from "../ai-demand-positioning/certification/adp-section-completeness-v1.js";

export {
  NEXT_ACTION_EXECUTOR_VERSION,
  selectNextAction,
  loadRunStateFromDisk,
  recordZeroProgress,
} from "./next-action-executor.js";

export {
  PUBLICATION_CONTRACT_VERSION,
  buildPublicationDryRunPlan,
  reconcileAirtableReadback,
  classifyOpportunityPublicationState,
  assertPublicationAuth,
} from "./publication-and-readback.js";

export {
  AIRTABLE_SOT_MATRIX_VERSION,
  AIRTABLE_SOURCE_OF_TRUTH_MATRIX,
  SCHEMA_GAP_AND_MIGRATION_PLAN,
  getAirtableSotDocument,
} from "./airtable-source-of-truth.js";

export {
  LEARNING_POLICY_VERSION,
  LEARNING_CLASS,
  LEARNING_ADOPTION_STATUS,
  HISTORICAL_HOTEL_REGRESSION_MATRIX,
  createLearningRecord,
  isLearningAdopted,
} from "./learning-and-regression.js";

export {
  MARRIOTT_PORTFOLIO_IDENTITY,
  ADP_EXPLICIT_EXCLUSIONS,
  loadCensusLinks,
  resolveOsHotelIdentity,
} from "./hotel-identity.js";

import { OS_VERSION } from "./execution-states.js";
import { getGdiWorkflowDefinition } from "./gdi-full-e2e-workflow-v1.js";
import { getAdpWorkflowDefinition } from "./adp-full-e2e-workflow-v1.js";
import { getAirtableSotDocument } from "./airtable-source-of-truth.js";
import { HISTORICAL_HOTEL_REGRESSION_MATRIX } from "./learning-and-regression.js";

export {
  AUTONOMOUS_RUNNER_VERSION,
  runAutonomousWorkflow,
} from "./autonomous-stage-runner-v1.js";

export function getOperatingSystemManifest() {
  return Object.freeze({
    osVersion: OS_VERSION,
    products: Object.freeze({
      GDI: getGdiWorkflowDefinition(),
      ADP: getAdpWorkflowDefinition(),
    }),
    airtable: getAirtableSotDocument(),
    regressionMatrix: HISTORICAL_HOTEL_REGRESSION_MATRIX,
    principle:
      "Workflow implementation and versioned contracts are the authority; Cursor prompts only initiate and supervise an approved workflow.",
  });
}
