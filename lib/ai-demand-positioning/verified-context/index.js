export {
  ADP_MINIMUM_ATTRIBUTE_COVERAGE_V1,
  DEFAULT_MINIMUM_ELIGIBLE_ATTRIBUTES,
  resolveMinimumEligibleAttributeThreshold,
  assessMinimumAttributeCoverage,
} from "./adp-minimum-attribute-coverage-v1.js";

export {
  ADP_ATTRIBUTE_ELIGIBILITY_CLASSIFIER_V1,
  ELIGIBILITY_STATUS,
  SEMANTIC_EQUIVALENCE_GROUPS,
  attributeDefinitionsRegistryVersion,
  hashSelectionInventory,
  classifySelectedAttributeEligibility,
  collapseEligibleToUniqueConcepts,
} from "./adp-attribute-eligibility-classifier-v1.js";

export {
  ADP_ATTRIBUTE_SUGGESTION_CANDIDATES_V1,
  DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES,
  suggestAttributeCandidates,
} from "./adp-attribute-suggestion-candidates-v1.js";

export {
  ADP_VERIFIED_CONTEXT_BRIDGE_V1,
  DIAGNOSTIC_MODE,
  FIXTURE_BASELINE_MODE,
  MEASUREMENT_COMPARISON_STATUS,
  PILOT_HOTELS,
  REGRESSION_HOTELS,
  assessObservationEvidence,
  loadPublishedRealityGapReference,
  explainDenominatorDifference,
  buildVerifiedContextBridgeDiagnostic,
  buildPilotSuiteDiagnostics,
} from "./adp-verified-context-bridge-v1.js";

export {
  ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1,
  STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL,
} from "./adp-strategic-selection-airtable-contract-v1.js";
