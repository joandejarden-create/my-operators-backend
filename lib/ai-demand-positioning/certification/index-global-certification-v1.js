/**
 * Barrel exports for the global ADP certification framework.
 */

export {
  ADP_PERIOD_PIPELINE_STATES,
  isCustomerOfficialAllowed,
  isAdminPreviewAllowed,
  mapLegacyCertificationToPipelineState,
} from "./adp-period-pipeline-states-v1.js";

export {
  certifyAdpPeriod,
  runAdpIdentityPreflight,
  persistCertificationManifest,
  loadCertificationManifest,
  ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
} from "./certify-adp-period-v1.js";

export {
  buildAdpHotelIdentityContract,
  validateAdpHotelIdentityContract,
  runIdentityCanaries,
  buildIdentityCanaries,
  assertMissingAliasWouldFailCanary,
  listAllAdpPropertyProfiles,
  IDENTITY_PREFLIGHT_OUTCOMES,
} from "./adp-hotel-identity-contract-v1.js";

export {
  buildScenarioUniverseManifest,
  diffScenarioUniverses,
  scenarioIdsFromPeriod,
} from "./adp-scenario-universe-contract-v1.js";

export {
  evaluateAdpComparability,
  buildMatchedPeerComparison,
  buildCustomerComparisonGate,
  ADP_COMPARABILITY_OUTCOMES,
  NOT_FORMALLY_COMPARABLE_CUSTOMER_COPY,
} from "./adp-comparability-engine-v1.js";

export {
  auditSourceAttributionLabels,
  classifyCitationForProperty,
  SOURCE_ATTRIBUTION_TAXONOMY,
  SOURCE_LABEL_METRICS,
} from "./adp-source-attribution-contract-v1.js";

export {
  resolveDomainOwnershipForProperty,
  lookupBrandDomainOwnership,
  BRAND_DOMAIN_OWNERSHIP_REGISTRY_V1,
} from "./adp-domain-ownership-registry-v1.js";

export {
  detectAdpAnomalies,
  forensicProviderZeroPresence,
  explainAdpAnomaly,
  auditCrossMetricConsistency,
} from "./adp-anomaly-rules-v1.js";

export {
  assertPeriodMutableForMetricWrite,
  buildCorrectionPeriodLinkage,
} from "./adp-period-immutability-v1.js";

export {
  ADP_REGRESSION_CONTROL_HOTELS_V1,
  ADP_REGRESSION_TEST_TYPES,
  listRegressionControlPropertyIds,
} from "./adp-regression-control-set-v1.js";

export {
  PROPERTY_QA_STATUS,
  PERIOD_CERTIFICATION_STATUS,
  CURRENT_PERIOD_CLASS,
  PUBLISH_ELIGIBILITY,
  LEGACY_STATUS,
  COMPARABILITY_STATUS,
  isCertificationEraPeriod,
  mapEngineToPropertyQaStatus,
  resolvePeriodCertificationStatus,
  resolveCurrentPeriodClass,
  resolvePublishEligibility,
} from "./adp-status-dimensions-v1.js";

export {
  ADP_PROVIDER_COMPLETENESS_POLICY_NAME,
  evaluateProviderCompletenessGate,
  describeProviderCompletenessPolicy,
  maxAllowedFailures,
} from "./adp-provider-completeness-policy-v1.js";

export {
  buildCanonicalAdpInventory,
  buildAdpPropertyInventoryRow,
  summarizeInventoryCounts,
} from "./adp-certification-inventory-v1.js";
