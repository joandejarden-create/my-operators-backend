/**
 * ADP Demand Leak Audit — public exports.
 */

export * from "./schema-v1.js";
export { createLeakAuditStore, resolveLeakAuditRoot, defaultLeakAuditStore } from "./store-v1.js";
export {
  assertLeakAuditWritePathAllowed,
  assertNoProductionMutationImports,
  PRODUCTION_MUTATION_ENTRY_POINTS,
} from "./isolation-guards-v1.js";
export { matchHotelReadOnly } from "./hotel-match-v1.js";
export { selectDemandTerritories, normalizeDemandSegmentKey } from "./territory-select-v1.js";
export { buildLimitedPromptPlan, toClientSafePromptFields } from "./prompt-catalog-v1.js";
export {
  processObservationFromResponse,
  buildPhase1ManualObservations,
} from "./observation-process-v1.js";
export { generateLeakAuditReportContent, WHO_DOES_THE_WORK, assertCautiousReportLanguage } from "./report-generator-v1.js";
export {
  buildClientSafeReportPayload,
  assertNoFullPromptInClientPayload,
  assertClientReportSafety,
} from "./client-report-payload-v1.js";
export { runLeakAuditPhase1, logLeakAuditProviderFailure } from "./run-audit-v1.js";
export {
  promoteLeakAuditToPilotStub,
  PROMOTE_CONFIRMATION_TEXT,
} from "./promote-stub-v1.js";
export { loadLeakAuditSampleReport, loadLeakAuditPortfolioSampleReport, SAMPLE_REPORT_FIXTURE_PATH, PORTFOLIO_SAMPLE_FIXTURE_PATH } from "./sample-report-v1.js";
export {
  generateSinglePropertyLeakAuditReport,
  markLeakAuditReportSent,
  ensureLeakAuditTempRoot,
} from "./report-generation-v1.js";
export { generatePortfolioLeakAuditReport } from "./portfolio-generation-v1.js";
export { copyFromAdpToLeakAudit } from "./copy-from-adp-v1.js";
export {
  RESEARCH_MODES,
  INTERNAL_RESEARCH_MODE,
  EXTERNAL_PRODUCT_LABEL,
  LEAK_AUDIT_LITE_COST_CONTROLS,
  PROMPT_SET_VERSION_LITE,
  LITE_PROVIDERS,
  buildScenariosMonitoredLabel,
  buildCoverMetaLine,
  buildDiagnosticScopeLabel,
  assertLiteCostControls,
  FORBIDDEN_PAID_ADP_WRITERS,
} from "./research-mode-lite-v1.js";
export {
  buildLitePromptPlan,
  LITE_SCENARIO_COUNT,
  assertNoFullPromptLibrary,
} from "./prompt-catalog-v1.js";
export {
  AIRTABLE_TABLE_CONTRACTS,
  LEAK_AUDIT_AIRTABLE_SCHEMA_VERSION,
  LEAK_AUDIT_TABLE_NAMES,
  LEAK_AUDIT_TABLE_FIELDS,
  listRequiredLeakAuditTables,
} from "./airtable-schema.js";
export {
  createLeakAuditAirtableClient,
  isLeakAuditAirtableConfigured,
  getLeakAuditAirtableConfig,
} from "./airtable-client.js";
export {
  createLeakAuditRepository,
  getDefaultLeakAuditRepository,
  detectLeakAuditStorageBackend,
  resolveLeakAuditLiveRoot,
  LEAK_AUDIT_LIVE_RELATIVE,
} from "./leak-audit-repository.js";
export {
  runFullVsLiteComparison,
  assertComparisonBundleShape,
  COMPARISON_HOTELS,
  COMPARISON_REPORT_DIR,
  scoreDirectionalReliability,
} from "./full-vs-lite-comparison-v1.js";
