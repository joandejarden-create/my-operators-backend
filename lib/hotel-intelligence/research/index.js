/**
 * Packet 2.6C — Hotel Intelligence Deep Research Center (public API surface).
 * Packet 2.6C-R2 adds live Webhound + profile discovery exports.
 */

export {
  RESEARCH_STATUSES,
  ACTIVE_STATUSES,
  TERMINAL_STATUSES,
  customerStageLabel,
  isActiveStatus,
  isTerminalStatus,
} from "./statuses.js";

export {
  RESEARCH_TEMPLATES,
  FOLLOW_UP_TEMPLATE_IDS,
  listTemplates,
  getTemplate,
  customerTemplateView,
  TEMPLATE_REGISTRY_VERSION,
} from "./templates.js";

export { recommendFollowUps } from "./recommend.js";
export { createResearchRepository } from "./repository.js";
export { createResearchOrchestrator } from "./orchestrator.js";
export {
  ensureKgpvRun1Backfill,
  KGPV_HOTEL_ID,
  KGPV_RUN1_REQUEST_ID,
  KGPV_RUN1_RUN_ID,
  resolveKgpvHistoricalCostUsd,
} from "./backfill-kgpv.js";
export {
  resolveReportExportLinks,
  reportTypeLabel,
  REPORT_KINDS,
  REPORT_EXPORT_CONTRACT_VERSION,
} from "./report-export-contract.js";
export {
  isExternalResearchEnabled,
  isFounderInternalDebug,
  assertExternalResearchAllowed,
  requireProviderBudget,
  clampProviderBudgetUsd,
  getPilotMaxProviderBudgetUsd,
  getDailyExternalBudgetUsd,
  getMaxConcurrentWebhoundRuns,
  PILOT_MAX_PROVIDER_BUDGET_USD,
  PILOT_DEFAULT_DAILY_EXTERNAL_BUDGET_USD,
} from "./policy.js";
export {
  createSimulationProvider,
  createNativeProvider,
  createWebhoundProvider,
  resolveProvider,
  PROVIDER_IDS,
  PROVIDER_STRATEGIES,
} from "./providers.js";
export { createWebhoundMcpClient, resolveWebhoundKey } from "./webhound-mcp-client.js";
export {
  buildResearchCenterPayload,
  buildResearchCenterPayloadAsync,
} from "./center-payload.js";
export {
  isSimulationRequest,
  isLiveArchiveEligible,
  filterLiveArchiveRequests,
  filterLiveActiveRequests,
  reportArtifactExists,
  canExposeViewReport,
  canExposeDownloadPdf,
  assertCompletableResearchResult,
  purgeSimulationRequestsFromIndex,
  assertKgpvPropertyIdentity,
  classifyThemeFrequency,
  mayPromoteReviewThemeToFinding,
  KGPV_PROPERTY_IDENTITY,
  THEME_FREQUENCY,
  LIVE_ARCHIVE_STATUSES,
} from "./archive-integrity.js";
export { normalizeResearchArtifact } from "./addendum-normalize.js";
export { buildResearchAddendumDossier } from "./addendum-builder.js";
export { buildChangeOpportunityAnalysis } from "./change-opportunity-analysis.js";
export {
  persistResearchAddendum,
  getPersistedResearchAddendum,
  listPersistedResearchAddendums,
} from "./addendum-store.js";
export {
  discoverProfessionalProfiles,
  enrichProfessionalProfiles,
  extractLinkedInInUrlsFromText,
  findEvidenceProfileCandidatesForPerson,
  toCustomerPersonProfileFields,
  verifyProfileCandidate,
  buildProfileDiscoveryQueries,
  assertNotFalseGabrielaMatch,
  customerProfileLink,
  isPersonLinkedInUrl,
  PROFILE_MATCH_STATUS,
} from "./profile-discovery.js";
export { assertMayStartPaidWebhoundRun, getDailySpendSnapshot } from "./spend-guard.js";
export { appendResearchAuditEvent, listResearchAuditEvents } from "./audit-log.js";
export { buildHotelIdentitySeed, buildWebhoundResearchPrompt } from "./hotel-seed.js";
export {
  findStartArtifactSession,
  detectResearchOrphans,
  recoverOrphanProviderRun,
  resumeIncompleteResearchPipelines,
} from "./orphan-recovery.js";
export {
  materializeResearchArchive,
  listResearchHistory,
} from "./archive-materializer.js";
