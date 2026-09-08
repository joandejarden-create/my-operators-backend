export {
  DOSSIER_SCHEMA_VERSION,
  createEmptyDossier,
  refreshDossierCounts,
  validateDossier,
  toLibraryCard,
  DOSSIER_TYPE,
  DOSSIER_TYPE_RESEARCH_ADDENDUM,
  DOSSIER_TITLE,
  DOSSIER_TITLE_RESEARCH_ADDENDUM,
  DOSSIER_SECTION_IDS,
  DOSSIER_SECTION_TITLES,
  DOSSIER_STATUSES,
  FINDING_STATUSES,
  FINDING_STATUS_LABELS,
} from "./schema.js";

export {
  listDossiersForHotel,
  getDossierById,
  getDossierForHotelRecord,
  rebuildKgpvDossierFixture,
  clearDossierRegistryCache,
  isRenderableDossier,
} from "./registry.js";

export {
  adaptKgpvDeepResearchToDossier,
  loadAndAdaptKgpvDossier,
  buildClaimCandidatesFromDeep,
  adaptNativeResearchDossierMeta,
} from "./adapters/from-kgpv-deep-research.js";

export {
  adaptWebhoundModulesToDossier,
  loadAndAdaptKgpvModulesDossier,
  markdownToBlocks,
} from "./adapters/from-webhound-kgpv-modules.js";

export {
  adaptCambridgeWebhoundToDossier,
  loadAndAdaptCambridgeFullDossier,
  CAMBRIDGE_DOSSIER_ID_V3,
  CAMBRIDGE_DOSSIER_ID_V2,
  CAMBRIDGE_DOSSIER_ID_V1,
  CAMBRIDGE_AIRTABLE_ID,
  CAMBRIDGE_DHL,
} from "./adapters/from-webhound-cambridge-full.js";

export {
  validateClientSafeReport,
  normalizeResearchSources,
  prepareCustomerProse,
  customerRelationshipLabels,
} from "./client-safe/index.js";

export {
  evaluateFullInvestigationQuality,
  assertReportHotelMatch,
  FULL_HI_REQUIRED_CHAPTER_IDS,
} from "./full-investigation-quality.js";

export {
  validateIntelligenceReport,
  evaluateResearchAddendumQuality,
  customerReportValidationMessage,
  REPORT_CONTRACTS,
  CHANGE_OPPORTUNITY_REQUIRED_SECTIONS,
  GENERIC_ADDENDUM_REQUIRED_SECTIONS,
} from "./report-contracts.js";

export {
  resolveReportHotelIdentity,
  enrichReportForPublishing,
  assertReportLocationCanonical,
  LOCATION_NOT_AVAILABLE,
} from "./report-hotel-identity.js";

export {
  buildReportDownloadFilename,
  resolveReportFilenameSlug,
  REPORT_FILENAME_SLUGS,
  contentDispositionAttachment,
  slugifyReportToken,
} from "./report-filename.js";

export { buildReportCoverMetadata } from "./report-cover-metadata.js";

export {
  bindFindingFields,
  bindFindingsList,
  classifyFindingTheme,
  assertFindingRationaleBoundById,
  FINDING_THEME_WHY,
} from "./finding-binding.js";

export {
  buildDiligenceItems,
  openQuestionsView,
  verifyNextView,
} from "./diligence-questions.js";
