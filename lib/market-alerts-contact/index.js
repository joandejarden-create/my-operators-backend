/**
 * Market Alerts — Actionable Contact Intelligence (Phase A)
 */

export {
  inferStakeholderRoles,
  stakeholderRoleLabels,
  isBlockedGenericRole,
  STAKEHOLDER_ROLES,
  BLOCKED_GENERIC_ROLES,
} from "./stakeholder-roles.js";

export {
  assessContactEnrichmentEligibility,
  isContactEnrichmentEnabled,
  getContactEnrichmentLimits,
  hasAdequateNamedIdentityForDirectEnrich,
  resolveOrganizations,
  resolveCommercialOrganizations,
  scoreCommercialOrganization,
  extractNamedStakeholder,
  CONTACT_BLOCK_REASONS,
  COMMERCIAL_ORG_CONFIDENCE,
} from "./eligibility.js";

export {
  extractArticleStakeholders,
  resolveArticleText,
  articleClassToRoleLabel,
  ARTICLE_STAKEHOLDER_CLASS,
  DECISION_MAKER_CONFIDENCE,
  STAKEHOLDER_EXTRACTION_SOURCE,
  STAKEHOLDER_EXTRACTION_STATUS,
} from "./article-stakeholder-extract.js";

export {
  ensureArticleBodyForStakeholderExtraction,
  deriveStakeholderExtractionStatus,
  clearArticleBodyCache,
} from "./fetch-article-body.js";

export {
  classifyContactConfidence,
  shouldDisplayContactByDefault,
  CONTACT_CONFIDENCE,
} from "./confidence.js";

export {
  planContactEnrichment,
  planContactEnrichmentBatch,
  enrichAlertContacts,
  maybeEnrichAfterIngest,
  getCachedAlertContacts,
  beginProviderRunBudget,
  CONTACT_PROVIDER_CAP_REASON,
} from "./orchestrator.js";

export {
  createProviderRunBudget,
  getActiveProviderRunBudget,
  endProviderRunBudget,
} from "./provider-budget.js";

export {
  assessContactPersistenceSafety,
  getContactPersistenceMode,
  CONTACT_PERSISTENCE_MODE,
} from "./persistence.js";

export { createSurfeContactProvider } from "./surfe-adapter.js";
export { createNoopContactProvider, CONTACT_PROVIDER_ID } from "./provider-interface.js";
export { createContactRecord, toContactCard, toContactCards } from "./contact-card.js";
export { getContactCacheDir, purgeSurfePiiFromLocalCache } from "./cache.js";

export {
  identifyAlertStakeholders,
  toStakeholderUiCard,
} from "./identify-stakeholders.js";

export {
  MARKET_ALERT_STAKEHOLDERS_TABLE,
  map_marketAlertStakeholderFields,
  IDENTIFICATION_STATUS,
  CONTACT_LOOKUP_STATUS,
  stripSurfeContactDetails,
  buildStakeholderStableKey,
} from "./stakeholder-schema.js";

export {
  validateStakeholderWrite,
  upsertStakeholder,
  listStakeholdersForAlert,
  persistIdentificationResult,
} from "./stakeholder-airtable.js";

export {
  findDecisionMakerOnDemand,
  revealContactDetailsOnDemand,
} from "./on-demand.js";

export {
  LINKEDIN_RESOLUTION_STATUS,
  LINKEDIN_RESOLUTION_CONFIDENCE,
  LINKEDIN_RESOLUTION_SOURCE,
  scoreLinkedInCandidate,
  resolveLinkedInProfile,
  linkedInFieldsForPersistence,
} from "./linkedin-resolve.js";

export { withInflightGuard } from "./inflight-guard.js";