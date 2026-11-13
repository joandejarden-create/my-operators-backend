/**
 * GDI Customer Publication Package — public exports.
 */

export {
  GDI_CUSTOMER_PUBLICATION_PACKAGE_V1,
  GDI_PUBLICATION_PRESENTATION_SCHEMA_V1,
  FINGERPRINT_EXCLUDED_OPPORTUNITY_KEYS,
  PUBLICATION_CONTENT_FIELDS,
  computePublicationInventoryCounts,
  fingerprintPublicationContent,
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  publicationPackageServeMeta,
} from "./gdi-customer-publication-package-v1.js";

export {
  GDI_PUBLICATION_LIFECYCLE_V1,
  GDI_PUBLICATION_LIFECYCLE_STATE,
  GDI_PUBLICATION_LIFECYCLE_ORDER,
  MANDATORY_PUBLICATION_GATES,
  lifecycleIndex,
  canAdvanceLifecycle,
  validateFounderApprovalReceipt,
  assertPublicationGatesPassed,
  isCustomerVisibleLifecycleState,
} from "./gdi-publication-lifecycle-v1.js";

export {
  GDI_PUBLICATION_QUALITY_GATE_V1,
  auditOpportunityPresentationQuality,
  runPublicationQualityGate,
  remediateDuplicateOrgAssociationSentence,
  stripEmptyParentheses,
  dedupeTileClassificationSourcing,
} from "./gdi-publication-quality-gate-v1.js";

export {
  GDI_PUBLICATION_PARITY_COMPARATOR_V1,
  comparePublicationInventory,
  comparePublicationContent,
  compareRenderedCustomerText,
  comparePublicationParity,
} from "./gdi-publication-parity-comparator-v1.js";

export {
  GDI_PUBLICATION_PACKAGE_STORE_V1,
  resolvePublicationPackageRoot,
  createGdiPublicationPackageStore,
} from "./gdi-publication-package-store-v1.js";

export {
  GDI_SHARE_APPROVED_PACKAGE_RESOLVER_V1,
  isApprovedPublicationPackageRequired,
  resolveShareOpportunitiesFromApprovedPackage,
  resolvePreviewOpportunitiesFromPublicationPackage,
  mergeShareFeedbackOntoPackageOpportunities,
} from "./gdi-share-approved-package-resolver-v1.js";

export {
  GDI_CUSTOMER_FACING_CONTENT_REMEDIATION_V1,
  applyCustomerFacingContentRemediation,
  rewritePlaceholderUncertainty,
  stripDuplicatedSourcingBoilerplate,
  softenUnsupportedConfirmedRoomClaims,
  alignSalesActionWithGuidance,
} from "./gdi-customer-facing-content-remediation-v1.js";
