/**
 * GDI 2.0 publication decision model (Phase 2A).
 *
 * Separates three decisions that must never be conflated:
 *   A. commercialEligibility  — sales-ready commercial substance
 *   B. presentationClassification — how the row may be labeled/presented
 *   C. publicationAuthorization — founder/CLI allowlist to promote externally
 *
 * Visibility ≠ eligibility. Authorization ≠ eligibility.
 * Default: does not change customer-facing boolean visibility.
 */

import {
  evaluateSharedCommercialEligibility,
  COMMERCIAL_ELIGIBILITY_STATUS,
} from "./commercial-eligibility-contract-v1.js";
import {
  isCustomerFacingOpportunity,
  isGdiTestOrFixtureOpportunity,
  isGeneratorOnlyCustomerRecord,
} from "./customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";
import { isWRomePilotQualifiedCustomerVisible } from "./expansion-pilot/w-rome-pilot-visibility-v0.js";
import { isReviewedProspectCustomerVisible } from "./reviewed-prospect-publication-v1.js";
import { isInventoryCustomerVisible } from "./inventory-visibility-v1.js";
import { isGdiQualifiedCustomerVisibilityV1Enabled } from "./feature-flag.js";

export const PUBLICATION_DECISION_MODEL_V1 = "gdi-publication-decision-model-v1";

export const PRESENTATION_CLASSIFICATION = Object.freeze({
  SALES_READY: "SALES_READY",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  INFORMATIONAL_INVENTORY: "INFORMATIONAL_INVENTORY",
  HISTORICAL: "HISTORICAL",
  INTERNAL_ONLY: "INTERNAL_ONLY",
});

export const VISIBILITY_BASIS = Object.freeze({
  STRICT_READY: "STRICT_READY",
  REVIEWED_PROSPECT_PACKAGE: "REVIEWED_PROSPECT_PACKAGE",
  LEGACY_COMPATIBILITY: "LEGACY_COMPATIBILITY",
  INVENTORY_VISIBILITY: "INVENTORY_VISIBILITY",
  W_ROME_PILOT: "W_ROME_PILOT",
  QUALIFIED_MATURITY_FLAG: "QUALIFIED_MATURITY_FLAG",
  SURFACE_ONLY_ESCAPE: "SURFACE_ONLY_ESCAPE",
  INCLUDE_HISTORICAL: "INCLUDE_HISTORICAL",
  NOT_CUSTOMER_VISIBLE: "NOT_CUSTOMER_VISIBLE",
});

/**
 * Explain *why* an opportunity is customer-visible without treating visibility as eligibility.
 */
export function explainCustomerVisibilityBasis(opportunity = {}, opts = {}) {
  const env = opts.env || process.env;
  if (isGeneratorOnlyCustomerRecord(opportunity) || opportunity.isTestData === true) {
    return {
      customerVisible: false,
      basis: VISIBILITY_BASIS.NOT_CUSTOMER_VISIBLE,
      note: "generator_or_test",
    };
  }
  if (isWRomePilotQualifiedCustomerVisible(opportunity, env)) {
    return {
      customerVisible: true,
      basis: VISIBILITY_BASIS.W_ROME_PILOT,
      impliesCommercialEligibility: false,
    };
  }
  if (isReviewedProspectCustomerVisible(opportunity, opts)) {
    return {
      customerVisible: true,
      basis: VISIBILITY_BASIS.REVIEWED_PROSPECT_PACKAGE,
      impliesCommercialEligibility: false,
    };
  }
  if (isInventoryCustomerVisible(opportunity, opts)) {
    return {
      customerVisible: true,
      basis: VISIBILITY_BASIS.INVENTORY_VISIBILITY,
      impliesCommercialEligibility: false,
    };
  }
  if (opts.surfaceOnly === true) {
    return {
      customerVisible: isCustomerFacingOpportunity(opportunity, opts),
      basis: VISIBILITY_BASIS.SURFACE_ONLY_ESCAPE,
      impliesCommercialEligibility: false,
    };
  }
  if (opts.includeHistorical === true) {
    return {
      customerVisible: isCustomerFacingOpportunity(opportunity, opts),
      basis: VISIBILITY_BASIS.INCLUDE_HISTORICAL,
      impliesCommercialEligibility: false,
    };
  }
  const ready = isGdiCustomerOpportunityReady(opportunity, opts);
  if (ready.ok) {
    return {
      customerVisible: true,
      basis: VISIBILITY_BASIS.STRICT_READY,
      impliesCommercialEligibility: null, // still evaluate shared contract separately
      ready,
    };
  }
  if (isLegacyVisibilityCompatible(opportunity)) {
    return {
      customerVisible: true,
      basis: VISIBILITY_BASIS.LEGACY_COMPATIBILITY,
      impliesCommercialEligibility: false,
    };
  }
  if (
    isGdiQualifiedCustomerVisibilityV1Enabled(env) &&
    String(opportunity.gdiMaturityState || "") === "QUALIFIED" &&
    !isGdiTestOrFixtureOpportunity(opportunity)
  ) {
    return {
      customerVisible: true,
      basis: VISIBILITY_BASIS.QUALIFIED_MATURITY_FLAG,
      impliesCommercialEligibility: false,
    };
  }
  return {
    customerVisible: false,
    basis: VISIBILITY_BASIS.NOT_CUSTOMER_VISIBLE,
    impliesCommercialEligibility: false,
  };
}

function mapPresentationClassification({
  commercial,
  visibility,
  opts = {},
}) {
  if (opts.includeHistorical === true) {
    return PRESENTATION_CLASSIFICATION.HISTORICAL;
  }
  if (commercial.status === COMMERCIAL_ELIGIBILITY_STATUS.SALES_READY) {
    return PRESENTATION_CLASSIFICATION.SALES_READY;
  }
  if (commercial.researchContinuation) {
    // Visible via inventory/legacy/pilot still ≠ sales-ready
    if (
      visibility.basis === VISIBILITY_BASIS.INVENTORY_VISIBILITY ||
      visibility.basis === VISIBILITY_BASIS.W_ROME_PILOT
    ) {
      return PRESENTATION_CLASSIFICATION.INFORMATIONAL_INVENTORY;
    }
    if (visibility.basis === VISIBILITY_BASIS.LEGACY_COMPATIBILITY) {
      return PRESENTATION_CLASSIFICATION.HISTORICAL;
    }
    if (visibility.basis === VISIBILITY_BASIS.REVIEWED_PROSPECT_PACKAGE) {
      // Reviewed package may be customer-visible without shared sales-ready
      return PRESENTATION_CLASSIFICATION.INFORMATIONAL_INVENTORY;
    }
    return PRESENTATION_CLASSIFICATION.RESEARCH_CONTINUATION;
  }
  if (visibility.customerVisible) {
    return PRESENTATION_CLASSIFICATION.INFORMATIONAL_INVENTORY;
  }
  return PRESENTATION_CLASSIFICATION.INTERNAL_ONLY;
}

/**
 * Publication authorization is founder/CLI controlled — never inferred from eligibility alone.
 */
export function evaluatePublicationAuthorization(opportunity = {}, opts = {}) {
  const authorized =
    opts.authorizePublication === true ||
    opportunity.publicationAuthorized === true ||
    opportunity.founderPublicationAuthorized === true;
  return {
    authorized: Boolean(authorized),
    requiresExplicitFounderAuth: true,
    commercialEligibilityDoesNotAuthorize: true,
    visibilityDoesNotAuthorize: true,
    basis: authorized
      ? opts.authorizationBasis || "explicit_authorize_publication"
      : "not_authorized",
    note:
      "Sales-ready commercial eligibility is necessary but not sufficient for publication.",
  };
}

/**
 * Full A/B/C decision. Preserves existing visibility boolean via isCustomerFacingOpportunity.
 */
export function evaluatePublicationDecision(opportunity = {}, opts = {}) {
  const commercial = evaluateSharedCommercialEligibility(opportunity, opts);
  const visibilityExplained = explainCustomerVisibilityBasis(opportunity, opts);
  const customerVisible = isCustomerFacingOpportunity(opportunity, opts);
  const publicationAuthorization = evaluatePublicationAuthorization(
    opportunity,
    opts,
  );
  const presentationClassification = mapPresentationClassification({
    commercial,
    visibility: visibilityExplained,
    opts,
  });

  // Guard: never invent new customer exposure from shared eligibility alone
  const wouldWeakenVisibility =
    commercial.salesReady === true &&
    customerVisible === false &&
    opts.allowEligibilityToGrantVisibility === true;

  return {
    version: PUBLICATION_DECISION_MODEL_V1,
    commercialEligibility: commercial,
    presentationClassification,
    publicationAuthorization,
    customerVisible,
    visibilityBasis: visibilityExplained.basis,
    visibilityExplained,
    invariants: {
      visibilityIsNotEligibility: true,
      authorizationIsNotEligibility: true,
      eligibilityDoesNotGrantVisibilityByDefault: !wouldWeakenVisibility,
      noNewCustomerExposureFromWeakerPaths: true,
    },
    promoteExternally:
      commercial.salesReady === true &&
      publicationAuthorization.authorized === true &&
      customerVisible === true,
  };
}

/**
 * Shadow annotation helper — attaches decision fields without mutating visibility semantics.
 */
export function attachPublicationDecisionShadow(opportunity = {}, opts = {}) {
  const decision = evaluatePublicationDecision(opportunity, opts);
  return {
    ...opportunity,
    gdi2PublicationDecisionV1: {
      presentationClassification: decision.presentationClassification,
      commercialStatus: decision.commercialEligibility.status,
      commercialEligible: decision.commercialEligibility.commercialEligible,
      visibilityBasis: decision.visibilityBasis,
      publicationAuthorized: decision.publicationAuthorization.authorized,
      gaps: decision.commercialEligibility.gaps,
      nextActions: decision.commercialEligibility.nextActions,
      shadowOnly: true,
      doesNotChangeCustomerVisibility: true,
    },
  };
}
