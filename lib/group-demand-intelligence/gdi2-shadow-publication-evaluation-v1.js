/**
 * GDI 2.0 Phase 2B — shadow publication evaluation.
 *
 * Compares existing FullE2E eligibility vs shared commercial contract + decision model.
 * NEVER changes publication selections, customer visibility, persisted records, or shares.
 * NEVER invokes paid research.
 */

import {
  evaluateSharedCommercialEligibility,
  COMMERCIAL_ELIGIBILITY_STATUS,
  ELIGIBILITY_REASON,
} from "./commercial-eligibility-contract-v1.js";
import {
  evaluatePublicationDecision,
  PRESENTATION_CLASSIFICATION,
  VISIBILITY_BASIS,
} from "./publication-decision-model-v1.js";
import { classifyResearchContinuationDisposition } from "./research-continuation-disposition-v1.js";

export const GDI2_SHADOW_PUBLICATION_EVALUATION_V1 =
  "gdi-2-shadow-publication-evaluation-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Redact person-identifying contact fields for review exports.
 */
export function redactOpportunityForShadowExport(opportunity = {}) {
  return {
    id: opportunity.id || opportunity.opportunityId || null,
    hotelId: opportunity.hotelId || null,
    organizationName: text(opportunity.organizationName || opportunity.company || "") || null,
    researchSegment: opportunity.researchSegment || opportunity.segment || null,
    eventName: opportunity.eventName || null,
    opportunityType: opportunity.opportunityType || null,
    hasNamedContact: Boolean(
      text(opportunity.contactName) ||
        text(opportunity.primaryContact?.name) ||
        text(opportunity.primaryContactName),
    ),
    hasContactRoute: Boolean(
      text(opportunity.contactEmail) ||
        text(opportunity.primaryContact?.email) ||
        text(opportunity.organizationContactEmail) ||
        text(opportunity.functionalContactEmail) ||
        text(opportunity.contactUrl),
    ),
    hasBuyerRole: Boolean(
      text(
        opportunity.primaryContact?.role ||
          opportunity.primaryContact?.title ||
          opportunity.contactResponsibility ||
          opportunity.contactRole ||
          opportunity.buyerRole ||
          "",
      ),
    ),
  };
}

function inferDisagreementRootCauses({
  code,
  shared,
  existingEligibility,
  visibilityBasis,
}) {
  const causes = [];
  const gaps = shared?.gaps || [];
  if (code === "EXISTING_READY_SHARED_NOT") {
    if (
      gaps.includes(ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN) ||
      shared?.buyer?.responsibility?.code ===
        ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN
    ) {
      causes.push("missing_buyer_responsibility");
    }
    if (gaps.includes(ELIGIBILITY_REASON.BUYER_ROLE_IRRELEVANT)) {
      causes.push("incorrect_or_irrelevant_role");
    }
    if (gaps.includes(ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER)) {
      causes.push("missing_or_irrelevant_named_buyer");
    }
    if (gaps.includes(ELIGIBILITY_REASON.BUYER_AFFILIATION_MISMATCH)) {
      causes.push("buyer_affiliation_uncertain");
    }
    if (gaps.includes(ELIGIBILITY_REASON.MISSING_CONTACT_ROUTE)) {
      causes.push("missing_usable_contact_route");
    }
    if (gaps.includes(ELIGIBILITY_REASON.MISSING_HOTEL_DEMAND_THESIS)) {
      causes.push("missing_hotel_specific_demand_thesis");
    }
    if (gaps.includes(ELIGIBILITY_REASON.SOURCES_INSUFFICIENT)) {
      causes.push("weak_source_provenance");
    }
    if (gaps.includes("INVALID_BUYER_CONTACT_ATTRIBUTION")) {
      causes.push("unnecessary_or_strict_direct_contact_attribution");
    }
    if (!causes.length) causes.push("shared_gate_stricter_than_existing");
  }
  if (code === "SHARED_READY_EXISTING_NOT" || code === "SHARED_READY_EXISTING_NOT_VISIBILITY_PATH") {
    if (
      visibilityBasis === VISIBILITY_BASIS.LEGACY_COMPATIBILITY ||
      visibilityBasis === VISIBILITY_BASIS.REVIEWED_PROSPECT_PACKAGE ||
      visibilityBasis === VISIBILITY_BASIS.INVENTORY_VISIBILITY ||
      visibilityBasis === VISIBILITY_BASIS.W_ROME_PILOT
    ) {
      causes.push("legacy_or_pilot_visibility_exception_path");
    }
    if (existingEligibility?.hotelFacing === false) {
      causes.push("existing_hotel_facing_false");
    }
    if (existingEligibility?.scalingPublicationQualified === false) {
      causes.push("existing_scaling_gate_false");
    }
    if (
      existingEligibility?.associationStatus &&
      existingEligibility.associationStatus !== "QUALIFIED"
    ) {
      causes.push("existing_association_not_qualified");
    }
    if (!causes.length) causes.push("existing_engine_data_shape_or_entity_fitness");
  }
  return causes;
}

function classifyDisagreement({
  existingSalesReady,
  sharedSalesReady,
  existingContinuation,
  visibilityBasis,
  shared,
  existingEligibility,
}) {
  let base;
  if (existingSalesReady && sharedSalesReady) {
    base = { code: "AGREE_SALES_READY", severity: "none" };
  } else if (!existingSalesReady && !sharedSalesReady) {
    base = {
      code: existingContinuation ? "AGREE_CONTINUATION" : "AGREE_NOT_READY",
      severity: "none",
    };
  } else if (existingSalesReady && !sharedSalesReady) {
    base = {
      code: "EXISTING_READY_SHARED_NOT",
      severity: "high",
      note:
        "Existing engine would publish/qualify; shared GDI 2.0 contract would not — investigate buyer responsibility / sources / route.",
    };
  } else if (
    visibilityBasis === VISIBILITY_BASIS.LEGACY_COMPATIBILITY ||
    visibilityBasis === VISIBILITY_BASIS.REVIEWED_PROSPECT_PACKAGE ||
    visibilityBasis === VISIBILITY_BASIS.INVENTORY_VISIBILITY ||
    visibilityBasis === VISIBILITY_BASIS.W_ROME_PILOT
  ) {
    base = {
      code: "SHARED_READY_EXISTING_NOT_VISIBILITY_PATH",
      severity: "medium",
      note: "Shared sales-ready but existing path not hotel-facing; visibility exception separate.",
    };
  } else {
    base = {
      code: "SHARED_READY_EXISTING_NOT",
      severity: "medium",
      note:
        "Shared contract passes while existing gate does not — do not auto-promote; review engine gap.",
    };
  }
  return {
    ...base,
    rootCauses: inferDisagreementRootCauses({
      code: base.code,
      shared,
      existingEligibility,
      visibilityBasis,
    }),
  };
}

function remediationFor(shared = {}, disagreement = {}) {
  const actions = [...(shared.nextActions || [])];
  if (disagreement.code === "EXISTING_READY_SHARED_NOT") {
    if (shared.gaps?.includes(ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN)) {
      actions.unshift(
        "Recover buyer title/role evidence before treating as GDI 2.0 sales-ready.",
      );
    }
    if (shared.gaps?.includes(ELIGIBILITY_REASON.SOURCES_INSUFFICIENT)) {
      actions.unshift("Attach traceable source URLs/quotes (shared contract gate).");
    }
  }
  return [...new Set(actions)];
}

/**
 * Shadow-evaluate one opportunity against existing eligibility flags.
 *
 * @param {object} opportunity
 * @param {{
 *   hotel?: object,
 *   existingSalesReady?: boolean,
 *   existingResearchContinuation?: boolean,
 *   existingEligibility?: object,
 *   segmentFamily?: string,
 *   env?: object,
 * }} [opts]
 */
export function evaluateGdi2ShadowPublication(opportunity = {}, opts = {}) {
  const shared = evaluateSharedCommercialEligibility(opportunity, {
    hotel: opts.hotel,
    segmentFamily: opts.segmentFamily,
  });
  const decision = evaluatePublicationDecision(opportunity, {
    hotel: opts.hotel,
    segmentFamily: opts.segmentFamily,
    env: opts.env,
    authorizePublication: opts.authorizePublication === true,
  });
  const continuation = classifyResearchContinuationDisposition({
    opportunity,
    hotel: opts.hotel || {},
    providerFailed: opts.providerFailed === true,
    strategyEmptyResults: opts.strategyEmptyResults === true,
    authorizedBudgetExhausted: opts.authorizedBudgetExhausted === true,
    lodgingEvidenceIncomplete: opts.lodgingEvidenceIncomplete === true,
    contactNotYetVerified: opts.contactNotYetVerified === true,
    laterMilestonePending: opts.laterMilestonePending === true,
  });

  const existingSalesReady = opts.existingSalesReady === true;
  const existingContinuation = opts.existingResearchContinuation === true;
  const existingEligibilitySummary = opts.existingEligibility
    ? {
        hotelFacing: opts.existingEligibility.hotelFacing === true,
        eligibility: opts.existingEligibility.eligibility || null,
        scalingPublicationQualified:
          opts.existingEligibility.scalingPublicationQualified === true,
        associationStatus:
          opts.existingEligibility.associationEventsQualification?.status ||
          opts.existingEligibility.publicationStatus ||
          null,
      }
    : null;

  const disagreement = classifyDisagreement({
    existingSalesReady,
    sharedSalesReady: shared.salesReady === true,
    existingContinuation,
    visibilityBasis: decision.visibilityBasis,
    shared,
    existingEligibility: existingEligibilitySummary,
  });

  return {
    version: GDI2_SHADOW_PUBLICATION_EVALUATION_V1,
    shadowOnly: true,
    doesNotChangePublicationSelection: true,
    doesNotChangeCustomerVisibility: true,
    doesNotInvokePaidResearch: true,
    doesNotPersist: true,
    opportunityRef: redactOpportunityForShadowExport(opportunity),
    existing: {
      salesReady: existingSalesReady,
      researchContinuation: existingContinuation,
      eligibilitySummary: existingEligibilitySummary,
    },
    shared: {
      status: shared.status,
      salesReady: shared.salesReady === true,
      researchContinuation: shared.researchContinuation === true,
      gaps: shared.gaps || [],
      reasons: shared.reasons || [],
      gates: shared.gates,
      buyerResponsibility: shared.buyer?.responsibility?.code || null,
      demandOk: shared.demand?.ok === true,
      routeOk: shared.route?.ok === true,
      sourcesOk: shared.sources?.ok === true,
      nextActions: shared.nextActions || [],
    },
    presentationClassification: decision.presentationClassification,
    visibilityBasis: decision.visibilityBasis,
    customerVisible: decision.customerVisible,
    publicationAuthorization: {
      authorized: decision.publicationAuthorization.authorized === true,
      basis: decision.publicationAuthorization.basis,
    },
    continuationDisposition: {
      disposition: continuation.disposition,
      commerciallyExhausted: continuation.commerciallyExhausted === true,
      preserveOpportunity: continuation.preserveOpportunity === true,
      incompleteSignals: continuation.incompleteSignals || [],
      nextActions: continuation.nextActions || [],
    },
    disagreement,
    recommendedRemediation: remediationFor(shared, disagreement),
    classifications: {
      SALES_READY: PRESENTATION_CLASSIFICATION.SALES_READY,
      RESEARCH_CONTINUATION: PRESENTATION_CLASSIFICATION.RESEARCH_CONTINUATION,
      sharedStatus: COMMERCIAL_ELIGIBILITY_STATUS,
    },
  };
}

/**
 * Attach shadow payload onto a publication candidate / opportunity wrapper.
 * Mutates nothing about eligibility booleans passed in — returns new object.
 */
export function attachGdi2ShadowEvaluation(row = {}, opts = {}) {
  const opportunity = row.opportunity || row.opportunityPayload || row;
  const existingSalesReady =
    opts.existingSalesReady ??
    (row.hotelFacingEligible === true ||
      row.scalingQualification?.publicationQualified === true ||
      row.associationEventsQualification?.qualified === true ||
      opportunity.hotelFacingEligible === true);
  const existingResearchContinuation =
    opts.existingResearchContinuation ??
    (row.researchContinuation === true ||
      row.scalingQualification?.researchContinuation === true ||
      row.associationEventsQualification?.researchContinuation === true ||
      opportunity.researchContinuation === true);

  const shadow = evaluateGdi2ShadowPublication(opportunity, {
    ...opts,
    existingSalesReady: Boolean(existingSalesReady),
    existingResearchContinuation: Boolean(existingResearchContinuation),
    existingEligibility:
      opts.existingEligibility ||
      row.eligibility ||
      row.scalingQualification ||
      row.associationEventsQualification ||
      null,
  });

  return {
    ...row,
    gdi2ShadowEvaluationV1: shadow,
  };
}

/**
 * Summarize an array of shadow evaluations (redacted counts only).
 */
export function summarizeGdi2ShadowEvaluations(shadows = []) {
  const list = (shadows || []).map((s) => s.gdi2ShadowEvaluationV1 || s);
  const counts = {
    evaluated: list.length,
    existingSalesReady: 0,
    sharedSalesReady: 0,
    agreeSalesReady: 0,
    existingReadySharedNot: 0,
    sharedReadyExistingNot: 0,
    agreeContinuation: 0,
    missingBuyerResponsibility: 0,
    missingHotelDemand: 0,
    missingContactRoute: 0,
    researchContinuationCandidates: 0,
    visibilityExceptions: 0,
  };
  const byDisagreement = {};
  const byVisibilityBasis = {};
  for (const s of list) {
    if (!s) continue;
    if (s.existing?.salesReady) counts.existingSalesReady += 1;
    if (s.shared?.salesReady) counts.sharedSalesReady += 1;
    const code = s.disagreement?.code || "UNKNOWN";
    byDisagreement[code] = (byDisagreement[code] || 0) + 1;
    if (code === "AGREE_SALES_READY") counts.agreeSalesReady += 1;
    if (code === "EXISTING_READY_SHARED_NOT") counts.existingReadySharedNot += 1;
    if (code === "SHARED_READY_EXISTING_NOT" || code === "SHARED_READY_EXISTING_NOT_VISIBILITY_PATH") {
      counts.sharedReadyExistingNot += 1;
    }
    if (code === "AGREE_CONTINUATION") counts.agreeContinuation += 1;
    if (s.shared?.gaps?.includes(ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN)) {
      counts.missingBuyerResponsibility += 1;
    }
    if (s.shared?.gaps?.includes(ELIGIBILITY_REASON.MISSING_HOTEL_DEMAND_THESIS)) {
      counts.missingHotelDemand += 1;
    }
    if (s.shared?.gaps?.includes(ELIGIBILITY_REASON.MISSING_CONTACT_ROUTE)) {
      counts.missingContactRoute += 1;
    }
    if (s.shared?.researchContinuation || s.continuationDisposition?.disposition === "RESEARCH_CONTINUATION") {
      counts.researchContinuationCandidates += 1;
    }
    const vb = s.visibilityBasis || VISIBILITY_BASIS.NOT_CUSTOMER_VISIBLE;
    byVisibilityBasis[vb] = (byVisibilityBasis[vb] || 0) + 1;
    if (
      vb === VISIBILITY_BASIS.LEGACY_COMPATIBILITY ||
      vb === VISIBILITY_BASIS.REVIEWED_PROSPECT_PACKAGE ||
      vb === VISIBILITY_BASIS.INVENTORY_VISIBILITY ||
      vb === VISIBILITY_BASIS.W_ROME_PILOT
    ) {
      counts.visibilityExceptions += 1;
    }
  }
  return {
    version: GDI2_SHADOW_PUBLICATION_EVALUATION_V1,
    note: "Fixture/local corpus counts — not certified production opportunity inventory.",
    counts,
    byDisagreement,
    byVisibilityBasis,
  };
}
