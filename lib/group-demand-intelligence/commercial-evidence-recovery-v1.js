/**
 * GDI 2.0 Phase 2D — combined offline commercial evidence recovery.
 * Applies demand + buyer overlays for evaluation only — never writes storage
 * and never flips hotelFacingEligible / customer visibility.
 */

import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import {
  recoverDemandThesis,
  applyDemandThesisRecoveryOverlay,
} from "./demand-thesis-recovery-v1.js";
import {
  recoverBuyerResponsibility,
  applyBuyerResponsibilityRecoveryOverlay,
} from "./buyer-responsibility-recovery-v1.js";
import { redactOpportunityForShadowExport } from "./gdi2-shadow-publication-evaluation-v1.js";

export const COMMERCIAL_EVIDENCE_RECOVERY_V1 =
  "gdi-commercial-evidence-recovery-v1";

export const RECOVERY_CATEGORY = Object.freeze({
  A_EXISTING_EVIDENCE_RECOVERED: "A_EXISTING_EVIDENCE_RECOVERED",
  B_DETERMINISTIC_MAPPING_CORRECTED: "B_DETERMINISTIC_MAPPING_CORRECTED",
  C_NEW_RESEARCH_REQUIRED: "C_NEW_RESEARCH_REQUIRED",
  D_COMMERCIALLY_UNSUPPORTED: "D_COMMERCIALLY_UNSUPPORTED",
  E_UNABLE_TO_EVALUATE_MISSING_SOURCE_MATERIAL:
    "E_UNABLE_TO_EVALUATE_MISSING_SOURCE_MATERIAL",
  A_ALREADY_SUPPORTED: "A_ALREADY_SUPPORTED",
});

function pickDominantCategory(demandCat, buyerCat, beforeReady, afterReady) {
  if (beforeReady) return RECOVERY_CATEGORY.A_ALREADY_SUPPORTED;
  if (!afterReady) {
    const order = [
      RECOVERY_CATEGORY.E_UNABLE_TO_EVALUATE_MISSING_SOURCE_MATERIAL,
      RECOVERY_CATEGORY.D_COMMERCIALLY_UNSUPPORTED,
      RECOVERY_CATEGORY.C_NEW_RESEARCH_REQUIRED,
      RECOVERY_CATEGORY.B_DETERMINISTIC_MAPPING_CORRECTED,
      RECOVERY_CATEGORY.A_EXISTING_EVIDENCE_RECOVERED,
    ];
    for (const c of order) {
      if (demandCat === c || buyerCat === c) return c;
    }
    return RECOVERY_CATEGORY.C_NEW_RESEARCH_REQUIRED;
  }
  // Became sales-ready via recovery/mapping
  if (
    demandCat === RECOVERY_CATEGORY.B_DETERMINISTIC_MAPPING_CORRECTED ||
    buyerCat === RECOVERY_CATEGORY.B_DETERMINISTIC_MAPPING_CORRECTED
  ) {
    return RECOVERY_CATEGORY.B_DETERMINISTIC_MAPPING_CORRECTED;
  }
  return RECOVERY_CATEGORY.A_EXISTING_EVIDENCE_RECOVERED;
}

/**
 * Recover commercial evidence overlays and re-evaluate shared eligibility.
 */
export function recoverCommercialEvidence(opportunity = {}, opts = {}) {
  const before = evaluateSharedCommercialEligibility(opportunity, opts);
  const demand = recoverDemandThesis(opportunity, opts);
  const buyer = recoverBuyerResponsibility(opportunity);

  let working = { ...opportunity };
  const demandApply = applyDemandThesisRecoveryOverlay(working, demand);
  working = demandApply.opportunity;
  const buyerApply = applyBuyerResponsibilityRecoveryOverlay(working, buyer);
  working = buyerApply.opportunity;

  const after = evaluateSharedCommercialEligibility(working, opts);

  const category = pickDominantCategory(
    demand.category,
    buyer.category,
    before.salesReady === true,
    after.salesReady === true,
  );

  const evidenceBackedNewSalesReady =
    before.salesReady !== true &&
    after.salesReady === true &&
    (category === RECOVERY_CATEGORY.A_EXISTING_EVIDENCE_RECOVERED ||
      category === RECOVERY_CATEGORY.B_DETERMINISTIC_MAPPING_CORRECTED);

  return {
    version: COMMERCIAL_EVIDENCE_RECOVERY_V1,
    offlineOnly: true,
    doesNotMutateStorage: true,
    doesNotChangeCustomerVisibility: true,
    doesNotChangeHotelFacingEligible: true,
    doesNotAutoPromote: true,
    opportunityRef: redactOpportunityForShadowExport(opportunity),
    before: {
      status: before.status,
      salesReady: before.salesReady === true,
      gaps: before.gaps || [],
    },
    after: {
      status: after.status,
      salesReady: after.salesReady === true,
      gaps: after.gaps || [],
    },
    demand,
    buyer,
    overlaysApplied: {
      demand: demandApply.applied === true,
      buyer: buyerApply.applied === true,
    },
    recoveredOpportunity: working,
    category,
    evidenceBackedNewSalesReady,
    falseQualificationRisk:
      after.salesReady === true &&
      (demand.contradictoryEvidence || []).length > 0,
    resolved: {
      demandThesis:
        before.gaps?.includes("MISSING_HOTEL_DEMAND_THESIS") &&
        !after.gaps?.includes("MISSING_HOTEL_DEMAND_THESIS"),
      buyerResponsibility:
        before.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN") &&
        !after.gaps?.includes("BUYER_RESPONSIBILITY_UNKNOWN"),
      namedBuyer:
        before.gaps?.includes("MISSING_NAMED_RELEVANT_BUYER") &&
        !after.gaps?.includes("MISSING_NAMED_RELEVANT_BUYER"),
      contactRoute:
        before.gaps?.includes("MISSING_CONTACT_ROUTE") &&
        !after.gaps?.includes("MISSING_CONTACT_ROUTE"),
    },
  };
}
