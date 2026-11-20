/**
 * Phase 1C / PR-2 orchestrator — recommendation + coverage + validation + YOTEL review.
 */

import { loadPropertyProfile } from "../data-model.js";
import { buildAttributeRecommendationPackage } from "./adp-attribute-recommendation-engine-v1.js";
import { reviewYotelAirportAttributes } from "./adp-yotel-airport-attribute-review-v1.js";
import { STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL } from "./adp-strategic-selection-airtable-contract-v1.js";
import { PILOT_HOTELS, REGRESSION_HOTELS } from "./adp-verified-context-bridge-v1.js";

export const ADP_ATTRIBUTE_SELECTION_GOVERNANCE_V1 =
  "ADP_ATTRIBUTE_SELECTION_GOVERNANCE_V1";

export function buildHotelSelectionGovernanceReport(propertyId, opts = {}) {
  const profile = opts.profile || loadPropertyProfile(propertyId);
  if (!profile) {
    return { ok: false, error: "property_profile_missing", propertyId };
  }
  const pack = buildAttributeRecommendationPackage(profile, opts);
  return {
    ok: true,
    contractVersion: ADP_ATTRIBUTE_SELECTION_GOVERNANCE_V1,
    propertyId,
    coverageStatus: pack.coverage.status,
    uniqueEligibleConceptCount: pack.uniqueEligibleConceptCount,
    selectedRawCount: pack.selectedRawCount,
    shortlistCount: pack.proposedShortlistMax20.count,
    proposedExclusions: pack.proposedShortlistMax20.proposedExclusions,
    topRecommendations: pack.recommendations.slice(0, 25),
    shortlist: pack.proposedShortlistMax20,
    researchNeededQueue: pack.researchNeededQueue.slice(0, 20),
    validationSummary: pack.validationSummary,
    semanticCollapse: pack.semanticCollapse,
    selectionPreserved: pack.selectionPreserved,
    package: pack,
  };
}

export function buildPhase1cGovernanceSuite(opts = {}) {
  const pilots = PILOT_HOTELS.map((p) => buildHotelSelectionGovernanceReport(p.propertyId, opts));
  const regression = REGRESSION_HOTELS.map((p) =>
    buildHotelSelectionGovernanceReport(p.propertyId, opts)
  );
  const yotelReview = reviewYotelAirportAttributes();

  // Cross-hotel isolation
  const isolation = [];
  for (let i = 0; i < pilots.length; i++) {
    for (let j = i + 1; j < pilots.length; j++) {
      const a = pilots[i];
      const b = pilots[j];
      isolation.push({
        a: a.propertyId,
        b: b.propertyId,
        ok: a.propertyId !== b.propertyId,
        selectionsEqual:
          JSON.stringify(a.selectionPreserved) === JSON.stringify(b.selectionPreserved),
      });
    }
  }

  return {
    contractVersion: ADP_ATTRIBUTE_SELECTION_GOVERNANCE_V1,
    suite: "PHASE_1C_PR2_ATTRIBUTE_SELECTION_GOVERNANCE",
    pilots,
    regression,
    yotelAirportReview: yotelReview,
    isolation,
    airtableContract: STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL,
    methodologyCompatibility: {
      customerFacingRealityGapUnchanged: true,
      historicalPeriodsImmutable: true,
      weightingUnchanged: true,
      productionResearchBlocking: false,
      airtableWrites: false,
    },
    generatedAt: new Date().toISOString(),
  };
}
