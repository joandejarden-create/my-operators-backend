#!/usr/bin/env node
/**
 * Permanent regressions: HIF V7 no-RFP policy + independent commercial dimensions.
 */
import assert from "node:assert/strict";
import {
  HIF_V7_COMMERCIAL_DIMENSIONS,
  EVIDENCE_PATHWAY,
  COMMERCIAL_RELEVANCE,
  COMMERCIAL_ACCESSIBILITY,
  COMMERCIAL_TIMING,
  requiresPublicRfp,
  classifyCommercialRelevance,
  classifyCommercialAccessibility,
  qualifiesCommerciallyRelevantFirst100,
  qualifiesCommerciallyAccessible,
  qualifiesOutreachReady,
  scoreCommercialDimensions,
  isCommerciallyRelevant,
  isCommerciallyAccessible,
} from "../lib/hotel-intelligence/sec-edgar/hif-v7-commercial-dimensions-v7.js";
import { COMMERCIAL_CATEGORY } from "../lib/hotel-intelligence/sec-edgar/hif-v7-procurement-engagement-v7.js";
import { DECISION_WINDOW_KIND } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { GEO_CLASS } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";

assert.equal(HIF_V7_COMMERCIAL_DIMENSIONS, "hif-v7-commercial-dimensions-v7");
assert.equal(requiresPublicRfp(EVIDENCE_PATHWAY.PUBLIC_COMPETITIVE_PROCESS), false);
assert.equal(requiresPublicRfp(EVIDENCE_PATHWAY.OWNER_LED_PRIVATE_DECISION), false);

// 1. Verified private owner-led brand review qualifies without public RFP
{
  const row = {
    hotelName: "Lakeville Inn",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET,
    bct: true,
    evidencePathway: EVIDENCE_PATHWAY.OWNER_LED_PRIVATE_DECISION,
    ownerLedBrandReview: true,
    brandOrManagementDecisionEvidenced: true,
    commercialTiming: COMMERCIAL_TIMING.M0_6,
    monthsToDecision: 2,
    decisionPartyIdentified: true,
    ownerOrContractingParty: "Elevate Hospitality / JAMP Hotels",
    engagementRouteProbable: true,
  };
  const gate = qualifiesCommerciallyRelevantFirst100(row);
  assert.equal(gate.qualifies, true, "private owner-led brand review must qualify without RFP");
  assert.equal(gate.requiresPublicRfp, false);
  assert.equal(row.procurementStatus, undefined);
}

// 2. Verified upcoming management decision qualifies without public tender
{
  const row = {
    hotelName: "Example Managed Hotel",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET,
    mct: true,
    evidencePathway: EVIDENCE_PATHWAY.OWNER_LED_PRIVATE_DECISION,
    ownerLedOperatorReview: true,
    brandOrManagementDecisionEvidenced: true,
    commercialTiming: COMMERCIAL_TIMING.M6_12,
    monthsToDecision: 8,
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(row).qualifies, true);
}

// 3. Public RFP does not automatically qualify unrelated redevelopment
{
  const row = {
    hotelName: "Kenricia Hotel",
    physicalCountry: "CA",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.HOTEL_REDEVELOPMENT,
    evidencePathway: EVIDENCE_PATHWAY.PUBLIC_COMPETITIVE_PROCESS,
    openProcessVerified: true,
    procurementStatus: "OPEN_FOR_SUBMISSIONS",
    commercialTiming: COMMERCIAL_TIMING.ACTIVE_DECISION,
  };
  const rel = classifyCommercialRelevance(row);
  assert.equal(rel.principalBctMct, false);
  assert.equal(qualifiesCommerciallyRelevantFirst100(row).qualifies, false);
}

// 4. Hotel acquisition alone does not qualify as BCT/MCT
{
  const row = {
    hotelName: "Acquired Downtown Hotel",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET,
    hotelAcquisitionOnly: true,
    linkedBrandOrOperatorDecision: false,
    commercialTiming: COMMERCIAL_TIMING.M0_6,
  };
  assert.equal(classifyCommercialRelevance(row).commercialRelevance, COMMERCIAL_RELEVANCE.NOT_RELEVANT);
  assert.equal(qualifiesCommerciallyRelevantFirst100(row).qualifies, false);
}

// 5. Unknown owner/contact does not automatically reject commercially relevant opportunity
{
  const row = {
    hotelName: "Private Review Hotel Alpha",
    physicalCountry: "GB",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET,
    mct: true,
    ownerLedOperatorReview: true,
    brandOrManagementDecisionEvidenced: true,
    commercialTiming: COMMERCIAL_TIMING.M6_12,
    monthsToDecision: 9,
    // no ownerOrContractingParty
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(row).qualifies, true);
  const access = classifyCommercialAccessibility(row);
  assert.equal(access.commercialAccessibility, COMMERCIAL_ACCESSIBILITY.ACCESS_ROUTE_RESEARCH_REQUIRED);
}

// 6. Unknown engagement route is not classified as inaccessible/closed
{
  const access = classifyCommercialAccessibility({
    hotelName: "X",
    decisionPartyIdentified: false,
  });
  assert.notEqual(access.commercialAccessibility, COMMERCIAL_ACCESSIBILITY.ACCESS_CLOSED);
  assert.equal(access.commercialAccessibility, COMMERCIAL_ACCESSIBILITY.ACCESS_ROUTE_RESEARCH_REQUIRED);
}

// 7. Completed private deal does not count as open commercial decision
{
  const row = {
    hotelName: "Hotel Pommier",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET,
    bct: true,
    decisionCompleted: true,
    brandOrManagementDecisionEvidenced: true,
    commercialTiming: COMMERCIAL_TIMING.M0_6,
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(row).qualifies, false);
  assert.equal(classifyCommercialRelevance(row).commercialRelevance, COMMERCIAL_RELEVANCE.NOT_RELEVANT);
}

// 8. Expiration-only proxy remains insufficient for Priority A / first-100
{
  const row = {
    hotelName: "Expiration Proxy Inn",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET,
    bct: true,
    decisionWindowKind: DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
    monthsToDecision: 6,
    commercialTiming: COMMERCIAL_TIMING.M6_12,
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(row).qualifies, false);
  assert.equal(
    classifyCommercialRelevance(row).commercialRelevance,
    COMMERCIAL_RELEVANCE.INSUFFICIENT_EVIDENCE,
  );
}

// 9. Country and timing reporting preserves all verified countries
{
  const scored = [
    scoreCommercialDimensions({
      hotelName: "US Hotel",
      physicalCountry: "US",
      geographicClass: GEO_CLASS.VERIFIED,
      commercialCategory: COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET,
      bct: true,
      ownerLedBrandReview: true,
      brandOrManagementDecisionEvidenced: true,
      commercialTiming: COMMERCIAL_TIMING.M0_6,
    }),
    scoreCommercialDimensions({
      hotelName: "SG Preopening",
      physicalCountry: "SG",
      geographicClass: GEO_CLASS.VERIFIED,
      commercialCategory: COMMERCIAL_CATEGORY.HOTEL_DEVELOPMENT,
      brandOrManagementDecisionEvidenced: true,
      commercialTiming: COMMERCIAL_TIMING.M6_12,
    }),
  ];
  const countries = new Set(scored.map((s) => s.physicalCountry));
  assert.ok(countries.has("US"));
  assert.ok(countries.has("SG"));
}

// 10. Commercially relevant and commercially accessible counts remain distinct
{
  const relevantUnknownAccess = {
    hotelName: "Relevant Unknown Access Hotel",
    physicalCountry: "MX",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.BRAND_CONVERSION_TARGET,
    bct: true,
    ownerLedBrandReview: true,
    brandOrManagementDecisionEvidenced: true,
    commercialTiming: COMMERCIAL_TIMING.M6_12,
    monthsToDecision: 7,
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(relevantUnknownAccess).qualifies, true);
  assert.equal(qualifiesCommerciallyAccessible(relevantUnknownAccess).qualifies, false);
  assert.equal(qualifiesOutreachReady(relevantUnknownAccess).qualifies, false);
}

// 11. First-100 milestone counting follows strict evidence rules (adjacent excluded)
{
  const adjacent = {
    hotelName: "Aena Recife Airport Hotel Site",
    physicalCountry: "BR",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.HOTEL_DEVELOPMENT,
    openProcessVerified: true,
    commercialTiming: COMMERCIAL_TIMING.ACTIVE_DECISION,
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(adjacent).qualifies, false);
}

// 12. Worldwide eligibility remains unrestricted
{
  const za = {
    hotelName: "Worldwide Eligible Hotel ZA",
    physicalCountry: "ZA",
    geographicClass: GEO_CLASS.VERIFIED,
    commercialCategory: COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET,
    mct: true,
    ownerLedOperatorReview: true,
    brandOrManagementDecisionEvidenced: true,
    commercialTiming: COMMERCIAL_TIMING.M0_6,
    monthsToDecision: 3,
  };
  assert.equal(qualifiesCommerciallyRelevantFirst100(za).qualifies, true);
}

assert.ok(isCommerciallyRelevant(COMMERCIAL_RELEVANCE.VERIFIED_RELEVANT));
assert.ok(isCommerciallyAccessible(COMMERCIAL_ACCESSIBILITY.ACCESSIBLE_PROBABLE));
assert.equal(isCommerciallyAccessible(COMMERCIAL_ACCESSIBILITY.ACCESS_ROUTE_RESEARCH_REQUIRED), false);

console.log("PASS test-hif-v7-commercial-dimensions-v7 (12 regressions)");
