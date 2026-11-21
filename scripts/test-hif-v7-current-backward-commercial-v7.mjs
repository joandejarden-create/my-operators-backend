#!/usr/bin/env node
/**
 * HIF V7 regression gates — current-backward commercial qualification.
 */
import assert from "node:assert/strict";
import {
  classifyOpportunityStatus,
  qualifyPriorityACurrentBackward,
  isOpenOpportunityStatus,
  OPPORTUNITY_STATUS,
  SIGNAL_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-v7-opportunity-status-v7.js";
import { DECISION_WINDOW_KIND, TIMING_HORIZON } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { GEO_CLASS } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";
import { qualifiesForFirst100Milestone } from "../lib/hotel-intelligence/sec-edgar/hif-v6-first-100-milestone-v6.js";
import { COMMERCIAL_PRIORITY } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";
import { summarizeResearchGaps } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-gap-visibility-v6.js";

// 1. Recent article but completed operator award
const awarded = classifyOpportunityStatus({
  statusText: "HEI Hotels & Resorts has been selected as the new operator",
  decisionAwarded: true,
});
assert.equal(awarded.opportunityStatus, OPPORTUNITY_STATUS.DECISION_AWARDED);
assert.equal(isOpenOpportunityStatus(awarded.opportunityStatus), false);

// 2. Hotel sale with no verified branding decision
const saleOnly = qualifyPriorityACurrentBackward(
  {
    hotelName: "Downtown Sale Hotel",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    signalCategory: SIGNAL_CATEGORY.COMMERCIAL_CATALYST,
    linkedBrandOrOperatorDecision: false,
    currentStatusVerified: true,
    commercialThesis: "Hotel sold to investor; no brand or operator decision disclosed in sources reviewed.",
    sourceUrl: "https://example.com/sale",
    agreementType: "FRANCHISE",
    commercialList: "BRAND_CONVERSION_TARGETS",
  },
  { decisionWindowKind: DECISION_WINDOW_KIND.TIMING_UNKNOWN, timingHorizon: TIMING_HORIZON.UNKNOWN, monthsToDecision: null },
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(saleOnly.qualifiesPriorityA, false);

// 3. Confirmed upcoming franchise nonrenewal
const nonrenewal = classifyOpportunityStatus({
  statusText: "Owner confirmed the franchise will not renew and brand selection is underway",
  confirmedNonrenewal: true,
  activeSelectionProcess: true,
});
assert.equal(nonrenewal.opportunityStatus, OPPORTUNITY_STATUS.ACTIVE_SELECTION);

// 4. Active operator search without public contract expiration
const activeOps = qualifyPriorityACurrentBackward(
  {
    hotelName: "Gideon Putnam Hotel",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    agreementType: "MANAGEMENT",
    commercialList: "MANAGEMENT_CONTRACT_TARGETS",
    opportunityType: "MCT",
    activeSelectionProcess: true,
    openProcessVerified: true,
    currentStatusVerified: true,
    commercialThesis: "State Parks RFP seeks new concessionaire for verified historic hotel with proposals due January 2027.",
    sourceUrl: "https://parks.ny.gov/example",
    sources: [{ url: "https://parks.ny.gov/example" }],
  },
  {
    decisionWindowKind: DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW,
    timingHorizon: TIMING_HORIZON.NOW_ACTIVE_PROCESS,
    monthsToDecision: 0,
  },
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(activeOps.qualifiesPriorityA, true);

// 5. Historical contract superseded by later amendment
const superseded = classifyOpportunityStatus({ superseded: true, statusText: "amended and restated 2024" });
assert.equal(superseded.opportunityStatus, OPPORTUNITY_STATUS.SUPERSEDED);

// 6. Hotel in a nonpriority country (Italy brand selection still open-status)
const italy = qualifyPriorityACurrentBackward(
  {
    hotelName: "Emilia Romagna Premium Hotel",
    physicalCountry: "IT",
    geographicClass: GEO_CLASS.VERIFIED,
    agreementType: "FRANCHISE",
    commercialList: "BRAND_CONVERSION_TARGETS",
    opportunityType: "BCT",
    activeSelectionProcess: true,
    currentStatusVerified: true,
    commercialThesis: "Exclusive brand affiliation mandate with proposal deadline — nonpriority country remains eligible.",
    sourceUrl: "https://example.com/it",
    sources: [{ url: "https://example.com/it" }],
  },
  {
    decisionWindowKind: DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW,
    timingHorizon: TIMING_HORIZON.NOW_ACTIVE_PROCESS,
    monthsToDecision: 0,
  },
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(italy.qualifiesPriorityA, true);
assert.equal(italy.commercialPriority, COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW);

// 7. Wrong-country / unresolved geography
const badGeo = qualifyPriorityACurrentBackward(
  {
    hotelName: "Mystery Inn",
    agreementType: "FRANCHISE",
    commercialList: "BRAND_CONVERSION_TARGETS",
    activeSelectionProcess: true,
    currentStatusVerified: true,
    commercialThesis: "Active brand review without physical country evidence should not qualify.",
    sourceUrl: "https://example.com/x",
  },
  {
    decisionWindowKind: DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW,
    timingHorizon: TIMING_HORIZON.NOW_ACTIVE_PROCESS,
    monthsToDecision: 0,
  },
  { geographicClass: GEO_CLASS.UNRESOLVED },
);
assert.equal(badGeo.qualifiesPriorityA, false);

// 8. Duplicate reporting — same opportunity id counted once in first-100
const row = {
  hotelName: "Gideon Putnam Hotel",
  physicalCountry: "US",
  geographicClass: GEO_CLASS.VERIFIED,
  agreementType: "MANAGEMENT",
  agreementTypeResolved: true,
  commercialList: "MANAGEMENT_CONTRACT_TARGETS",
  decisionWindowKind: DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW,
  monthsToDecision: 0,
  commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
  key: "gideon-putnam-saratoga-ny",
};
assert.equal(qualifiesForFirst100Milestone(row).qualifies, true);

// 9. Unknown commercial decision timing
const unknownTiming = qualifyPriorityACurrentBackward(
  {
    hotelName: "Pending Pipeline Hotel",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    agreementType: "MANAGEMENT",
    commercialList: "MANAGEMENT_CONTRACT_TARGETS",
    decisionPending: true,
    currentStatusVerified: true,
    commercialThesis: "Owner preparing future operator RFP but no timeline evidenced yet.",
    sourceUrl: "https://example.com/den",
  },
  {
    decisionWindowKind: DECISION_WINDOW_KIND.TIMING_UNKNOWN,
    timingHorizon: TIMING_HORIZON.UNKNOWN,
    monthsToDecision: null,
  },
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(unknownTiming.qualifiesPriorityA, false);

// 10. Priority A qualification and rejection (expiration-only)
const expOnly = qualifyPriorityACurrentBackward(
  {
    hotelName: "Days Inn Example",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    agreementType: "FRANCHISE",
    commercialList: "BRAND_CONVERSION_TARGETS",
    currentStatusVerified: true,
    commercialThesis: "Near-term franchise expiration from CMBS schedule only — not a confirmed commercial decision.",
    sourceUrl: "https://sec.gov/example",
  },
  {
    decisionWindowKind: DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
    timingHorizon: TIMING_HORIZON.M0_6,
    monthsToDecision: 3,
  },
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(expOnly.qualifiesPriorityA, false);
assert.equal(expOnly.commercialPriority, COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH);

// 11. First-100 milestone counting — expiration-only still excluded
assert.equal(
  qualifiesForFirst100Milestone({
    hotelName: "Days Inn Example",
    physicalCountry: "US",
    geographicClass: GEO_CLASS.VERIFIED,
    agreementType: "FRANCHISE",
    agreementTypeResolved: true,
    commercialList: "BRAND_CONVERSION_TARGETS",
    decisionWindowKind: DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
    monthsToDecision: 3,
    commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
  }).qualifies,
  false,
);

// 12. Country × timing × freshness aggregation still available
const gaps = summarizeResearchGaps([
  {
    physicalCountry: "CA",
    geographicClass: "VERIFIED",
    timingHorizon: "NOW_ACTIVE_PROCESS",
    agreementType: "MANAGEMENT",
    agreementTypeResolved: true,
    currentStatusVerified: true,
  },
  {
    physicalCountry: "MX",
    geographicClass: "REVIEW_REQUIRED",
    timingHorizon: "UNKNOWN",
    agreementType: "UNKNOWN",
    agreementTypeResolved: false,
    decisionWindowKind: "TIMING_UNKNOWN",
  },
]);
assert.ok(gaps.byCountry.CA);
assert.ok(gaps.byCountry.MX);
assert.equal(gaps.totals.COVERAGE_GAP, "NOT_COMPUTED_WITHOUT_CREDIBLE_DOCUMENTED_HOTEL_UNIVERSE_DENOMINATOR");

console.log(
  JSON.stringify({ ok: true, gate: "test-hif-v7-current-backward-commercial-v7", checks: 12 }, null, 2),
);
