#!/usr/bin/env node
/**
 * Regression gates for commercial opportunity recovery (WHERE × WHEN × HOW CURRENT).
 */
import assert from "node:assert/strict";
import {
  annotateResearchFreshness,
  classifyResearchDirection,
  classifySourceVintage,
  classifyEvidenceFreshness,
  inferPublicationDateFromSource,
  RESEARCH_DIRECTION,
  SOURCE_VINTAGE,
  EVIDENCE_FRESHNESS,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-direction-freshness-v6.js";
import { deriveNextCommercialDecisionWindow, DECISION_WINDOW_KIND } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { classifyPhysicalGeography, GEO_CLASS } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";
import { classifyCommercialActionability, COMMERCIAL_PRIORITY } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";
import { qualifiesForFirst100Milestone, buildFirst100Scorecard } from "../lib/hotel-intelligence/sec-edgar/hif-v6-first-100-milestone-v6.js";
import { summarizeResearchGaps } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-gap-visibility-v6.js";

const asOf = "2026-11-21";

// 1. Source publication date versus discovery date
const pub = inferPublicationDateFromSource({
  sourceUrl: "https://www.sec.gov/Archives/edgar/data/1532799/000153949717001423/n987_424b.htm",
});
assert.equal(pub, "2017-01-01");
const annotated = annotateResearchFreshness(
  {
    hotelName: "Test Inn",
    sourceUrl: "https://www.sec.gov/Archives/edgar/data/1532799/000153949717001423/n987_424b.htm",
    wave: "NAME_FREE_CONTINUOUS",
    discoveredAt: "2026-11-21T12:00:00Z",
    expirationDate: "2027-06-01",
  },
  asOf,
);
assert.equal(annotated.sourcePublicationDate, "2017-01-01");
assert.equal(annotated.researchRunTimestamp, asOf);
assert.notEqual(annotated.sourcePublicationDate, annotated.eventDiscoveryTimestamp);

// 2. Historical-forward vs current-backward provenance
assert.equal(
  classifyResearchDirection({ wave: "NAME_FREE_SEC_CONTINUOUS", sourceFamily: "SEC_CMBS" }).researchDirection,
  RESEARCH_DIRECTION.HISTORICAL_FORWARD,
);
assert.equal(
  classifyResearchDirection({ discoveryMethod: "CURRENT_BACKWARD", wave: "MGMT_DISCOVERY" }).researchDirection,
  RESEARCH_DIRECTION.CURRENT_BACKWARD,
);
assert.equal(
  classifyResearchDirection({ wave: "PRIORITY_B_RECOVERY" }).researchDirection,
  RESEARCH_DIRECTION.CONTRACT_WINDOW_TARGETED,
);
assert.equal(classifyResearchDirection({}).researchDirection, RESEARCH_DIRECTION.OTHER_UNKNOWN);

// 3. Contract expiration versus actual notice deadline
const expOnly = deriveNextCommercialDecisionWindow({ expirationDate: "2027-03-01" }, asOf);
assert.equal(expOnly.decisionWindowKind, DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY);
const withNotice = deriveNextCommercialDecisionWindow(
  { expirationDate: "2028-01-01", noticeDeadline: "2027-01-15" },
  asOf,
);
assert.equal(withNotice.decisionWindowKind, DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW);
assert.equal(withNotice.nextCommercialDecisionDate, "2027-01-15");
assert.notEqual(withNotice.nextCommercialDecisionDate, "2028-01-01");

// 4. Current status after amendments / freshness
assert.equal(classifySourceVintage("2012-05-01"), SOURCE_VINTAGE.BEFORE_2015);
assert.equal(
  classifyEvidenceFreshness({}, asOf, "2012-05-01").evidenceFreshness,
  EVIDENCE_FRESHNESS.SUPERSESSION_RISK_HIGH,
);
assert.equal(
  classifyEvidenceFreshness({ currentStatusVerified: true }, asOf, "2012-05-01").evidenceFreshness,
  EVIDENCE_FRESHNESS.CURRENT_STATUS_CORROBORATED,
);

// 5. Verified versus provisional geography
const geoV = classifyPhysicalGeography({
  hotelCountry: "US",
  hotelName: "Hampton Inn Austin TX",
  city: "Austin",
  stateProvince: "TX",
});
assert.ok([GEO_CLASS.VERIFIED, GEO_CLASS.REVIEW_REQUIRED].includes(geoV.geographicClass));
const geoU = classifyPhysicalGeography({ hotelName: "Mystery Hotel" });
assert.equal(geoU.geographicClass, GEO_CLASS.UNRESOLVED);

// 6. Country × timing aggregation (gap visibility)
const gaps = summarizeResearchGaps([
  { physicalCountry: "US", geographicClass: "VERIFIED", timingHorizon: "0_6_MONTHS", agreementType: "FRANCHISE", agreementTypeResolved: true },
  { physicalCountry: "US", geographicClass: "REVIEW_REQUIRED", timingHorizon: "UNKNOWN", agreementType: "UNKNOWN", agreementTypeResolved: false, decisionWindowKind: "TIMING_UNKNOWN" },
]);
assert.equal(gaps.totals.GEOGRAPHIC_VERIFICATION_GAP, 1);
assert.equal(gaps.totals.TIMING_GAP, 1);
assert.equal(gaps.totals.COVERAGE_GAP, "NOT_COMPUTED_WITHOUT_CREDIBLE_DOCUMENTED_HOTEL_UNIVERSE_DENOMINATOR");

// 7. Priority A evidence gates — expiration-only cannot be A
const actB = classifyCommercialActionability(
  { agreementType: "FRANCHISE", agreementTypeResolved: true, commercialList: "BRAND_CONVERSION_TARGETS" },
  expOnly,
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.notEqual(actB.commercialPriority, COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW);
const actA = classifyCommercialActionability(
  { agreementType: "FRANCHISE", agreementTypeResolved: true, commercialList: "BRAND_CONVERSION_TARGETS" },
  withNotice,
  { geographicClass: GEO_CLASS.VERIFIED },
);
assert.equal(actA.commercialPriority, COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW);

// 8. First-100 milestone qualification — expiration-only excluded
const noCount = qualifiesForFirst100Milestone({
  hotelName: "Holiday Inn Express Test",
  physicalCountry: "US",
  geographicClass: GEO_CLASS.VERIFIED,
  agreementType: "FRANCHISE",
  agreementTypeResolved: true,
  commercialList: "BRAND_CONVERSION_TARGETS",
  decisionWindowKind: DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
  monthsToDecision: 6,
  commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
});
assert.equal(noCount.qualifies, false);
assert.ok(noCount.reasons.includes("EXPIRATION_ONLY_PROXY_EXCLUDED"));

const yesCount = qualifiesForFirst100Milestone({
  hotelName: "Holiday Inn Express Test",
  physicalCountry: "US",
  geographicClass: GEO_CLASS.VERIFIED,
  agreementType: "FRANCHISE",
  agreementTypeResolved: true,
  commercialList: "BRAND_CONVERSION_TARGETS",
  decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
  monthsToDecision: 6,
  nextCommercialDecisionDate: "2027-01-15",
  commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
});
assert.equal(yesCount.qualifies, true);

const scorecard = buildFirst100Scorecard(
  [
    {
      hotelName: "Holiday Inn Express Alpha",
      physicalCountry: "US",
      geographicClass: GEO_CLASS.VERIFIED,
      agreementType: "FRANCHISE",
      agreementTypeResolved: true,
      commercialList: "BRAND_CONVERSION_TARGETS",
      decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
      monthsToDecision: 3,
      commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
      key: "holiday inn express alpha|US|2027-01-15",
    },
    {
      hotelName: "Holiday Inn Express Beta expiration only",
      physicalCountry: "US",
      geographicClass: GEO_CLASS.VERIFIED,
      agreementType: "FRANCHISE",
      agreementTypeResolved: true,
      commercialList: "BRAND_CONVERSION_TARGETS",
      decisionWindowKind: DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY,
      monthsToDecision: 3,
      commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
      key: "b|US|2027-03-01",
    },
  ],
  asOf,
);
assert.equal(scorecard.qualifiedNearTermOpportunities, 1);
assert.equal(scorecard.target, 100);

// 9. Historical/superseded handling
const hist = qualifiesForFirst100Milestone({
  hotelName: "Old Inn",
  physicalCountry: "US",
  geographicClass: GEO_CLASS.VERIFIED,
  agreementType: "FRANCHISE",
  agreementTypeResolved: true,
  commercialList: "BRAND_CONVERSION_TARGETS",
  decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
  monthsToDecision: 3,
  commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
  status: "HISTORICAL_OR_SUPERSEDED",
});
assert.equal(hist.qualifies, false);

// 10–12. Dedup / worldwide / preservation — duplicate id rejected; MX eligible; counts not fabricated
const dup = buildFirst100Scorecard(
  [
    {
      hotelName: "Dup Inn",
      physicalCountry: "MX",
      geographicClass: GEO_CLASS.VERIFIED,
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      commercialList: "MANAGEMENT_CONTRACT_TARGETS",
      decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
      monthsToDecision: 4,
      commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
      key: "dup|MX|2027-02-01",
    },
    {
      hotelName: "Dup Inn",
      physicalCountry: "MX",
      geographicClass: GEO_CLASS.VERIFIED,
      agreementType: "MANAGEMENT",
      agreementTypeResolved: true,
      commercialList: "MANAGEMENT_CONTRACT_TARGETS",
      decisionWindowKind: DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW,
      monthsToDecision: 4,
      commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
      key: "dup|MX|2027-02-01",
    },
  ],
  asOf,
);
assert.equal(dup.qualifiedNearTermOpportunities, 1);
assert.equal(dup.byCountry.MX, 1);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test-hif-v6-commercial-opportunity-recovery-v6",
      checks: 12,
    },
    null,
    2,
  ),
);
