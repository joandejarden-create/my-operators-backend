#!/usr/bin/env node
/**
 * Offline regression: Commercial Hypothesis Development V1.
 * No paid calls. No customer publication. No operational writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  developCommercialHypotheses,
  applyCommercialHypothesisDevelopmentInChain,
  assessHotelSpecificGeographyFit,
  CONFIRMED_CUSTOMER_REQUIREMENT,
} from "../lib/group-demand-intelligence/gdi-commercial-hypothesis-development-v1.js";
import {
  evaluateProactiveAccountDevelopment,
  evaluateProactivePublicationReviewReadiness,
} from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixture = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures/gdi/commercial-hypothesis-development-v1.json"),
    "utf8",
  ),
);

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS  ${name}`);
}
function byId(id) {
  return fixture.cases.find((c) => c.id === id);
}

check("corp_expansion_rooms_no_booking_unknown_requirement", () => {
  const c = byId("chd_corp_expansion_rooms_no_booking");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.equal(hyp.confirmedCustomerRequirement, CONFIRMED_CUSTOMER_REQUIREMENT.UNKNOWN);
  assert.equal(hyp.geography.okForAccountDevelopment, true);
  assert.equal(evaluateProactiveAccountDevelopment(c.opportunity).accountDevelopment, true);
});

check("secondary_catering_uncertainty_does_not_force_both", () => {
  const c = byId("chd_rooms_primary_catering_speculative");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.notEqual(hyp.proposedRevenueType, "BOTH");
});

check("project_visiting_unknown_volume_still_guest_rooms", () => {
  const c = byId("chd_project_visiting_unknown_volume");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.equal(hyp.confirmedCustomerRequirement, "UNKNOWN");
});

check("geo_unsuitable_infrastructure_blocks_publication_review", () => {
  const c = byId("chd_geo_unsuitable_infrastructure");
  const applied = applyCommercialHypothesisDevelopmentInChain(c.opportunity);
  assert.equal(applied.hypothesis.geography.okForAccountDevelopment, false);
  const pub = evaluateProactivePublicationReviewReadiness(applied.opportunity);
  assert.equal(pub.publicationReviewReady, false);
  assert.ok(
    pub.blockers.some((b) => /GEOGRAPHY|HOTEL_FIT|GENERIC/i.test(String(b))),
  );
});

check("association_catering_without_event_date", () => {
  const c = byId("chd_association_catering_no_date");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.proposedRevenueType, "LOCAL_CATERING");
});

check("venue_referral_primary_guest_rooms", () => {
  const c = byId("chd_venue_referral_rooms");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.equal(evaluateProactiveAccountDevelopment(c.opportunity).accountDevelopment, false);
});

check("arlington_contested_metro_not_automatic_bethesda_fit", () => {
  const c = byId("chd_arlington_contested_metro");
  const geo = assessHotelSpecificGeographyFit(c.opportunity);
  assert.equal(geo.okForAccountDevelopment, false);
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.ok(hyp.blocksPublicationReview.includes("HOTEL_SPECIFIC_GEOGRAPHY_NOT_DEFENSIBLE"));
});

check("confirmed_housing_call_sets_confirmed_requirement", () => {
  const c = byId("chd_confirmed_housing_call");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.confirmedCustomerRequirement, "YES");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    "CONFIRMED_DEMAND",
  );
});

check("generic_corporate_has_no_credible_primary", () => {
  const c = byId("chd_generic_corporate");
  const hyp = developCommercialHypotheses(c.opportunity);
  assert.equal(hyp.hasCrediblePrimary, false);
});

check("undetermined_cleared_when_proposed_revenue_exists", () => {
  const c = byId("chd_corp_expansion_rooms_no_booking");
  const applied = applyCommercialHypothesisDevelopmentInChain({
    ...c.opportunity,
    commercialDemandType: "UNDETERMINED",
  });
  assert.equal(applied.opportunity.commercialDemandType, "GUEST_ROOMS");
  assert.equal(applied.opportunity.proposedRevenueType, "GUEST_ROOMS");
});

check("knowbe4_real_evidence_contested_geography", () => {
  const ors = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunity-research-state/disc_5976f7572131.json",
      ),
      "utf8",
    ),
  );
  const opp = ors.opportunity || ors;
  const hyp = developCommercialHypotheses(opp);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.equal(hyp.geography.okForAccountDevelopment, false);
  assert.equal(hyp.confirmedCustomerRequirement, "UNKNOWN");
});

check("ameresco_real_evidence_contested_geography_guest_rooms_hypothesis", () => {
  const ors = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunity-research-state/disc_2fc664271289.json",
      ),
      "utf8",
    ),
  );
  const opp = ors.opportunity || ors;
  const hyp = developCommercialHypotheses(opp);
  assert.equal(hyp.proposedRevenueType, "GUEST_ROOMS");
  assert.equal(hyp.geography.okForAccountDevelopment, false);
  assert.ok(!hyp.bothIndependentlySupported);
});

console.log(`\nOK  ${passed} commercial hypothesis development checks passed (offline, $0)`);
