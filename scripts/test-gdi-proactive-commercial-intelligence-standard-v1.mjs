#!/usr/bin/env node
/**
 * Offline regression: GDI Proactive Commercial Intelligence Standard V1.
 * No paid calls. No customer publication. No operational writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyCommercialDemandThesis,
  DEMAND_THESIS_STANDARD,
  DEMAND_THESIS_NOT_REQUIRED,
} from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import {
  evaluateFivePartCommercialThesis,
  evaluateProactiveAccountDevelopment,
  evaluateProactivePublicationReviewReadiness,
  extractVerifiedCommercialTrigger,
  PROACTIVE_CI_NOT_REQUIRED,
} from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../lib/group-demand-intelligence/account-development-eligibility-v1.js";
import { orgDeepKey } from "../lib/group-demand-intelligence/research/gdi-paid-deep-two-stage-dedupe-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixture = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures/gdi/proactive-commercial-intelligence-standard-v1.json"),
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

check("not_required_includes_confirmed_booking_and_rfp", () => {
  for (const key of [
    "confirmed_room_night_demand",
    "published_rfp",
    "announced_meeting",
    "verified_room_night_volume",
    "confirmed_hotel_selection",
    "active_rfp",
  ]) {
    assert.ok(DEMAND_THESIS_NOT_REQUIRED.includes(key) || PROACTIVE_CI_NOT_REQUIRED.includes(key));
  }
});

check("office_expansion_no_rfp_is_reliable_inferred_ad", () => {
  const c = byId("fx_office_expansion_no_rfp");
  const thesis = classifyCommercialDemandThesis(c.opportunity);
  assert.equal(thesis.thesisStandard, DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND);
  const ad = evaluateProactiveAccountDevelopment(c.opportunity);
  assert.equal(ad.accountDevelopment, true);
  const pub = evaluateProactivePublicationReviewReadiness(c.opportunity);
  assert.equal(pub.publicationReviewReady, true);
  assert.equal(pub.customerPublicationReady, false);
});

check("project_visiting_workforce_reliable_inferred", () => {
  const c = byId("fx_project_visiting_workforce");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  );
  assert.equal(evaluateProactiveAccountDevelopment(c.opportunity).accountDevelopment, true);
});

check("training_program_reliable_inferred", () => {
  const c = byId("fx_training_program_rooms_meetings");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  );
});

check("future_cycle_competitor_hosted_not_rejected_for_missing_contract", () => {
  const c = byId("fx_future_cycle_competitor_hosted");
  const thesis = classifyCommercialDemandThesis(c.opportunity);
  assert.equal(thesis.thesisStandard, DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND);
  assert.ok(!thesis.mustRemainResearchContinuation);
});

check("venue_without_guestrooms_trigger_recognized", () => {
  const c = byId("fx_venue_referral_no_guestrooms");
  const trigger = extractVerifiedCommercialTrigger(c.opportunity);
  assert.equal(trigger.hasVerifiedTrigger, true);
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  );
  const five = evaluateFivePartCommercialThesis(c.opportunity);
  assert.ok(five.gaps.includes("VERIFIED_NAMED_COMMERCIAL_ENTRY_POINT"));
});

check("professional_event_planner_entity_not_conference_brand", () => {
  const c = byId("fx_professional_event_planner");
  assert.match(c.opportunity.organizationName, /Planner/i);
  assert.doesNotMatch(c.opportunity.organizationName, /Annual Conference/i);
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  );
});

check("local_chapter_catering_activity_reliable_inferred", () => {
  const c = byId("fx_local_chapter_catering");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  );
});

check("verified_trigger_without_named_entry_point_stays_incomplete", () => {
  const c = byId("fx_trigger_no_named_contact");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  );
  const five = evaluateFivePartCommercialThesis(c.opportunity);
  assert.ok(five.gaps.includes("VERIFIED_NAMED_COMMERCIAL_ENTRY_POINT"));
  assert.equal(evaluateProactiveAccountDevelopment(c.opportunity).accountDevelopment, false);
});

check("named_contact_without_trigger_remains_speculative", () => {
  const c = byId("fx_named_contact_no_trigger");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND,
  );
  assert.equal(evaluateAccountDevelopmentEligibility(c.opportunity).accountDevelopment, false);
});

check("strong_fit_unconfirmed_requirement_does_not_require_confirmed_hotel", () => {
  const c = byId("fx_strong_fit_unconfirmed_requirement");
  const thesis = classifyCommercialDemandThesis(c.opportunity);
  assert.equal(thesis.thesisStandard, DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND);
  assert.ok(thesis.notRequired.includes("confirmed_room_night_demand"));
  assert.equal(evaluateProactiveAccountDevelopment(c.opportunity).accountDevelopment, true);
});

check("out_of_market_expansion_rejected", () => {
  const c = byId("fx_out_of_market_rejected");
  const trigger = extractVerifiedCommercialTrigger(c.opportunity);
  assert.equal(trigger.outOfMarketRejected, true);
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND,
  );
});

check("generic_corporate_profile_speculative", () => {
  const c = byId("fx_generic_corporate_profile");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND,
  );
});

check("confirmed_demand_still_maps_from_explicit_housing_language", () => {
  const c = byId("fx_confirmed_demand_sales_ready_shape");
  assert.equal(
    classifyCommercialDemandThesis(c.opportunity).thesisStandard,
    DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND,
  );
});

check("same_account_cross_segment_shares_org_key", () => {
  const c = byId("fx_cross_segment_same_account");
  const a = orgDeepKey(c.opportunity);
  const b = orgDeepKey(c.sibling);
  assert.equal(a, b);
  assert.notEqual(c.opportunity.researchSegment, c.sibling.researchSegment);
});

check("engine_does_not_require_two_named_contacts_or_universal_booking_authority", () => {
  const c = byId("fx_office_expansion_no_rfp");
  const five = evaluateFivePartCommercialThesis(c.opportunity);
  assert.ok(five.complete);
  assert.ok(five.notRequired.includes("two_named_contacts") || PROACTIVE_CI_NOT_REQUIRED.includes("two_named_contacts"));
  assert.ok(
    five.notRequired.includes("universal_direct_hotel_booking_responsibility") ||
      PROACTIVE_CI_NOT_REQUIRED.includes("universal_direct_hotel_booking_responsibility"),
  );
});

console.log(`\nOK  ${passed} proactive commercial intelligence checks passed (offline, $0)`);
