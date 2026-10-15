/**
 * Focused regressions for drawer field repairs:
 * - claim labels not in customer prose
 * - peak rooms ≠ housing program
 * - occasion geography not hotel-city shorthand
 * - San Diego allowlist
 * - distinct demand vs hotel sections after research apply
 */
import assert from "node:assert/strict";
import {
  stripClaimLabel,
  toCustomerProse,
  synthesizeHotelRelevanceResearch,
  applyHotelRelevanceToOpportunity,
} from "../lib/group-demand-intelligence/workflow/hotel-relevance-research-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import {
  isReviewedProspectHotelEnabled,
  REVIEWED_PROSPECT_HOTEL_IDS,
} from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";

assert.equal(stripClaimLabel("MIXED: WashCon attendees (hotel/host not confirmed — x)."), "WashCon attendees");
assert.equal(toCustomerProse("INFERENCE: Teams travel overnight."), "Teams travel overnight.");
assert.ok(!/^MIXED:/i.test(toCustomerProse("MIXED: Foo bar baz qux enough length here for substance.")));

const dtoPeak = toOpportunityListDto({
  peakRooms: null,
  estimatedPeakRooms: "UNKNOWN",
  roomDemandStatusLabel: "Verified Housing Program",
});
assert.equal(dtoPeak.peakRooms, null);
assert.equal(dtoPeak.estimatedPeakRooms, null);

const dtoGeo = toOpportunityListDto({
  eventLocationSummary: "Bethesda, MD",
  destinationStatus:
    "Multi-site DMV fields (not Bethesda-centered): Alexandria VA; SoccerPlex Boyds",
});
assert.ok(dtoGeo.destinationStatus.includes("Multi-site"));

assert.ok(REVIEWED_PROSPECT_HOTEL_IDS.includes("recnPB3Q02NO8Ma1o"));
assert.equal(
  isReviewedProspectHotelEnabled("recnPB3Q02NO8Ma1o", {
    GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1",
    GDI_REVIEWED_PROSPECT_HOTEL_IDS:
      "recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9,recnPB3Q02NO8Ma1o",
  }),
  true
);
assert.equal(
  isReviewedProspectHotelEnabled("recnPB3Q02NO8Ma1o", {
    GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1",
    GDI_REVIEWED_PROSPECT_HOTEL_IDS: "recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9",
  }),
  false
);

const nado = {
  id: "gdi_opp_test_nado",
  hotelId: "rec0eTEnCqlsGJJt9",
  organizationName: "NADO / DDAA",
  title: "NADO / DDAA Washington Conference 2028",
  prospectReviewCardId: "may-nado-2028",
  prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
  prospectHotelFacing: true,
  venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
  eventLocationSummary: "Arlington — hotel TBA; downtown DC is stretch",
  eventCity: "Arlington",
  venueCity: "Arlington",
  travelingCohort:
    "WashCon 2028 attendees (regional development organizations) — historically Arlington / Crystal City lodging pattern.",
  whyHotelDemand:
    "MIXED: WashCon 2028 attendees travel for the Arlington-pattern conference. (hotel/host not confirmed — do not treat as open buying).",
  hotelOpportunityThesis:
    "Stretch vs Arlington host pattern. Pursue only if NADO signals openness to downtown DC properties.",
  competitiveContextNote:
    "Arlington pattern historically. Downtown preference unconfirmed.",
  venueHistoryNote: "WashCon historically Arlington-oriented; 2028 hotel unpublished.",
  primaryContact: {
    name: "Jamie McCormick",
    email: "jmccormick@nado.org",
    phone: "202-921-4448",
  },
};

const research = synthesizeHotelRelevanceResearch(nado, {
  hotelId: "rec0eTEnCqlsGJJt9",
});
assert.ok(research.hotelBusinessMechanism?.text);
assert.ok(!/^MIXED:/i.test(research.hotelBusinessMechanism.text));
assert.equal(research.hotelBusinessMechanism.claimKind, "MIXED");
assert.ok(research.hotelFitAssessment?.dimensions?.geographyFit?.assessment);
assert.ok(
  /stretch|arlington|crystal/i.test(
    research.hotelFitAssessment.dimensions.geographyFit.assessment
  )
);

const applied = applyHotelRelevanceToOpportunity(nado, research);
assert.ok(!/^MIXED:/i.test(applied.whyHotelDemand || ""));
assert.ok(applied.summaryWhat);
assert.notEqual(applied.summaryWhat, applied.whyHotelDemand);
assert.notEqual(applied.travelingCohort, applied.hotelOpportunityThesis);
assert.equal(applied.prospectReviewSection, "EXPLORATORY");
assert.ok(applied.hotelFitDimensionAssessments || applied.hotelFitAssessment);

console.log("PASS test-gdi-drawer-field-repairs-v1");
