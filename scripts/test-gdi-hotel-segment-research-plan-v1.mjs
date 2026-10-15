#!/usr/bin/env node
/**
 * Unit checks for hotel segment research plan V1.
 */
import assert from "node:assert/strict";
import {
  RESEARCH_SEGMENT,
  APPLICABILITY,
  RESEARCH_COVERAGE,
  assessSegmentApplicability,
  classifyOpportunitySegment,
  buildHotelSegmentResearchPlan,
} from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";

const mayCfg = {
  displayName: "The Mayflower Hotel, Autograph Collection",
  city: "Washington",
  capabilityProfile: {
    totalGuestrooms: 581,
    meetingRoomCount: 27,
    totalMeetingSpaceSqFt: 42582,
    classification: "historic_downtown_meetings_social",
  },
  demandTerritory: { label: "Washington DC / Downtown / Connecticut Avenue" },
  commercialPriorities: { targetSegments: [], weekendFocus: true },
};

const sdCfg = {
  displayName: "Wyndham San Diego Bayside",
  city: "San Diego",
  capabilityProfile: {
    totalGuestrooms: 600,
    meetingRoomCount: 15,
    totalMeetingSpaceSqFt: 27187,
    classification: "bayfront_meetings_convention_adjacent",
  },
  demandTerritory: {
    label: "San Diego / North Harbor Drive / Downtown Waterfront",
    includes: ["Convention Center", "Gaslamp", "harbor"],
  },
  commercialPriorities: { targetSegments: [], weekendFocus: true },
};

const mayApp = assessSegmentApplicability(mayCfg);
assert.equal(
  mayApp[RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT].applicability,
  APPLICABILITY.APPLICABLE
);
assert.equal(
  mayApp[RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS].applicability,
  APPLICABILITY.APPLICABLE
);
assert.equal(
  mayApp[RESEARCH_SEGMENT.SPORTS].applicability,
  APPLICABILITY.APPLICABLE
);
assert.equal(
  mayApp[RESEARCH_SEGMENT.SCALING_COMPANIES].applicability,
  APPLICABILITY.APPLICABLE
);

const sdApp = assessSegmentApplicability(sdCfg);
assert.equal(
  sdApp[RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS].applicability,
  APPLICABILITY.APPLICABLE
);
assert.equal(
  sdApp[RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION].applicability,
  APPLICABILITY.APPLICABLE
);

assert.equal(
  classifyOpportunitySegment({
    peVenueId: "pev_x",
    opportunityType: "VENUE_PARTNERSHIP",
    demandFamily: "PRIVATE_EVENTS",
    title: "Midway",
  }),
  RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
);
assert.equal(
  classifyOpportunitySegment({
    title: "Lockheed Martin exhibiting team — WEST 2027",
    segment: "Association",
  }),
  RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS
);
assert.equal(
  classifyOpportunitySegment({
    title: "2026 11U PG West Coast Showdown",
    segment: "youth sports",
    organizationName: "Perfect Game",
  }),
  RESEARCH_SEGMENT.SPORTS
);

const assocOnlyPlan = buildHotelSegmentResearchPlan({
  hotelId: "rec0eTEnCqlsGJJt9",
  hotelConfig: mayCfg,
  opportunities: [
    {
      id: "a1",
      title: "AHIMA Advocacy Summit 2027",
      segment: "Association",
      opportunityType: "OVERFLOW_HOUSING",
      organizationName: "AHIMA",
      hotelOpportunityThesis: "Downtown DC fit for advocacy lodging",
      summaryWhat: "Traveling members need downtown rooms",
      contactUrl: "https://example.org/contact/housing",
      prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
      prospectHotelFacing: true,
      sources: [{ url: "https://example.org" }],
    },
  ],
  artifacts: { paidBudgetApproved: false },
});
assert.equal(assocOnlyPlan.summary.associationOnlyTrap, true);
assert.equal(assocOnlyPlan.summary.fullCoverageClaimAllowed, false);

const pePlan = buildHotelSegmentResearchPlan({
  hotelId: "recnPB3Q02NO8Ma1o",
  hotelConfig: sdCfg,
  opportunities: [
    {
      id: "pe1",
      title: "USS Midway Museum — Preferred Lodging Partnership",
      segment: "Private Events",
      demandFamily: "PRIVATE_EVENTS",
      opportunityType: "VENUE_PARTNERSHIP",
      organizationName: "USS Midway Museum",
      peVenueId: "pev_7aa5",
      hotelVenueFitId: "hvf_x",
      hotelOpportunityThesis: "Bayfront catchment referral",
      summaryWhat: "Evening events without on-site lodging",
      contactUrl: "https://www.midway.org/plan-an-event/evening-events",
      sources: [{ url: "https://www.midway.org/plan-an-event/evening-events" }],
      prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
      prospectHotelFacing: true,
    },
    {
      id: "a2",
      title: "WEST 2027",
      segment: "Association",
      opportunityType: "GROUP_LODGING_OCCASION",
      organizationName: "AFCEA",
      hotelOpportunityThesis: "Convention Center catchment",
      summaryWhat: "Attendees need downtown lodging",
      contactUrl: "https://example.org/plan-an-event/housing",
      sources: [{ url: "https://example.org" }],
      prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
      prospectHotelFacing: true,
    },
  ],
  artifacts: { peCanonicalPersisted: true, peVenueCount: 2 },
});
const peSeg = pePlan.segments.find(
  (s) => s.segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
);
assert.equal(peSeg.researchCoverage, RESEARCH_COVERAGE.RESEARCHED);
assert.equal(pePlan.summary.associationOnlyTrap, false);

console.log("PASS test-gdi-hotel-segment-research-plan-v1");
