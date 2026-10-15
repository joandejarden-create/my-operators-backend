#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  SCALING_GROWTH_SIGNAL,
  SCALING_QUALIFY_FAIL,
  assessScalingCompaniesApplicability,
  qualifyScalingCompanyCandidate,
  buildScalingCompanyOpportunityCard,
  buildScalingCompanyDemandGeneratorPackage,
  computeScalingCompanyOpportunityId,
  findExistingScalingCompanyMatches,
} from "../lib/group-demand-intelligence/workflow/scaling-companies-research-v1.js";
import {
  RESEARCH_SEGMENT,
  assessSegmentApplicability,
  classifyOpportunitySegment,
} from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";
import { OPPORTUNITY_TYPE } from "../lib/group-demand-intelligence/claim-types.js";

const hotel = {
  displayName: "Wyndham San Diego Bayside",
  city: "San Diego",
  capabilityProfile: {
    totalGuestrooms: 600,
    meetingRoomCount: 15,
    classification: "bayfront_meetings_convention_adjacent",
  },
  demandTerritory: { label: "San Diego / North Harbor Drive" },
};

assert.equal(assessScalingCompaniesApplicability(hotel).applicable, true);
assert.equal(
  assessSegmentApplicability(hotel)[RESEARCH_SEGMENT.SCALING_COMPANIES]
    .applicability,
  "applicable"
);

// Funding alone fails
const fundingOnly = qualifyScalingCompanyCandidate(
  {
    organizationName: "Acme Bio",
    growthSignal: SCALING_GROWTH_SIGNAL.FUNDING_WITH_OPERATING_EXPANSION,
    sourceUrl: "https://techcrunch.com/funding",
    growthEvidenceDate: "2026-09-01",
    datedGrowthEvidence: "Series B announced",
  },
  hotel
);
assert.equal(fundingOnly.ok, false);
assert.ok(fundingOnly.failReasons.includes(SCALING_QUALIFY_FAIL.FUNDING_ONLY));
assert.ok(
  fundingOnly.failReasons.includes(SCALING_QUALIFY_FAIL.MISSING_LOCAL_CONNECTION)
);

// Directory only fails
const dirOnly = qualifyScalingCompanyCandidate(
  {
    organizationName: "DirCo",
    localConnection: "HQ in San Diego",
    sourceUrl: "https://www.crunchbase.com/organization/dirco",
    growthSignal: SCALING_GROWTH_SIGNAL.HIRING_DISTRIBUTED_TEAMS,
  },
  hotel
);
assert.ok(dirOnly.failReasons.includes(SCALING_QUALIFY_FAIL.DIRECTORY_ONLY));

// Qualified standing card — no event date invented
const card = buildScalingCompanyOpportunityCard(
  {
    organizationName: "Harbor Analytics",
    officialDomain: "harboranalytics.example",
    website: "https://harboranalytics.example",
    localConnection:
      "Opened downtown San Diego customer-success office (North Harbor corridor)",
    growthSignals: [
      SCALING_GROWTH_SIGNAL.NEW_OFFICE_OR_LOCAL_EXPANSION,
      SCALING_GROWTH_SIGNAL.CUSTOMER_IMPLEMENTATION_TRAINING,
    ],
    growthEvidenceDate: "2026-08-15",
    datedGrowthEvidence:
      "Company press release: San Diego CS office opened Aug 2026 to support West Coast implementations",
    proposedHotelUse:
      "Plausible overnight + meeting use for visiting implementation engineers and customer training cohorts (inferred — volume unknown)",
    whyThisHotel:
      "Bayfront meetings hotel within practical catchment of the new downtown office; can host short training blocks and visitor lodging",
    whyEstablishContactNow:
      "Office is newly opened — establish preferred nearby lodging before patterns harden",
    establishedFacts: [
      "San Diego CS office opened Aug 2026 (company press)",
    ],
    inferredHotelDemand: [
      "Visiting implementation staff may need nearby rooms during customer training (inferred)",
    ],
    contactRole: "customer_success",
    contactUrl: "https://harboranalytics.example/contact",
    sourceUrl: "https://harboranalytics.example/news/san-diego-office",
    publish: false,
  },
  { hotelId: "recnPB3Q02NO8Ma1o", hotelConfig: hotel }
);
assert.equal(card.ok, true);
assert.equal(
  card.opportunity.opportunityType,
  OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP
);
assert.equal(card.opportunity.eventStartDate, null);
assert.equal(card.opportunity.standingOpportunity, true);
assert.match(card.opportunity.eventDateDisplay, /Ongoing/i);

const pkg = buildScalingCompanyDemandGeneratorPackage(
  {
    organizationName: "Harbor Analytics",
    officialDomain: "harboranalytics.example",
    website: "https://harboranalytics.example",
    localConnection: "Opened downtown San Diego CS office",
    growthSignals: [SCALING_GROWTH_SIGNAL.NEW_OFFICE_OR_LOCAL_EXPANSION],
    growthEvidenceDate: "2026-08-15",
    datedGrowthEvidence: "Office opened Aug 2026",
    proposedHotelUse: "Visitor lodging for CS implementations",
    whyThisHotel: "Bayfront catchment to new office",
    sourceUrl: "https://harboranalytics.example/news/san-diego-office",
    publish: false,
  },
  { hotelId: "recnPB3Q02NO8Ma1o", hotelConfig: hotel }
);
assert.equal(pkg.ok, true);
// Airtable select lacks SCALING_COMPANY — persist LARGE_EMPLOYER; scaling on opportunity
assert.equal(pkg.demandGenerator.organizationType, "LARGE_EMPLOYER");
assert.equal(pkg.opportunity.researchSegment, "scaling_companies");
assert.equal(pkg.signal.standingCorporateRelationship, true);

const id1 = computeScalingCompanyOpportunityId(
  "recnPB3Q02NO8Ma1o",
  "Harbor Analytics",
  "harboranalytics.example"
);
const id2 = computeScalingCompanyOpportunityId(
  "recnPB3Q02NO8Ma1o",
  "Harbor Analytics",
  "harboranalytics.example"
);
assert.equal(id1, id2);

const matches = findExistingScalingCompanyMatches(
  [card.opportunity, { id: "other", organizationName: "Other Co", segment: "Association" }],
  "Harbor Analytics",
  "harboranalytics.example"
);
assert.equal(matches.length, 1);

assert.equal(
  classifyOpportunitySegment(card.opportunity),
  RESEARCH_SEGMENT.SCALING_COMPANIES
);

console.log("PASS test-gdi-scaling-companies-research-v1");
