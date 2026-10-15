#!/usr/bin/env node
/**
 * Evidence-based use-case qualification integrity (not presentation-prose eligibility).
 */
import assert from "node:assert/strict";
import {
  evaluateProspectSuitability,
  evaluateProspectPublicationEligibility,
  classifyProspectUseCase,
  preservePublicationMetadata,
  authorizeProspectPublicationPromotion,
  authorizeProspectPublicationWithdrawal,
  REVIEWED_PUBLICATION_BASIS,
  PUBLICATION_EVENT_TYPE,
  PROSPECT_USE_CASE,
  applyContactPackageToOpportunity,
} from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isReviewedProspectCustomerVisible } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";

function withContact(opp, email = "events@example.org") {
  return { ...opp, contactEmail: email };
}

// A. Empty vs populated summaryWhat cannot change evidence eligibility
{
  const research = {
    organizationName: "Example Association",
    title: "Advocacy Meeting 2027",
    timing: "April 1–2, 2027",
    travelEvidenceNote: "Delegates travel from chapter markets",
    fitExplanation: "Downtown Autograph fit",
    hotelFitScore: 60,
    officialSource: "https://example.org/events/2027",
    sources: [{ url: "https://example.org/events/2027" }],
  };
  const empty = evaluateProspectSuitability({ ...research, summaryWhat: "" });
  const filled = evaluateProspectSuitability({
    ...research,
    summaryWhat:
      "Large traveler cohort needing overnight rooms and banquet space downtown.",
  });
  assert.equal(empty.ok, filled.ok);
  assert.deepEqual(empty.failed, filled.failed);
}

// B. Well-written unsupported summary still fails travel evidence
{
  const s = evaluateProspectSuitability({
    organizationName: "Unsupported Org",
    title: "Summit 2027",
    summaryWhat:
      "Hundreds of travelers will book room blocks and need banquet catering near the venue.",
    summaryWhyMatters: "Perfect hotel fit with strong lodging demand.",
    fitExplanation: "Downtown fit",
    hotelFitScore: 70,
    officialSource: "https://example.org/x",
    sources: [{ url: "https://example.org/x" }],
  });
  assert.equal(s.useCase, PROSPECT_USE_CASE.DATED_LODGING);
  assert.equal(s.ok, false);
  assert.ok(s.failed.includes("travel_need_evidence"));
}

// C. Structured PE referral evidence passes without summaryWhat / dated block
{
  const pe = withContact({
    organizationName: "deck655",
    opportunityType: "VENUE_PARTNERSHIP",
    peVenueId: "pev_test",
    summaryWhat: "",
    fitExplanation:
      "Waterfront venue supports complementary accommodation partnership and lodging referrals.",
    hotelFitScore: 68,
    sources: [
      {
        type: "OFFICIAL_PRIVATE_EVENT_PAGE",
        title: "Wedding receptions and private events",
        url: "https://deck655.com/",
      },
    ],
  });
  assert.equal(
    classifyProspectUseCase(pe),
    PROSPECT_USE_CASE.PREFERRED_LODGING_REFERRAL
  );
  const elig = evaluateProspectPublicationEligibility(pe);
  assert.equal(elig.hotelFacing, true, "supported referral PE must pass");
}

// D. Proximity-only venue remains unresolved
{
  const pe = withContact({
    organizationName: "Nearby Hall",
    opportunityType: "VENUE_PARTNERSHIP",
    peVenueId: "pev_near",
    fitExplanation:
      "Venue is nearby / walking distance from the hotel with no on-site lodging.",
    hotelFitScore: 50,
    sources: [
      {
        type: "official_venue",
        title: "Venue homepage",
        url: "https://nearby.example/",
      },
    ],
  });
  const s = evaluateProspectSuitability(pe);
  assert.equal(s.ok, false);
  assert.ok(s.failed.includes("lodging_referral_use_case"));
}

// E. LMC passes with guestrooms NOT_REQUIRED
{
  const lmc = withContact({
    organizationName: "Metro Chapter",
    title: "Metro Chapter — Local Meetings & Catering",
    researchSegment: "local_meetings_catering",
    guestroomStatus: "NOT_REQUIRED",
    eventActivityEvidence: "Annual awards gala with banquet dinner.",
    fitExplanation: "Ballroom inventory supports banquet.",
    hotelFitScore: 62,
    sources: [{ url: "https://chapter.example/gala" }],
  });
  const elig = evaluateProspectPublicationEligibility(lmc);
  assert.equal(elig.hotelFacing, true);
  assert.equal(
    evaluateProspectSuitability(lmc).useCase,
    PROSPECT_USE_CASE.LOCAL_MEETINGS_CATERING
  );
}

// F. Standing planner needs no invented date
{
  const planner = withContact({
    organizationName: "City Planners Co",
    title: "City Planners Co — Professional planners",
    lmcSubsegment: "professional_planners_intermediaries",
    entityKind: "PLANNER",
    eventActivityEvidence:
      "Portfolio shows downtown hotel weddings and corporate galas.",
    fitExplanation: "Meeting inventory supports planner-led hotel programs.",
    hotelFitScore: 61,
    sources: [{ url: "https://planners.example/portfolio" }],
  });
  const s = evaluateProspectSuitability(planner);
  assert.equal(s.useCase, PROSPECT_USE_CASE.PROFESSIONAL_PLANNER);
  assert.equal(s.ok, true);
  assert.equal(evaluateProspectPublicationEligibility(planner).hotelFacing, true);
}

// G. Scaling requires dated growth — funding-only fails
{
  const fundingOnly = withContact({
    organizationName: "Funded Co",
    title: "Funded Co — Scaling",
    researchSegment: "scaling_companies",
    localConnection: "DC office at 1 Main St",
    growthSignals: ["FUNDING_ROUND"],
    fitExplanation: "Nearby lodging for visiting teams",
    hotelFitScore: 55,
    sources: [{ url: "https://funded.example/news" }],
  });
  assert.equal(
    evaluateProspectSuitability(fundingOnly).ok,
    false,
    "funding-only insufficient"
  );
  assert.ok(
    evaluateProspectSuitability(fundingOnly).failed.includes(
      "dated_growth_or_expansion_evidence"
    )
  );

  const expanding = withContact({
    ...fundingOnly,
    datedGrowthEvidence: "Opened DC office Sep 2025; hiring plan published.",
    growthEvidenceDate: "2025-09-01",
    growthSignals: ["NEW_OFFICE_OR_LOCAL_EXPANSION"],
    operatingExpansionEvidence: true,
  });
  assert.equal(evaluateProspectSuitability(expanding).ok, true);
}

// H. Contact enrichment preserves publication basis
{
  const prior = applyContactPackageToOpportunity(
    {
      id: "gdi_sc_test",
      organizationName: "Wisedocs",
      title: "Wisedocs — Scaling",
      researchSegment: "scaling_companies",
      localConnection: "DC office",
      datedGrowthEvidence: "New DC office 2025",
      growthEvidenceDate: "2025-07-01",
      growthSignals: ["NEW_OFFICE_OR_LOCAL_EXPANSION"],
      operatingExpansionEvidence: true,
      fitExplanation: "Downtown meetings/lodging fit",
      hotelFitScore: 58,
      sources: [{ url: "https://wisedocs.ai/x" }],
      prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
      prospectHotelFacing: true,
      customerVisible: true,
    },
    {
      person: "Manal Abu-Shawar",
      contactMethod: "info@wisedocs.com",
      businessPhone: "Not established",
    }
  );
  const enriched = preservePublicationMetadata(prior, {
    ...prior,
    primaryContactName: "Manal Abu-Shawar",
    prospectPublicationBasis: null, // simulate accidental drop
  });
  assert.equal(enriched.prospectPublicationBasis, "REVIEWED_PACKAGE_V1");
  assert.equal(enriched.id, "gdi_sc_test");
}

// I. List/share/export eligibility consistency for equivalent PE
{
  const pe = withContact({
    id: "gdi_pe_test",
    hotelId: "recnPB3Q02NO8Ma1o",
    organizationName: "Sparks Gallery",
    opportunityType: "VENUE_PARTNERSHIP",
    peVenueId: "pev_sparks",
    prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
    prospectHotelFacing: true,
    customerVisible: true,
    fitExplanation:
      "Non-hotel wedding venue supports complementary accommodation partnership.",
    hotelFitScore: 68,
    sources: [
      {
        type: "OFFICIAL_PRIVATE_EVENT_PAGE",
        title: "Weddings and private gatherings",
        url: "https://sparksgallery.com/weddings",
      },
    ],
  });
  const elig = evaluateProspectPublicationEligibility(pe);
  assert.equal(elig.hotelFacing, true);
  assert.equal(
    isReviewedProspectCustomerVisible(pe, { hotelId: pe.hotelId }),
    true
  );
  const facing = filterCustomerFacingOpportunities([pe], { hotelId: pe.hotelId });
  assert.equal(facing.length, 1);
}

// J. Publication transition lifecycle: internal → authorized promote → enrich → reimport → withdraw
{
  const internal = withContact({
    id: "gdi_sc_lifecycle",
    hotelId: "rec0eTEnCqlsGJJt9",
    organizationName: "Lifecycle Co",
    title: "Lifecycle Co — Scaling",
    researchSegment: "scaling_companies",
    localConnection: "DC office",
    datedGrowthEvidence: "Opened DC office 2025",
    growthEvidenceDate: "2025-07-01",
    growthSignals: ["NEW_OFFICE_OR_LOCAL_EXPANSION"],
    operatingExpansionEvidence: true,
    fitExplanation: "Downtown meetings/lodging fit",
    hotelFitScore: 58,
    sources: [{ url: "https://lifecycle.example/x" }],
    prospectPublicationBasis: null,
    prospectHotelFacing: false,
    customerVisible: false,
    customerFeedback: { note: "keep-me" },
    salesActions: [{ id: "a1" }],
    decisionOutcomes: [{ id: "o1" }],
  });
  assert.equal(
    isReviewedProspectCustomerVisible(internal, { hotelId: internal.hotelId }),
    false,
    "initial internal not visible"
  );

  // Facing flags alone must NOT authorize promotion
  const fakeFacing = {
    ...internal,
    prospectHotelFacing: true,
    customerVisible: true,
  };
  const denied = authorizeProspectPublicationPromotion(fakeFacing, {
    authorizePublication: false,
  });
  assert.equal(denied.ok, false);
  assert.equal(denied.reason, "authorizePublication_required");
  assert.equal(fakeFacing.prospectPublicationBasis, null);

  const promoted = authorizeProspectPublicationPromotion(internal, {
    authorizePublication: true,
    reason: "test_authorized_promotion",
    authorizedBy: "unit_test",
  });
  assert.equal(promoted.ok, true);
  assert.equal(promoted.patched.prospectPublicationBasis, REVIEWED_PUBLICATION_BASIS);
  assert.equal(promoted.patched.prospectHotelFacing, true);
  assert.equal(promoted.patched.customerVisible, true);
  assert.equal(promoted.event.type, PUBLICATION_EVENT_TYPE.AUTHORIZED_PROMOTION);
  assert.equal(
    isReviewedProspectCustomerVisible(promoted.patched, {
      hotelId: internal.hotelId,
    }),
    true
  );

  // Subsequent enrichment preserves basis + feedback
  const enriched = preservePublicationMetadata(promoted.patched, {
    ...promoted.patched,
    primaryContactName: "Jane Doe",
    prospectPublicationBasis: null,
    customerFeedback: null,
    salesActions: null,
  });
  assert.equal(enriched.prospectPublicationBasis, REVIEWED_PUBLICATION_BASIS);
  assert.equal(enriched.customerFeedback?.note, "keep-me");
  assert.equal(enriched.salesActions?.[0]?.id, "a1");
  assert.equal(enriched.decisionOutcomes?.[0]?.id, "o1");

  // Idempotent reimport / re-promote
  const again = authorizeProspectPublicationPromotion(enriched, {
    authorizePublication: true,
  });
  assert.equal(again.ok, true);
  assert.equal(again.idempotent, true);

  // Basis repair when facing without basis (Wisedocs pattern)
  const missingBasis = {
    ...enriched,
    prospectPublicationBasis: null,
    prospectHotelFacing: true,
    customerVisible: true,
    publicationEvents: [],
  };
  const repaired = authorizeProspectPublicationPromotion(missingBasis, {
    authorizePublication: true,
    reason: "basis_repair",
  });
  assert.equal(repaired.ok, true);
  assert.equal(repaired.event.type, PUBLICATION_EVENT_TYPE.BASIS_REPAIR);
  assert.equal(repaired.patched.prospectPublicationBasis, REVIEWED_PUBLICATION_BASIS);

  // Explicit withdrawal remains withdrawn through enrichment
  const withdrawn = authorizeProspectPublicationWithdrawal(repaired.patched, {
    authorizeWithdrawal: true,
    reason: "test_withdraw",
  });
  assert.equal(withdrawn.ok, true);
  assert.equal(withdrawn.patched.prospectPublicationWithdrawn, true);
  assert.equal(withdrawn.patched.customerVisible, false);
  const afterEnrichWithdrawn = preservePublicationMetadata(withdrawn.patched, {
    ...withdrawn.patched,
    primaryContactPhone: "202-555-0100",
    customerVisible: true,
    prospectHotelFacing: true,
  });
  assert.equal(afterEnrichWithdrawn.prospectPublicationWithdrawn, true);
  assert.equal(afterEnrichWithdrawn.customerVisible, false);
  assert.equal(afterEnrichWithdrawn.prospectHotelFacing, false);
  assert.equal(
    isReviewedProspectCustomerVisible(afterEnrichWithdrawn, {
      hotelId: internal.hotelId,
    }),
    false
  );
}

console.log("test-gdi-use-case-qualification-integrity-v1: PASS");
