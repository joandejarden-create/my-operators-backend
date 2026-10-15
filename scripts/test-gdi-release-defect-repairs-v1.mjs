#!/usr/bin/env node
/**
 * Focused regressions for GDI release defects:
 * - Internal evidence labels (MIXED:/FACT:/INFERENCE:) must not leak into prose
 * - Duplicate rationale paragraphs across drawer sections
 * - Housing status must not appear as peak room counts
 * - Account location must not substitute for occasion geography
 * - Authorized San Diego reviewed-prospect visibility (allowlist)
 */
import assert from "node:assert/strict";
import {
  stripEvidenceClaimPrefixes,
  distinctCustomerProse,
} from "../lib/group-demand-intelligence/workflow/customer-facing-prose-v1.js";
import {
  synthesizeHotelRelevanceResearch,
  applyHotelRelevanceToOpportunity,
} from "../lib/group-demand-intelligence/workflow/hotel-relevance-research-v1.js";
import { assessHotelFitForOpportunity } from "../lib/group-demand-intelligence/workflow/hotel-fit-assessment-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import {
  REVIEWED_PROSPECT_HOTEL_IDS,
  isReviewedProspectHotelEnabled,
} from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("stripEvidenceClaimPrefixes removes MIXED/FACT/INFERENCE", () => {
  assert.equal(
    stripEvidenceClaimPrefixes(
      "MIXED: WashCon attendees travel to Arlington. (hotel/host not confirmed — do not treat as open buying)."
    ),
    "WashCon attendees travel to Arlington. (hotel/host not confirmed — do not treat as open buying)."
  );
  assert.equal(
    stripEvidenceClaimPrefixes("INFERENCE: Overnight lodging is typical."),
    "Overnight lodging is typical."
  );
  assert.ok(!/^MIXED:/i.test(stripEvidenceClaimPrefixes("MIXED: x")));
});

check("distinctCustomerProse collapses near-duplicate paragraphs", () => {
  const paras = distinctCustomerProse(
    "WashCon 2028 attendees (regional development organizations) — historically Arlington / Crystal City lodging pattern.",
    "WashCon 2028 attendees (regional development organizations) — historically Arlington / Crystal City lodging pattern. Hotel/host is not confirmed — do not treat as open buying.",
    "Stretch vs Arlington host pattern."
  );
  assert.equal(paras.length, 2);
  assert.ok(!paras.some((p) => /^MIXED:/i.test(p)));
});

check("hotel-relevance research does not leak claim prefixes into fields", () => {
  const research = synthesizeHotelRelevanceResearch(
    {
      id: "test-nado",
      hotelId: "rec0eTEnCqlsGJJt9",
      organizationName: "National Association of Development Organizations (NADO) / DDAA",
      title: "NADO / DDAA Washington Conference 2028",
      travelingCohort:
        "WashCon 2028 attendees (regional development organizations) — historically Arlington / Crystal City lodging pattern.",
      whyHotelDemand:
        "WashCon 2028 attendees (regional development organizations) — historically Arlington / Crystal City lodging pattern.",
      eventLocationSummary: "Arlington, Virginia — hotel TBA",
      destinationStatus: "Arlington, Virginia",
      eventCity: "Arlington",
      venueSourcingStatus: "UNKNOWN",
      eventStartDate: "2028-03-19",
      eventEndDate: "2028-03-22",
      venueHistoryNote: "WashCon historically Arlington-oriented; Crystal Gateway pattern.",
      primaryContact: {
        name: "Jamie McCormick",
        email: "jmccormick@nado.org",
        phone: "202-921-4448",
      },
    },
    { hotelId: "rec0eTEnCqlsGJJt9" }
  );
  for (const field of [
    research.travelingCohort?.text,
    research.hotelBusinessMechanism?.text,
    research.propertySpecificFit?.text,
    research.limitations?.text,
    research.timingRationale?.text,
  ]) {
    assert.ok(field, "expected research field");
    assert.ok(
      !/^(?:FACT(?:\/FIT-GATE)?|INFERENCE|MIXED|HYPOTHESIS)\s*:/i.test(field),
      `leaked label in: ${field}`
    );
  }
  assert.ok(research.hotelBusinessMechanism?.claimKind);
  const opp = applyHotelRelevanceToOpportunity(
    {
      id: "test-nado",
      hotelId: "rec0eTEnCqlsGJJt9",
      organizationName: "NADO",
      title: "NADO WashCon 2028",
    },
    research
  );
  assert.ok(opp.hotelFitAssessment);
  assert.ok(opp.hotelFitQualitativeComponents || opp.hotelFitScore != null);
  assert.equal(opp.hotelFitPosture, "EXPLORATORY_STRETCH");
  // Cohort and mechanism should not be identical after apply
  assert.notEqual(
    String(opp.travelingCohort || "").slice(0, 80),
    String(opp.whyHotelDemand || "").slice(0, 80)
  );
});

check("peak rooms DTO never surfaces Verified Housing Program", () => {
  const dto = toOpportunityListDto({
    id: "beth-premier",
    hotelId: "recLuxvwwxID7U2B8",
    title: "Bethesda Premier Cup",
    peakRooms: null,
    estimatedPeakRooms: null,
    roomDemandStatus: "VERIFIED_HOUSING_PROGRAM",
    roomDemandStatusLabel: "Verified Housing Program",
    eventLocationSummary:
      "Multi-site DMV fields (not Bethesda-centered): Alexandria VA; Poolesville; Olney",
    city: "Bethesda",
  });
  assert.equal(dto.peakRooms, null);
  assert.equal(dto.estimatedPeakRooms, null);
  assert.equal(dto.housingProgramStatus, "VERIFIED_HOUSING_PROGRAM");
  assert.match(String(dto.housingProgramStatusLabel), /Verified Housing Program/i);
  assert.ok(!/Bethesda, MD/i.test(String(dto.eventLocationSummary || "")));
  assert.match(String(dto.eventLocationSummary), /Multi-site|Alexandria|Poolesville/i);
});

check("sanitize rejects housing label stuffed into peakRooms field", () => {
  const dto = toOpportunityListDto({
    peakRooms: "Verified Housing Program",
    roomDemandStatus: "VERIFIED_HOUSING_PROGRAM",
  });
  assert.equal(dto.peakRooms, null);
});

check("Mayflower NADO fit assessment is stretch, not invented Priority advantage", () => {
  const a = assessHotelFitForOpportunity({
    hotelId: "rec0eTEnCqlsGJJt9",
    title: "NADO Arlington Washington Conference 2028",
    destinationStatus: "Arlington, Virginia",
    eventLocationSummary: "Arlington — hotel TBA",
    eventCity: "Arlington",
    venueHistoryNote: "Historically Crystal Gateway Marriott / Arlington",
    venueSourcingStatus: "UNKNOWN",
    primaryContact: { email: "jmccormick@nado.org" },
  });
  assert.equal(a.posture, "EXPLORATORY_STRETCH");
  assert.match(a.summary, /No supported primary-host advantage/i);
  assert.ok(a.dimensions.geographyFit);
  assert.ok(a.dimensions.historicalFit);
  assert.ok(a.dimensions.contactability);
  assert.notEqual(a.dimensions.contactability.level, a.dimensions.physicalFit.level);
});

check("San Diego is on reviewed-prospect hotel allowlist", () => {
  assert.ok(REVIEWED_PROSPECT_HOTEL_IDS.includes("recnPB3Q02NO8Ma1o"));
  assert.equal(
    isReviewedProspectHotelEnabled("recnPB3Q02NO8Ma1o", {
      GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1",
      GDI_REVIEWED_PROSPECT_HOTEL_IDS:
        "recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9,recnPB3Q02NO8Ma1o",
    }),
    true
  );
  // Env allowlist without SD must not silently include code defaults
  assert.equal(
    isReviewedProspectHotelEnabled("recnPB3Q02NO8Ma1o", {
      GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1",
      GDI_REVIEWED_PROSPECT_HOTEL_IDS: "recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9",
    }),
    false
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nAll focused release-defect regressions passed.");
