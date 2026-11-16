#!/usr/bin/env node
/**
 * Permanent positive/negative regression for canonical CMI eligibility.
 *
 *   npm run test:gdi-cmi-canonical-eligibility-contract-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  GDI_CMI_CONTRACT_VERSION,
  CMI_EVIDENCE_STATUS,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  evaluateCmiQualificationGates,
  evaluateCmiDiscoveryCoverage,
  assertNotCmiPromotionReasons,
  classifyCmiEvidenceStatus,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";
import {
  TEMPLATE_E2E_STAGES,
  COMMERCIAL_ELIGIBILITY_POLICY_V1,
  TEMPLATE_VERSION_LOCK,
  TEMPLATE_REFERENCE,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const POSITIVE = {
  id: "cmi_fixture_positive_2027",
  title: "Association Congress 2027",
  organizationName: "Example Association",
  competitorHotel: "Competitor Grand Hotel",
  competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK,
  competitorBookingClaim:
    "Official 2027 housing page names Competitor Grand Hotel as the host hotel with published room rates for March 2027.",
  lodgingEvidence: "Published room rates + host hotel listing for 2027 edition.",
  sources: [
    { url: "https://example.org/congress-2027/housing" },
    { url: "https://example.org/2027/hotels" },
  ],
  tileEventStartDate: "2027-03-10",
  tileEventEndDate: "2027-03-12",
  eventStartDate: "2027-03-10",
  eventDates: "2027-03-10/2027-03-12",
  targetHotelSourcingStatus: TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED,
  commercialActionability:
    COMMERCIAL_ACTIONABILITY.COMPETITIVE_MARKET_INTELLIGENCE,
  whyThisHotel:
    "Develop organizer relationship for a future edition if they rotate or need overflow.",
  recommendedAction:
    "Contact the housing representative to establish whether overflow is needed. Competitor booking is confirmed at Competitor Grand Hotel — do not request an RFP as host.",
  primaryContact: {
    name: "Alex Rivera",
    title: "Meetings Manager",
    organization: "Example Association",
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  },
  cmiEvidenceRegisterV1: {
    bookingType: "HOST_HOTEL_PLUS_PUBLISHED_ROOM_RATES",
    commitmentScope: "VENUE_AND_GUESTROOMS",
    futureCycleThesis: "Annual series — monitor next housing cycle.",
  },
};

check("contract_locked_in_template", () => {
  assert.equal(
    TEMPLATE_VERSION_LOCK.cmiCanonicalEligibilityContract,
    GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1
  );
  assert.equal(TEMPLATE_VERSION_LOCK.cmiContractVersion, GDI_CMI_CONTRACT_VERSION);
  assert.equal(
    COMMERCIAL_ELIGIBILITY_POLICY_V1.competitiveMarketIntelligence.contractId,
    GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1
  );
  const stage = TEMPLATE_E2E_STAGES.find((s) => s.id === "4a");
  assert.ok(stage);
  assert.equal(stage.mandatory, true);
});

check("positive_verified_competitor_commitment_qualifies", () => {
  const r = evaluateCmiQualificationGates(POSITIVE);
  assert.equal(
    r.evidenceStatus,
    CMI_EVIDENCE_STATUS.CONFIRMED_COMPETITOR_COMMITMENT
  );
  assert.equal(r.qualifiesAsCustomerCmi, true);
  assert.equal(r.failedGates.length, 0);
  assert.equal(r.hotelMarketScopeEligible, false);
  assert.equal(r.competitiveMarketScopeEligible, true);
});

check("negative_preferred_hotel_listing_only", () => {
  const opp = {
    id: "neg_listing_2027",
    title: "Trade Show 2027",
    competitorHotel: "Nearby Rival Hotel",
    demandEvidenceClass: "OFFICIAL_HOTEL_LIST_INCLUSION",
    hotelSourcingStatusLabel: "Official hotel programme / preferred hotel list",
    lodgingEvidence: "Event recommends several preferred hotels including Rival.",
    tileEventStartDate: "2027-06-01",
    recommendedAction: "Ask to be added to the preferred hotel list.",
    primaryContact: { name: "Pat Lee", title: "Coordinator", contactClass: "INTERMEDIARY" },
  };
  const r = evaluateCmiQualificationGates(opp);
  assert.equal(r.evidenceStatus, CMI_EVIDENCE_STATUS.HOTEL_PROGRAM_LISTING_ONLY);
  assert.equal(r.qualifiesAsCustomerCmi, false);
  assert.ok(assertNotCmiPromotionReasons(opp).length > 0);
});

check("negative_nearby_competitor_only", () => {
  const opp = {
    id: "neg_nearby",
    title: "City Expo 2027",
    whatIsHappening: "Nearby competitor hotels surround the expo district.",
    tileEventStartDate: "2027-09-01",
  };
  const r = evaluateCmiQualificationGates(opp);
  assert.equal(r.qualifiesAsCustomerCmi, false);
  assert.ok(r.failedGates.includes("NAMED_COMPETING_HOTEL"));
});

check("negative_unknown_sourcing", () => {
  const opp = {
    id: "neg_unknown_2027",
    title: "Summit 2027",
    competitorHotel: "Some Hotel",
    hotelSourcingStatusLabel: "Hotel sourcing unknown",
    tileEventStartDate: "2027-04-01",
    primaryContact: { name: "Sam", title: "Planner", contactClass: "UNKNOWN_BOOKING_AUTHORITY" },
    recommendedAction: "Verify demand before pursuit.",
  };
  assert.equal(evaluateCmiQualificationGates(opp).qualifiesAsCustomerCmi, false);
});

check("negative_venue_misrepresented_as_room_block", () => {
  const opp = {
    ...POSITIVE,
    id: "neg_venue_as_block",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE,
    competitorBookingClaim:
      "Official page names Competitor Grand Hotel as conference hotel / host hotel for March 2027.",
    lodgingEvidence: "Event venue only — no published room rates.",
    cmiEvidenceRegisterV1: {
      bookingType: "CONFIRMED_HOTEL_SEDE_NO_PUBLISHED_ROOM_RATES",
      commitmentScope: "VENUE_PRIMARY_GUESTROOMS_NOT_SEPARATELY_PUBLISHED",
      futureCycleThesis: "Monitor next cycle.",
    },
  };
  const r = evaluateCmiQualificationGates(opp);
  // Venue commitment can still be CMI, but committed type must not invent room block.
  assert.equal(r.committedBusinessType, "EVENT_VENUE");
  assert.notEqual(r.committedBusinessType, "GUESTROOMS");
});

check("negative_expired_without_future_cycle", () => {
  const opp = {
    id: "neg_expired_2024",
    title: "Past Congress 2024",
    competitorHotel: "Old Host Hotel",
    competitorVenueStatus: COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE,
    competitorBookingClaim: "2024 host hotel was Old Host Hotel.",
    sources: [{ url: "https://example.org/2024/recap" }],
    tileEventStartDate: "2024-05-01",
    eventStartDate: "2024-05-01",
    recommendedAction: "No action — event is over.",
    primaryContact: { name: "Archive", title: "n/a" },
  };
  const status = classifyCmiEvidenceStatus(opp);
  assert.equal(status, CMI_EVIDENCE_STATUS.HISTORICAL_COMPETITOR_RELATIONSHIP);
  assert.equal(evaluateCmiQualificationGates(opp).qualifiesAsCustomerCmi, false);
});

check("negative_claim_without_sufficient_evidence", () => {
  const opp = {
    id: "neg_weak_claim",
    title: "Maybe Booked 2027",
    competitorHotel: "Rival Inn",
    competitorVenueConfirmed: true,
    sources: [{ url: "https://example.org/" }],
    tileEventStartDate: "2027-08-01",
    recommendedAction: "Pursue the account.",
    primaryContact: { name: "Jamie", title: "Manager", contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT" },
  };
  assert.equal(evaluateCmiQualificationGates(opp).qualifiesAsCustomerCmi, false);
});

check("coverage_gate_zero_cmi_requires_checklist", () => {
  const incomplete = evaluateCmiDiscoveryCoverage({
    researchAttempted: true,
    evidenceFindingsDocumented: true,
    confirmedCompetitorBookingCount: 0,
    rejectedEvidenceCount: 2,
    completedChecklistItems: CMI_DISCOVERY_COVERAGE_CHECKLIST.slice(0, 3),
  });
  assert.equal(incomplete.coverageComplete, false);
  assert.equal(incomplete.fullE2eCmiCoverageClaimAllowed, false);

  const complete = evaluateCmiDiscoveryCoverage({
    researchAttempted: true,
    evidenceFindingsDocumented: true,
    confirmedCompetitorBookingCount: 0,
    rejectedEvidenceCount: 5,
    completedChecklistItems: [...CMI_DISCOVERY_COVERAGE_CHECKLIST],
  });
  assert.equal(complete.coverageComplete, true);
  assert.equal(complete.zeroConfirmedCmiAcceptable, true);
});

check("bethesda_10_cmi_fixtures_present_immutable_publication", () => {
  const store = createGdiPublicationPackageStore({
    root: path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages"
    ),
  });
  const loaded = store.loadPackage(
    TEMPLATE_REFERENCE.hotelId,
    TEMPLATE_REFERENCE.publicationVersion
  );
  assert.equal(loaded.ok, true);
  assert.equal(
    loaded.package.contentFingerprintSha256,
    TEMPLATE_REFERENCE.contentFingerprintSha256
  );
  const cmi = (loaded.package.opportunities || []).filter(
    (o) =>
      o.competitiveMarketScopeEligible === true ||
      (o.inventoryPopulations || []).includes("COMPETITIVE_MARKET_INTELLIGENCE")
  );
  assert.equal(cmi.length, 10);
  // Do not mutate Bethesda publication — read-only fixture assertion only.
  const verified = cmi.filter((o) => o.cmiVerified === true);
  const historicalOrNeeds = cmi.filter(
    (o) =>
      o.competitiveStatus === "HISTORICAL" ||
      o.competitiveStatus === "NEEDS_VERIFICATION"
  );
  assert.ok(verified.length >= 5, "expect multiple verified CMI in approved pack");
  assert.ok(historicalOrNeeds.length >= 1, "expect non-verified competitive statuses present");
});

console.log(
  JSON.stringify(
    {
      suite: "gdi-cmi-canonical-eligibility-contract-v1",
      contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
      version: GDI_CMI_CONTRACT_VERSION,
      passed,
      failed,
    },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
