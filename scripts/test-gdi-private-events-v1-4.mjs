/**
 * GDI Private Events V1.4 — venue partnership qualification fixtures.
 */
import assert from "node:assert/strict";
import {
  EVENT_ACTIVITY_EVIDENCE_STATUS,
  ACTIVITY_EVIDENCE_TYPE,
  PARTNER_STATUS,
  VENUE_PRIORITY,
  classifyEventActivityEvidence,
  dedupeActivityEvidence,
  activitySupportsTruePartnership,
  classifyVenuePriority,
  qualifyVenuePartnership,
  buildHotelContext,
  buildHotelVenueRelationship,
  buildVenueEntity,
} from "../lib/group-demand-intelligence/private-events/index.js";
import { OPPORTUNITY_TYPE as OT } from "../lib/group-demand-intelligence/claim-types.js";
import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS  ${name}`);
  } catch (err) {
    failed += 1;
    console.log(`FAIL  ${name}`);
    console.log(`      ${err?.message || err}`);
  }
}

test("exact annual count → CONFIRMED_VOLUME", () => {
  const r = classifyEventActivityEvidence({
    estimatedAnnualPrivateEvents: 25,
    annualEventVolumeStatus: "CONFIRMED",
    website: "https://estate.example",
  });
  assert.equal(
    r.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.CONFIRMED_VOLUME
  );
});

test("no annual count + multiple official dated signals → STRONG_REPEATED_ACTIVITY", () => {
  const r = classifyEventActivityEvidence(
    {
      website: "https://mansion.example",
      officialDomain: "mansion.example",
      weddingsAdvertised: true,
    },
    [
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://mansion.example/weddings",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.EVENT_PACKAGE,
        sourceUrl: "https://mansion.example/packages",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.PUBLIC_BOOKING_LANGUAGE,
        sourceUrl: "https://mansion.example/inquire",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST,
        sourceUrl: "https://mansion.example/gallery/2024",
        official: true,
        sourceDate: "2024",
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST,
        sourceUrl: "https://mansion.example/gallery/2025",
        official: true,
        sourceDate: "2025",
      },
    ]
  );
  assert.equal(
    r.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY
  );
  assert.equal(activitySupportsTruePartnership(r.eventActivityEvidenceStatus), true);
});

test("only official weddings page → MODERATE_ACTIVITY", () => {
  const r = classifyEventActivityEvidence(
    {
      website: "https://club.example",
      officialDomain: "club.example",
      weddingsAdvertised: true,
    },
    [
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://club.example/weddings",
        official: true,
      },
    ]
  );
  assert.equal(
    r.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.MODERATE_ACTIVITY
  );
});

test("three duplicate directory pages → not strong", () => {
  const r = classifyEventActivityEvidence(
    { website: "https://real.example", officialDomain: "real.example" },
    [
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://www.theknot.com/marketplace/x",
        official: false,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://www.theknot.com/marketplace/x?ref=2",
        official: false,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://www.weddingwire.com/biz/x",
        official: false,
      },
    ]
  );
  assert.notEqual(
    r.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY
  );
});

test("official venue + multiple independent planner refs → strong", () => {
  const r = classifyEventActivityEvidence(
    {
      website: "https://garden.example",
      officialDomain: "garden.example",
      weddingsAdvertised: true,
    },
    [
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://garden.example/weddings",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.PUBLIC_BOOKING_LANGUAGE,
        sourceUrl: "https://garden.example/book",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.PLANNER_REFERENCE,
        sourceUrl: "https://planner-a.example/venues",
        official: false,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.PLANNER_REFERENCE,
        sourceUrl: "https://planner-b.example/venues",
        official: false,
      },
    ]
  );
  assert.equal(
    r.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY
  );
});

test("dedupe identical evidence paths", () => {
  const { items, duplicatesRemoved } = dedupeActivityEvidence([
    {
      evidenceType: ACTIVITY_EVIDENCE_TYPE.GALLERY,
      sourceUrl: "https://v.example/gallery/",
      official: true,
    },
    {
      evidenceType: ACTIVITY_EVIDENCE_TYPE.GALLERY,
      sourceUrl: "https://v.example/gallery",
      official: true,
    },
  ]);
  assert.equal(items.length, 1);
  assert.equal(duplicatesRemoved, 1);
});

test("exclusive hotel partner → block TRUE", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Exclusive Estate",
    officialDomain: "exclusive-estate.example",
    website: "https://exclusive-estate.example",
    lat: hotel.lat + 0.02,
    long: hotel.long,
    onSiteGuestrooms: 0,
    estimatedAnnualPrivateEvents: 40,
    annualEventVolumeStatus: "CONFIRMED",
    exclusiveHotelRelationship: true,
    eventContact: "e@exclusive-estate.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://exclusive-estate.example"],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  assert.equal(
    classifyVenuePriority(venue, rel).priority,
    VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY
  );
  assert.equal(
    qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel })
      .actionable,
    false
  );
});

test("NO_PUBLIC_PARTNER_FOUND does not equal confirmed no partner", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Open Club",
    officialDomain: "open-club.example",
    website: "https://open-club.example",
    lat: hotel.lat + 0.02,
    long: hotel.long,
    onSiteGuestrooms: 0,
    estimatedAnnualPrivateEvents: 30,
    annualEventVolumeStatus: "CONFIRMED",
    partnerStatus: PARTNER_STATUS.NO_PUBLIC_PARTNER_FOUND,
    partnerResearchComplete: true,
    eventContact: "e@open-club.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://open-club.example"],
    weddingsAdvertised: true,
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const q = qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel });
  assert.equal(q.partnerStatus, PARTNER_STATUS.NO_PUBLIC_PARTNER_FOUND);
  assert.equal(q.actionable, true);
  assert.ok(q.partnershipThesis.partnerStatusKnown.includes("not proof"));
});

test("no lodging + strong activity + good fit → eligible TRUE", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Strong Garden",
    officialDomain: "strong-garden.example",
    website: "https://strong-garden.example",
    lat: hotel.lat + 0.02,
    long: hotel.long,
    onSiteGuestrooms: 0,
    maxCapacity: 180,
    weddingsAdvertised: true,
    eventContact: "events@strong-garden.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://strong-garden.example"],
    partnerStatus: PARTNER_STATUS.NO_PUBLIC_PARTNER_FOUND,
    partnerResearchComplete: true,
    activityEvidence: [
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://strong-garden.example/weddings",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.EVENT_PACKAGE,
        sourceUrl: "https://strong-garden.example/packages",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.PUBLIC_BOOKING_LANGUAGE,
        sourceUrl: "https://strong-garden.example/book",
        official: true,
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST,
        sourceUrl: "https://strong-garden.example/2024",
        official: true,
        sourceDate: "2024",
      },
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.DATED_EVENT_POST,
        sourceUrl: "https://strong-garden.example/2025",
        official: true,
        sourceDate: "2025",
      },
    ],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const q = qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel });
  assert.equal(
    q.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.STRONG_REPEATED_ACTIVITY
  );
  assert.equal(q.actionable, true);
  assert.equal(q.opportunityType, OT.VENUE_PARTNERSHIP);
  assert.equal(q.doNotEstimateAnnualRevenue, undefined);
  assert.equal(q.partnershipThesis.doNotEstimateAnnualRevenue, true);
});

test("adequate on-site lodging → block/downgrade", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Resort Venue",
    officialDomain: "resort-venue.example",
    website: "https://resort-venue.example",
    lat: hotel.lat + 0.02,
    long: hotel.long,
    onSiteGuestrooms: 150,
    onSiteLodgingStatus: "ADEQUATE_LODGING",
    estimatedAnnualPrivateEvents: 40,
    annualEventVolumeStatus: "CONFIRMED",
    eventContact: "e@resort-venue.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://resort-venue.example"],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  assert.equal(
    qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel })
      .actionable,
    false
  );
});

test("MODERATE_ACTIVITY does not TRUE-promote", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Moderate Hall",
    officialDomain: "moderate-hall.example",
    website: "https://moderate-hall.example",
    lat: hotel.lat + 0.02,
    long: hotel.long,
    onSiteGuestrooms: 0,
    weddingsAdvertised: true,
    eventContact: "e@moderate-hall.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://moderate-hall.example"],
    activityEvidence: [
      {
        evidenceType: ACTIVITY_EVIDENCE_TYPE.OFFICIAL_WEDDING_PAGE,
        sourceUrl: "https://moderate-hall.example/weddings",
        official: true,
      },
    ],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const q = qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel });
  assert.equal(
    q.eventActivityEvidenceStatus,
    EVENT_ACTIVITY_EVIDENCE_STATUS.MODERATE_ACTIVITY
  );
  assert.equal(q.actionable, false);
});

test("replication: renaissance + cambridge no hotel-name logic", () => {
  for (const key of ["renaissance_ny", "cambridge_beaches"]) {
    const hotel = buildHotelContext(HOTELS[key]);
    const venue = buildVenueEntity({
      venueName: `${key} Estate Test`,
      officialDomain: `${key}-estate.example`,
      website: `https://${key}-estate.example`,
      lat: hotel.lat + 0.01,
      long: hotel.long,
      onSiteGuestrooms: 0,
      estimatedAnnualPrivateEvents: 20,
      annualEventVolumeStatus: "CONFIRMED",
      eventContact: `e@${key}-estate.example`,
      eventContactRole: "Director of Events",
      sourceUrls: [`https://${key}-estate.example`],
      weddingsAdvertised: true,
    });
    const rel = buildHotelVenueRelationship(hotel, venue);
    const q = qualifyVenuePartnership({
      venue,
      hotelCtx: hotel,
      relationship: rel,
    });
    assert.equal(q.actionable, true, key);
  }
});

console.log(`\nPrivate Events V1.4 tests: ${passed} pass, ${failed} fail`);
process.exit(failed ? 1 : 0);
