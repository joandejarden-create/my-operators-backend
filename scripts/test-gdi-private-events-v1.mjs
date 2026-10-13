/**
 * GDI Private Events V1 — offline unit + replication tests.
 * No live write / deploy / contact enrichment.
 */

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  DEMAND_SIGNAL_TYPE,
  DEMAND_FAMILY,
  demandFamilyForSignalType,
} from "../lib/group-demand-intelligence/demand-signal-types.js";
import { OPPORTUNITY_TYPE } from "../lib/group-demand-intelligence/claim-types.js";
import {
  buildVenueEntity,
  createVenueGraph,
  computeVenueId,
  buildHotelContext,
  buildHotelVenueRelationship,
  modelRoomDemand,
  isPrivatePersonSourceRejected,
  qualifySpecificEvent,
  qualifyVenuePartnership,
  classifyVenuePriority,
  runPrivateEventsForHotel,
  HOTEL_ARCHETYPE_PE,
  VENUE_PRIORITY,
  ROOM_DEMAND_CLAIM,
} from "../lib/group-demand-intelligence/private-events/index.js";
import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIX = join(__dirname, "../fixtures/group-demand-intelligence/private-events");

let passed = 0;
let failed = 0;
const failures = [];

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS  ${name}`);
  } catch (err) {
    failed += 1;
    failures.push({ name, error: String(err?.message || err) });
    console.log(`FAIL  ${name}`);
    console.log(`      ${err?.message || err}`);
  }
}

// —— Taxonomy ——
test("taxonomy: private event types map to PRIVATE_EVENTS family", () => {
  for (const t of [
    DEMAND_SIGNAL_TYPE.WEDDING,
    DEMAND_SIGNAL_TYPE.VENUE_PARTNERSHIP,
    DEMAND_SIGNAL_TYPE.DESTINATION_WEDDING,
  ]) {
    assert.equal(demandFamilyForSignalType(t), DEMAND_FAMILY.PRIVATE_EVENTS);
  }
  assert.equal(
    demandFamilyForSignalType(DEMAND_SIGNAL_TYPE.EVENT),
    DEMAND_FAMILY.GROUP_MEETINGS
  );
});

test("taxonomy: opportunity types exist", () => {
  assert.equal(
    OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT,
    "SPECIFIC_PRIVATE_EVENT"
  );
  assert.equal(OPPORTUNITY_TYPE.VENUE_PARTNERSHIP, "VENUE_PARTNERSHIP");
});

// —— Venue entity ——
test("venue entity: stable global id not hotel-prefixed", () => {
  const id = computeVenueId({
    venueName: "Oak Estate",
    officialDomain: "oak-estate.example",
    city: "Potomac",
    region: "MD",
  });
  assert.match(id, /^pev_[a-f0-9]{16}$/);
  assert.doesNotMatch(id, /bethesda|hotel/i);
});

test("venue graph: reuse across hotels (same domain+name)", () => {
  const graph = createVenueGraph();
  const a = graph.upsert({
    venueName: "Shared Hall",
    officialDomain: "shared-hall.example",
    city: "A",
    region: "MD",
    onSiteGuestrooms: 0,
    maxCapacity: 200,
    sourceUrls: ["https://shared-hall.example"],
  });
  const b = graph.upsert({
    venueName: "Shared Hall",
    officialDomain: "shared-hall.example",
    city: "A",
    region: "MD",
    estimatedAnnualPrivateEvents: 30,
    sourceUrls: ["https://shared-hall.example/events"],
  });
  assert.equal(a.venue.venueId, b.venue.venueId);
  assert.equal(b.reused, true);
  assert.equal(graph.size(), 1);
  assert.equal(b.venue.estimatedAnnualPrivateEvents, 30);
});

// —— Hotel context / catchment ——
test("hotel context: archetype catchment differs without hotel-name logic", () => {
  const sub = buildHotelContext(HOTELS.bethesda);
  const urb = buildHotelContext(HOTELS.renaissance_ny);
  const res = buildHotelContext(HOTELS.cambridge_beaches);
  assert.equal(sub.archetype, HOTEL_ARCHETYPE_PE.SUBURBAN_FULL_SERVICE);
  assert.equal(urb.archetype, HOTEL_ARCHETYPE_PE.URBAN_FULL_SERVICE);
  assert.equal(res.archetype, HOTEL_ARCHETYPE_PE.RESORT_DESTINATION);
  assert.ok(sub.practicalCatchment.coreMiles > urb.practicalCatchment.coreMiles);
  assert.ok(res.practicalCatchment.coreMiles > sub.practicalCatchment.coreMiles);
  assert.notDeepEqual(sub.preferredVenueTypes[0], urb.preferredVenueTypes[0]);
});

// —— Case fixtures from brief ——
test("case: 200-person wedding at zero-room venue 4 miles → modeled rooms", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Zero Room Estate",
    officialDomain: "zero-room.example",
    lat: hotel.lat + 4 / 69,
    long: hotel.long,
    onSiteGuestrooms: 0,
    maxCapacity: 220,
    weddingsAdvertised: true,
    sourceUrls: ["https://zero-room.example"],
    eventContact: "events@zero-room.example",
    eventContactRole: "Director of Events",
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  assert.ok(rel.distanceMiles != null && rel.distanceMiles < 5);
  const modeled = modelRoomDemand({
    signal: {
      eventType: "WEDDING",
      estimatedAttendance: 200,
      attendanceStatus: "ESTIMATED",
      roomBlockMentioned: true,
      publicEventEvidence: "out-of-town guests hotel block",
    },
    venue,
    hotelCtx: hotel,
    relationship: rel,
  });
  assert.equal(modeled.roomDemandStatus, ROOM_DEMAND_CLAIM.MODELED);
  assert.ok(modeled.potentialRoomsLow >= 5);
  assert.ok(modeled.potentialRoomsHigh > modeled.potentialRoomsLow);
  assert.match(modeled.displayLabel, /MODELED/);
});

test("case: 200-person wedding at resort with 250 guestrooms → reject/unknown", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Big Resort Venue",
    officialDomain: "big-resort.example",
    lat: hotel.lat + 3 / 69,
    long: hotel.long,
    onSiteGuestrooms: 250,
    maxCapacity: 300,
    sourceUrls: ["https://big-resort.example"],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const q = qualifySpecificEvent({
    signal: {
      eventName: "Resort Wedding",
      eventType: "WEDDING",
      eventDate: "2027-05-01",
      estimatedAttendance: 200,
      sourceUrl: "https://big-resort.example/w",
    },
    venue,
    hotelCtx: hotel,
    relationship: rel,
  });
  assert.equal(q.actionable, false);
  assert.ok(q.rejectReasons.includes("VENUE_HAS_SUBSTANTIAL_ON_SITE_ROOMS"));
});

test("case: local 80-person wedding near hotel → no lodging thesis", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Local Hall",
    officialDomain: "local-hall.example",
    lat: hotel.lat + 1 / 69,
    long: hotel.long,
    onSiteGuestrooms: 0,
    sourceUrls: ["https://local-hall.example"],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const modeled = modelRoomDemand({
    signal: {
      estimatedAttendance: 80,
      attendanceStatus: "ESTIMATED",
      publicEventEvidence: "local guests community only",
    },
    venue,
    hotelCtx: hotel,
    relationship: rel,
  });
  assert.equal(modeled.roomDemandStatus, ROOM_DEMAND_CLAIM.UNKNOWN);
});

test("case: destination wedding with travel language → strong nonlocality modeled", () => {
  const hotel = buildHotelContext(HOTELS.cambridge_beaches);
  const venue = buildVenueEntity({
    venueName: "Cliff Garden",
    officialDomain: "cliff-garden.example",
    lat: hotel.lat + 5 / 69,
    long: hotel.long,
    onSiteGuestrooms: 0,
    maxCapacity: 180,
    sourceUrls: ["https://cliff-garden.example"],
    eventContact: "events@cliff-garden.example",
    eventContactRole: "Director of Events",
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const modeled = modelRoomDemand({
    signal: {
      eventType: "DESTINATION_WEDDING",
      estimatedAttendance: 140,
      attendanceStatus: "ESTIMATED",
      roomBlockMentioned: true,
      publicEventEvidence: "destination wedding out-of-town guests airport transfer",
      plannerCompany: "Island Planners Ltd",
    },
    venue,
    hotelCtx: hotel,
    relationship: rel,
  });
  assert.equal(modeled.roomDemandStatus, ROOM_DEMAND_CLAIM.MODELED);
  assert.equal(modeled.factors.nonlocality, "STRONG");
});

test("case: venue 40 annual weddings no partner → HIGH_POTENTIAL_PARTNER path", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Busy Club",
    officialDomain: "busy-club.example",
    lat: hotel.lat + 2 / 69,
    long: hotel.long,
    onSiteGuestrooms: 0,
    maxCapacity: 200,
    estimatedAnnualPrivateEvents: 40,
    weddingsAdvertised: true,
    exclusiveHotelRelationship: false,
    hotelPartners: [],
    eventContact: "events@busy-club.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://busy-club.example"],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const pri = classifyVenuePriority(venue, rel);
  assert.equal(pri.priority, VENUE_PRIORITY.HIGH_POTENTIAL_PARTNER);
  const q = qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel });
  assert.equal(q.actionable, true);
  assert.equal(q.opportunityType, OPPORTUNITY_TYPE.VENUE_PARTNERSHIP);
});

test("case: exclusive hotel partner → LOW / reject", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Locked Club",
    officialDomain: "locked-club.example",
    lat: hotel.lat + 2 / 69,
    long: hotel.long,
    onSiteGuestrooms: 0,
    estimatedAnnualPrivateEvents: 50,
    exclusiveHotelRelationship: true,
    sourceUrls: ["https://locked-club.example"],
    eventContact: "e@locked-club.example",
    eventContactRole: "Director of Events",
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  assert.equal(
    classifyVenuePriority(venue, rel).priority,
    VENUE_PRIORITY.LOW_HOTEL_OPPORTUNITY
  );
  assert.equal(
    qualifyVenuePartnership({ venue, hotelCtx: hotel, relationship: rel }).actionable,
    false
  );
});

test("case: venue capacity unknown still classifies without crash", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Unknown Cap",
    officialDomain: "unk-cap.example",
    lat: hotel.lat + 2 / 69,
    long: hotel.long,
    onSiteGuestrooms: 0,
    maxCapacity: null,
    weddingsAdvertised: true,
    estimatedAnnualPrivateEvents: 12,
    sourceUrls: ["https://unk-cap.example"],
    eventContact: "e@unk-cap.example",
    eventContactRole: "Director of Events",
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const pri = classifyVenuePriority(venue, rel);
  assert.ok(Object.values(VENUE_PRIORITY).includes(pri.priority));
});

test("case: private personal data source rejected", () => {
  assert.equal(
    isPrivatePersonSourceRejected({
      sourceType: "PRIVATE_PERSONAL",
      publicEventEvidence: "bride personal address",
      coupleNamesOnly: true,
    }),
    true
  );
});

test("case: weak event signal rejected (no source / past)", () => {
  const hotel = buildHotelContext(HOTELS.bethesda);
  const venue = buildVenueEntity({
    venueName: "Weak Signal Hall",
    officialDomain: "weak.example",
    lat: hotel.lat,
    long: hotel.long,
    onSiteGuestrooms: 0,
    sourceUrls: ["https://weak.example"],
  });
  const rel = buildHotelVenueRelationship(hotel, venue);
  const q = qualifySpecificEvent({
    signal: { eventName: "Old Party", eventType: "SOCIAL_EVENT", eventDate: "2020-01-01" },
    venue,
    hotelCtx: hotel,
    relationship: rel,
  });
  assert.ok(q.rejectReasons.includes("PAST_OR_UNDATED_EVENT"));
});

test("no Bethesda-specific production logic in private-events modules", () => {
  const mods = [
    "constants.js",
    "venue-entity.js",
    "hotel-context.js",
    "lodging-capture.js",
    "venue-priority.js",
    "qualify.js",
    "run-pipeline.js",
  ];
  const root = join(__dirname, "../lib/group-demand-intelligence/private-events"); // scripts/ → lib/
  const banned =
    /\b(?:Bethesda|Marriott|Renaissance|Cambridge\s+Beaches|montgomery\s+county)\b/i;
  for (const m of mods) {
    const src = readFileSync(join(root, m), "utf8");
    assert.equal(banned.test(src), false, `${m} contains hotel/city hardcode`);
  }
});

// —— Replication ——
test("replication: three hotels same code, different mix", () => {
  const bVenues = JSON.parse(
    readFileSync(join(FIX, "venues-bethesda.json"), "utf8")
  ).venues;
  const rVenues = JSON.parse(
    readFileSync(join(FIX, "venues-renaissance-ny.json"), "utf8")
  ).venues;
  const cVenues = JSON.parse(
    readFileSync(join(FIX, "venues-cambridge-beaches.json"), "utf8")
  ).venues;

  const b = runPrivateEventsForHotel({ hotel: HOTELS.bethesda, venues: bVenues });
  const r = runPrivateEventsForHotel({
    hotel: HOTELS.renaissance_ny,
    venues: rVenues,
  });
  const c = runPrivateEventsForHotel({
    hotel: HOTELS.cambridge_beaches,
    venues: cVenues,
  });

  assert.ok(b.venueUniverse.total >= 50 && b.venueUniverse.total <= 100);
  assert.notEqual(b.hotelArchetype, r.hotelArchetype);
  assert.notEqual(b.hotelArchetype, c.hotelArchetype);
  assert.notDeepEqual(b.preferredVenueTypes, r.preferredVenueTypes);
  assert.notDeepEqual(b.preferredVenueTypes, c.preferredVenueTypes);
  // Different opportunity mix expected
  assert.ok(b.venueUniverse.HIGH_POTENTIAL_PARTNER >= 1);
  assert.ok(r.venueUniverse.total > 0);
  assert.ok(c.venueUniverse.total > 0);
});

test("replication: shared venue reuses venueId across hotel runs", () => {
  const graph = createVenueGraph();
  const shared = {
    venueName: "Regional Garden Estate Shared",
    officialDomain: "shared-garden-estate.example",
    city: "Potomac",
    region: "MD",
    onSiteGuestrooms: 0,
    maxCapacity: 220,
    estimatedAnnualPrivateEvents: 45,
    weddingsAdvertised: true,
    eventContact: "events@shared-garden-estate.example",
    eventContactRole: "Director of Events",
    sourceUrls: ["https://www.shared-garden-estate.example/weddings"],
    lat: 39.018,
    long: -77.208,
  };
  const b = runPrivateEventsForHotel({
    hotel: HOTELS.bethesda,
    venues: [shared],
    venueGraph: graph,
  });
  const r = runPrivateEventsForHotel({
    hotel: HOTELS.renaissance_ny,
    venues: [shared],
    venueGraph: graph,
  });
  assert.equal(graph.size(), 1);
  assert.equal(b.costs.cacheReuse + r.costs.cacheReuse >= 1, true);
  // Fit differs by geography
  const bAudit = b.venueUniverse.audits[0];
  const rAudit = r.venueUniverse.audits[0];
  assert.equal(bAudit.venueId, rAudit.venueId);
  assert.notEqual(bAudit.lodgingCatchmentFit, rAudit.lodgingCatchmentFit);
});

console.log(`\nPrivate Events V1 tests: ${passed} pass, ${failed} fail`);
if (failed) {
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}
