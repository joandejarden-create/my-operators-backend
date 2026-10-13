/**
 * GDI Private Events V1.1 — Airtable graph unit tests (offline).
 * No live writes. No Bethesda hardcodes in production modules.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";
import {
  findVenueMatch,
  upsertVenue,
  computeStableVenueId,
} from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import {
  computeFitId,
  upsertHotelVenueFit,
  assertCanonicalHotelId,
} from "../lib/group-demand-intelligence/private-events/airtable-fit-store.js";
import {
  computeSignalId,
  upsertPrivateEventSignal,
} from "../lib/group-demand-intelligence/private-events/airtable-signal-store.js";
import {
  buildGdiOpportunityFromPrivateEvent,
  peLinkAirtableFields,
} from "../lib/group-demand-intelligence/private-events/promote-to-gdi.js";
import { buildVenueEntity } from "../lib/group-demand-intelligence/private-events/venue-entity.js";
import {
  buildHotelContext,
  buildHotelVenueRelationship,
} from "../lib/group-demand-intelligence/private-events/hotel-context.js";
import {
  qualifyVenuePartnership,
  qualifySpecificEvent,
} from "../lib/group-demand-intelligence/private-events/qualify.js";
import { OPPORTUNITY_TYPE } from "../lib/group-demand-intelligence/claim-types.js";
import { DEMAND_FAMILY } from "../lib/group-demand-intelligence/demand-signal-types.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  PE_VENUE_ID_PREFIX,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const ret = fn();
    if (ret && typeof ret.then === "function") {
      throw new Error("use testAsync for async tests");
    }
    passed += 1;
    console.log(`PASS  ${name}`);
  } catch (err) {
    failed += 1;
    console.log(`FAIL  ${name}`);
    console.log(`      ${err?.message || err}`);
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`PASS  ${name}`);
  } catch (err) {
    failed += 1;
    console.log(`FAIL  ${name}`);
    console.log(`      ${err?.message || err}`);
  }
}

test("schema names: three PE tables + reused GDI opportunities", () => {
  assert.equal(PE_VENUES_TABLE_NAME, "Private Event Venues");
  assert.equal(HOTEL_VENUE_FIT_TABLE_NAME, "Hotel Venue Fit");
  assert.equal(PE_SIGNALS_TABLE_NAME, "Private Event Signals");
  assert.equal(GDI_OPPORTUNITIES_TABLE_NAME, "Group Demand Opportunities");
});

test("stable venue id uses pev_ prefix", () => {
  const id = computeStableVenueId({
    venueName: "Oak Estate",
    officialDomain: "oak-estate.example",
  });
  assert.ok(id.startsWith(PE_VENUE_ID_PREFIX));
});

await testAsync("venue insert dry-run produces Airtable field map", async () => {
  const r = await upsertVenue(
    {
      venueName: "Test Estate",
      officialDomain: "test-estate-v11.example",
      city: "X",
      region: "MD",
      onSiteGuestrooms: 0,
      maxCapacity: 200,
      sourceUrls: ["https://test-estate-v11.example"],
      eventContact: "events@test-estate-v11.example",
      eventContactRole: "Director of Events",
      lat: 38.99,
      long: -77.1,
    },
    { dryRun: true, existingCache: [] }
  );
  assert.equal(r.ok, true);
  assert.equal(r.action, "create");
  assert.ok(r.fields["Venue ID"]);
  assert.equal(r.fields["Venue Name"], "Test Estate");
});

await testAsync("venue update / dedupe prevents duplicate", async () => {
  const raw = {
    venueName: "Shared Hall V11",
    officialDomain: "shared-hall-v11.example",
    city: "A",
    region: "MD",
    onSiteGuestrooms: 0,
    maxCapacity: 180,
    sourceUrls: ["https://shared-hall-v11.example"],
    lat: 39.0,
    long: -77.2,
  };
  const first = await upsertVenue(raw, { dryRun: true, existingCache: [] });
  const cache = [
    {
      ...buildVenueEntity(raw),
      venueId: first.venueId,
      airtableRecordId: "recFAKE1",
    },
  ];
  const second = await upsertVenue(
    { ...raw, estimatedAnnualPrivateEvents: 40 },
    { dryRun: true, existingCache: cache }
  );
  assert.equal(second.action, "update");
  assert.equal(second.duplicatesPrevented, 1);
  assert.equal(second.venueId, first.venueId);
});

test("findVenueMatch domain+name", () => {
  const existing = [
    {
      ...buildVenueEntity({
        venueName: "Garden Place",
        officialDomain: "garden-place.example",
        city: "P",
        region: "MD",
      }),
      venueId: "pev_other_id_forced",
    },
  ];
  const m = findVenueMatch(
    {
      venueName: "Garden Place",
      officialDomain: "garden-place.example",
      city: "P",
      region: "MD",
    },
    existing
  );
  assert.equal(m.matchedBy, "DOMAIN_NAME");
  assert.equal(m.matchConfidence, "HIGH");
});

await testAsync("hotel-venue fit insert dry-run", async () => {
  const venue = buildVenueEntity({
    venueName: "Fit Venue",
    officialDomain: "fit-venue.example",
    lat: HOTELS.bethesda.lat + 2 / 69,
    long: HOTELS.bethesda.long,
    onSiteGuestrooms: 0,
    maxCapacity: 200,
    estimatedAnnualPrivateEvents: 30,
    weddingsAdvertised: true,
    sourceUrls: ["https://fit-venue.example"],
    eventContact: "e@fit-venue.example",
    eventContactRole: "Director of Events",
  });
  const fit = await upsertHotelVenueFit({
    hotel: HOTELS.bethesda,
    venue,
    dryRun: true,
  });
  assert.equal(fit.ok, true);
  assert.ok(fit.fitId.startsWith("hvf_"));
  assert.equal(fit.hotelId, HOTELS.bethesda.hotelId);
  assert.ok(["CORE", "COMPETITIVE", "STRETCH"].includes(fit.lodgingCatchmentFit));
});

await testAsync("same venue linked to two hotels — one venue id, two fits", async () => {
  const venue = buildVenueEntity({
    venueName: "Multi Hotel Estate",
    officialDomain: "multi-hotel-estate.example",
    lat: 39.018,
    long: -77.208,
    onSiteGuestrooms: 0,
    maxCapacity: 220,
    estimatedAnnualPrivateEvents: 45,
    weddingsAdvertised: true,
    sourceUrls: ["https://multi-hotel-estate.example"],
    eventContact: "e@multi-hotel-estate.example",
    eventContactRole: "Director of Events",
  });
  const a = await upsertHotelVenueFit({
    hotel: HOTELS.bethesda,
    venue,
    dryRun: true,
  });
  const b = await upsertHotelVenueFit({
    hotel: HOTELS.renaissance_ny,
    venue,
    dryRun: true,
  });
  assert.equal(a.venueId, b.venueId);
  assert.notEqual(a.fitId, b.fitId);
  assert.notEqual(a.lodgingCatchmentFit, b.lodgingCatchmentFit);
});

test("Venue X: A core-ish, B outside-ish, C outside — 1 venue 3 fits", () => {
  const venue = buildVenueEntity({
    venueName: "Geo Gradient Venue",
    officialDomain: "geo-gradient.example",
    lat: HOTELS.bethesda.lat + 1 / 69,
    long: HOTELS.bethesda.long,
    onSiteGuestrooms: 0,
    maxCapacity: 150,
    sourceUrls: ["https://geo-gradient.example"],
  });
  const fits = [HOTELS.bethesda, HOTELS.renaissance_ny, HOTELS.cambridge_beaches].map(
    (hotel) => {
      const ctx = buildHotelContext(hotel);
      return buildHotelVenueRelationship(ctx, venue);
    }
  );
  assert.equal(fits.length, 3);
  assert.ok(["CORE", "COMPETITIVE"].includes(fits[0].lodgingCatchmentFit));
  assert.equal(fits[1].lodgingCatchmentFit, "OUTSIDE");
  assert.equal(fits[2].lodgingCatchmentFit, "OUTSIDE");
  assert.equal(computeFitId(HOTELS.bethesda.hotelId, venue.venueId) !==
    computeFitId(HOTELS.renaissance_ny.hotelId, venue.venueId), true);
});

await testAsync("signal insert + update dry-run", async () => {
  const signal = {
    venueId: "pev_abc123",
    venueName: "Signal Venue",
    eventName: "Future Wedding",
    eventType: "WEDDING",
    eventDate: "2027-06-01",
    estimatedAttendance: 180,
    attendanceStatus: "ESTIMATED",
    sourceUrl: "https://signal.example/event",
    roomBlockMentioned: true,
  };
  const a = await upsertPrivateEventSignal(signal, { dryRun: true });
  assert.equal(a.ok, true);
  assert.ok(a.signalId.startsWith("pes_"));
  const b = await upsertPrivateEventSignal(
    { ...signal, estimatedAttendance: 200 },
    { dryRun: true }
  );
  assert.equal(b.signalId, a.signalId);
});

await testAsync("signal → GDI promotion builds canonical opp with PE links", async () => {
  const venue = buildVenueEntity({
    venueName: "Promo Venue",
    officialDomain: "promo-venue.example",
    lat: HOTELS.bethesda.lat + 2 / 69,
    long: HOTELS.bethesda.long,
    onSiteGuestrooms: 0,
    maxCapacity: 200,
    estimatedAnnualPrivateEvents: 25,
    weddingsAdvertised: true,
    sourceUrls: ["https://promo-venue.example"],
    eventContact: "e@promo-venue.example",
    eventContactRole: "Director of Events",
  });
  const hotelCtx = buildHotelContext(HOTELS.bethesda);
  const relationship = buildHotelVenueRelationship(hotelCtx, venue);
  const signal = {
    signalId: computeSignalId({
      venueId: venue.venueId,
      eventName: "Promo Wedding",
      eventDate: "2027-05-01",
      sourceUrl: "https://promo-venue.example/w",
    }),
    venueId: venue.venueId,
    eventName: "Promo Wedding",
    eventType: "WEDDING",
    eventDate: "2027-05-01",
    estimatedAttendance: 200,
    attendanceStatus: "ESTIMATED",
    roomBlockMentioned: true,
    publicEventEvidence: "out-of-town guests hotel block",
    plannerCompany: "Promo Planners LLC",
    sourceUrl: "https://promo-venue.example/w",
    potentialRoomsLow: 20,
    potentialRoomsHigh: 40,
    roomDemandStatus: "MODELED",
  };
  const q = qualifySpecificEvent({
    signal,
    venue,
    hotelCtx,
    relationship,
  });
  const fit = { fitId: computeFitId(hotelCtx.hotelId, venue.venueId), lodgingCatchmentFit: relationship.lodgingCatchmentFit };
  const opp = buildGdiOpportunityFromPrivateEvent({
    hotel: HOTELS.bethesda,
    venue,
    fit,
    signal,
    qualification: q,
  });
  assert.equal(opp.demandFamily, DEMAND_FAMILY.PRIVATE_EVENTS);
  assert.equal(opp.opportunityType, OPPORTUNITY_TYPE.SPECIFIC_PRIVATE_EVENT);
  assert.equal(opp.peVenueId, venue.venueId);
  assert.equal(opp.peSignalId, signal.signalId);
  assert.ok(opp.hotelVenueFitId);
  const links = peLinkAirtableFields(opp);
  assert.equal(links.peVenueId, venue.venueId);
});

await testAsync("venue partnership → GDI opportunity without signal", async () => {
  const venue = buildVenueEntity({
    venueName: "Partner Club",
    officialDomain: "partner-club.example",
    lat: HOTELS.bethesda.lat + 2 / 69,
    long: HOTELS.bethesda.long,
    onSiteGuestrooms: 0,
    maxCapacity: 200,
    estimatedAnnualPrivateEvents: 40,
    weddingsAdvertised: true,
    sourceUrls: ["https://partner-club.example"],
    eventContact: "e@partner-club.example",
    eventContactRole: "Director of Events",
  });
  const hotelCtx = buildHotelContext(HOTELS.bethesda);
  const relationship = buildHotelVenueRelationship(hotelCtx, venue);
  const q = qualifyVenuePartnership({ venue, hotelCtx, relationship });
  assert.equal(q.actionable, true);
  const opp = buildGdiOpportunityFromPrivateEvent({
    hotel: HOTELS.bethesda,
    venue,
    fit: { fitId: computeFitId(hotelCtx.hotelId, venue.venueId) },
    signal: null,
    qualification: q,
  });
  assert.equal(opp.opportunityType, OPPORTUNITY_TYPE.VENUE_PARTNERSHIP);
  assert.equal(opp.peSignalId, null);
  assert.ok(opp.peVenueId);
});

test("no orphan hotel fit when hotel id unresolved", () => {
  assert.throws(() => assertCanonicalHotelId(""), /hotel_id_unresolved/);
  assert.throws(() => assertCanonicalHotelId("not-a-valid-id"), /hotel_id_unresolved/);
  assert.equal(assertCanonicalHotelId("recLuxvwwxID7U2B8"), "recLuxvwwxID7U2B8");
});

test("no Bethesda-specific production logic in PE airtable modules", () => {
  const root = join(__dirname, "../lib/group-demand-intelligence/private-events");
  const banned =
    /\b(?:Bethesda|Marriott|Renaissance|Cambridge\s+Beaches|montgomery\s+county)\b/i;
  for (const m of [
    "airtable-field-map.js",
    "airtable-venue-store.js",
    "airtable-fit-store.js",
    "airtable-signal-store.js",
    "airtable-client.js",
    "promote-to-gdi.js",
  ]) {
    const src = readFileSync(join(root, m), "utf8");
    assert.equal(banned.test(src), false, `${m} contains hardcode`);
  }
});

console.log(`\nPrivate Events V1.1 tests: ${passed} pass, ${failed} fail`);
process.exit(failed ? 1 : 0);
