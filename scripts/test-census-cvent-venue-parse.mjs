#!/usr/bin/env node
/**
 * Unit tests for expanded Cvent venue parse + Choice census patch mapping.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseCventVenueHtml,
  CVENT_VENUE_CLIENT_VERSION,
} from "../lib/research-engine-v2/census-cvent-venue-client.js";
import {
  buildCventChoicePatch,
  CVENT_CHOICE_MATCHER_VERSION,
} from "../lib/research-engine-v2/census-cvent-choice-matcher.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixture = readFileSync(
  join(__dirname, "../fixtures/cvent-venue-comfort-inn-irapuato-snippet.html"),
  "utf8"
);

const venue = parseCventVenueHtml(
  fixture,
  "https://www.cvent.com/venues/irapuato/hotel/comfort-inn-irapuato/venue-f264d80b-e323-4365-842d-c91a18430d72"
);

assert.match(CVENT_VENUE_CLIENT_VERSION, /v4/);
assert.match(CVENT_CHOICE_MATCHER_VERSION, /v3-discovery-only/);
assert.equal(venue.title, "Comfort Inn Irapuato");
assert.equal(venue.guestRooms, 110);
assert.equal(venue.meetingRoomsCount, 1);
assert.equal(venue.choiceAffiliated, true);
assert.ok(venue.addressParts?.street?.includes("Villas de Irapuato"));
assert.equal(venue.addressParts?.postalCode, "36643");
assert.equal(
  venue.website,
  "https://www.choicehotels.com/mexico/irapuato/comfort-inn-hotels/mx092"
);
assert.ok(venue.listingText?.includes("110 spacious"));
assert.equal(venue.propertyType, "Hotel");
assert.equal(venue.latitude, 20.682647);
assert.equal(venue.longitude, -101.381644);
assert.ok(venue.hasMeetingSignal);
assert.ok(venue.totalMeetingSpace?.value > 0);
// Meeting rooms must never be confused with guest rooms
assert.notEqual(venue.guestRooms, venue.meetingRoomsCount);

const patch = buildCventChoicePatch(
  {
    "Property Name": "Comfort Inn Irapuato",
    "Current Brand": "Comfort Inn",
    City: "Irapuato",
    "Property Identity Key": "ind_choice_mx_mx092",
  },
  venue,
  venue.sourceUrl,
  { today: "2026-08-08" }
);

// source-policy-v1: Cvent venue facts are DISCOVERY_ONLY (steward notes / candidates)
assert.ok(
  !Object.prototype.hasOwnProperty.call(patch.patch || {}, "Rooms / Keys")
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(patch.patch || {}, "Official Property URL")
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(patch.patch || {}, "Meeting Space Flag")
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(patch.patch || {}, "Hotel Description - Source Text")
);
assert.ok(
  (patch.discoveryCandidates || []).some((c) => c.field === "Rooms / Keys") ||
    String(patch.patch?.["Notes for Steward"] || "").includes("cvent_discovery_only")
);
assert.ok(
  String(patch.patch?.["Notes for Steward"] || "").includes("cvent_discovery_only") ||
    String(patch.patch?.["Notes for Steward"] || "").includes("cvent_extras") ||
    String(patch.patch?.["Notes for Steward"] || "").includes("cvent_coords=")
);
// Never write Cvent coords directly
assert.equal(patch.patch?.Latitude, undefined);
assert.equal(patch.patch?.Longitude, undefined);

// Airport Asset Context also blocked as canonical Cvent venue write
const airportVenue = {
  ...venue,
  airportDistance: { value: 0.5, unit: "mi", raw: "0.5 mi" },
};
const airportPatch = buildCventChoicePatch(
  {
    "Property Name": "Comfort Inn Puerto Vallarta",
    "Current Brand": "Comfort Inn",
    City: "Puerto Vallarta",
    "Property Identity Key": "ind_choice_mx_test_pv",
    Address: "already filled",
    "Rooms / Keys": 100,
  },
  { ...airportVenue, title: "Comfort Inn Puerto Vallarta", address: "Blvd. Puerto Vallarta 1, Puerto Vallarta" },
  venue.sourceUrl,
  { today: "2026-08-08" }
);
assert.ok(
  !Object.prototype.hasOwnProperty.call(airportPatch.patch || {}, "Asset Context")
);

console.log(
  JSON.stringify(
    {
      ok: true,
      client: CVENT_VENUE_CLIENT_VERSION,
      matcher: CVENT_CHOICE_MATCHER_VERSION,
      guestRooms: venue.guestRooms,
      website: venue.website,
      patch_keys: Object.keys(patch.patch).sort(),
    },
    null,
    2
  )
);
