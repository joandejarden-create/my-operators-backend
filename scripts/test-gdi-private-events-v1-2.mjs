/**
 * Private Events V1.2 — unit gates (no live Serp spend).
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildVenueDiscoveryQueries,
  mapExtractToVenue,
  LIVE_VENUE_DISCOVERY_V1_2,
} from "../lib/group-demand-intelligence/private-events/live-venue-discovery.js";
import { buildForwardEventQueries } from "../lib/group-demand-intelligence/private-events/live-event-signals.js";
import { HOTELS } from "../fixtures/group-demand-intelligence/private-events/hotels.mjs";
import { HOTEL_ARCHETYPE_PE } from "../lib/group-demand-intelligence/private-events/constants.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// Version marker
assert.equal(LIVE_VENUE_DISCOVERY_V1_2, "gdi_private_events_live_venue_discovery_v1_2");
ok("version_marker");

// Generic query contract — market label drives geography
const bethesdaPlan = buildVenueDiscoveryQueries({
  marketLabel: "Bethesda MD Montgomery County",
  hotel: HOTELS.bethesda,
  maxQueries: 8,
});
assert.equal(bethesdaPlan.archetype, HOTEL_ARCHETYPE_PE.SUBURBAN_FULL_SERVICE);
assert.ok(bethesdaPlan.queries.length >= 4);
assert.ok(bethesdaPlan.queries.every((q) => /bethesda|montgomery/i.test(q.q)));
ok("bethesda_market_queries");

const nycPlan = buildVenueDiscoveryQueries({
  marketLabel: "Times Square New York NY",
  hotel: HOTELS.renaissance_ny,
  maxQueries: 6,
});
assert.equal(nycPlan.archetype, HOTEL_ARCHETYPE_PE.URBAN_FULL_SERVICE);
assert.ok(nycPlan.queries.every((q) => !/bethesda/i.test(q.q)));
assert.ok(nycPlan.queries.some((q) => /new york|times square/i.test(q.q)));
ok("renaissance_no_bethesda_leak");

const bermudaPlan = buildVenueDiscoveryQueries({
  marketLabel: "Sandys Bermuda",
  hotel: HOTELS.cambridge_beaches,
  maxQueries: 6,
});
assert.equal(bermudaPlan.archetype, HOTEL_ARCHETYPE_PE.RESORT_DESTINATION);
assert.ok(bermudaPlan.queries.every((q) => !/bethesda/i.test(q.q)));
ok("cambridge_no_bethesda_leak");

// Missing marketLabel fails closed
assert.throws(() => buildVenueDiscoveryQueries({ hotel: HOTELS.bethesda }), /marketLabel/);
ok("market_label_required");

// mapExtractToVenue builds pev_ id
const mapped = mapExtractToVenue(
  {
    venueName: "Example Historic Estate",
    venueType: "HISTORIC_ESTATE",
    city: "Rockville",
    region: "MD",
    website: "https://example-estate.com/weddings",
    onSiteLodgingStatus: "NO_LODGING",
    weddingsAdvertised: true,
    identityConfidence: "HIGH",
    sourceAuthority: "OFFICIAL_VENUE",
    keyFactsOfficialConfirmed: "PARTIAL",
  },
  { sourceUrls: ["https://example-estate.com/weddings"] }
);
assert.ok(mapped.venueId.startsWith("pev_"));
assert.equal(mapped.venueType, "HISTORIC_ESTATE");
assert.equal(mapped.onSiteLodgingStatus, "NO_LODGING");
assert.equal(mapped.research.officialWebsite, true);
ok("map_extract_to_venue");

// Forward event queries portable
const fqs = buildForwardEventQueries({
  venueName: "Example Estate",
  city: "Rockville",
});
assert.ok(fqs.length >= 3);
assert.ok(fqs.every((q) => /Example Estate/.test(q)));
ok("forward_event_queries");

// Production PE live modules must not hardcode Bethesda
const liveVenueSrc = readFileSync(
  join(ROOT, "lib/group-demand-intelligence/private-events/live-venue-discovery.js"),
  "utf8"
);
const liveSignalSrc = readFileSync(
  join(ROOT, "lib/group-demand-intelligence/private-events/live-event-signals.js"),
  "utf8"
);
assert.ok(!/bethesda/i.test(liveVenueSrc));
assert.ok(!/bethesda/i.test(liveSignalSrc));
assert.ok(!/marriott/i.test(liveVenueSrc));
ok("no_hotel_city_hardcodes_in_live_modules");

import { isPrivatePersonSourceRejected } from "../lib/group-demand-intelligence/private-events/lodging-capture.js";

assert.equal(
  isPrivatePersonSourceRejected({
    eventName: "Mary Vickerman and Adam Biggs's Wedding",
    sourceUrl: "https://www.theknot.com/us/mary-vickerman-and-adam-biggs-2027-01-23",
  }),
  true
);
ok("reject_couple_wedding_directory");

assert.equal(
  isPrivatePersonSourceRejected({
    eventName: "Annual Glenview Mansion Wedding and Events Expo",
    sourceUrl: "https://www.rockvillemd.gov/services/glenview-mansion-wedding-and-events-expo/",
    venueId: "pev_test",
  }),
  false
);
ok("allow_commercial_venue_expo");

console.log(`\n${passed} passed`);
