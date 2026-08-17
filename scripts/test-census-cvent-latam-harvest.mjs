#!/usr/bin/env node
/**
 * Unit tests for Cvent LATAM harvest modules (no live network / Airtable).
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveCventLatamCountries,
  buildCventCountryResultsUrl,
  findCventLatamCountry,
  CVENT_LATAM_CARIBBEAN_COUNTRIES,
} from "../lib/research-engine-v2/census-cvent-latam-country-registry.js";
import {
  classifyCventVenueUrl,
  extractCventVenueUuid,
  parseCventResultsMeta,
} from "../lib/research-engine-v2/census-cvent-country-results-harvester.js";
import {
  parseCventVenueHtml,
  CVENT_VENUE_CLIENT_VERSION,
} from "../lib/research-engine-v2/census-cvent-venue-client.js";
import {
  matchCventVenueToCensus,
  buildCventLatamUpdatePatch,
  buildCventCensusOnlyInsertFields,
  hasNearDuplicateCensusRow,
  nameSimilarity,
} from "../lib/research-engine-v2/census-cvent-latam-matcher.js";
import { INTERNAL_ONLY_INSERT_DEFAULTS } from "../lib/research-engine-v2/census-confidence-tiered-internal-completion.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixture = readFileSync(
  join(__dirname, "../fixtures/cvent-venue-comfort-inn-irapuato-snippet.html"),
  "utf8"
);

assert.ok(CVENT_LATAM_CARIBBEAN_COUNTRIES.length >= 40);
assert.equal(findCventLatamCountry("Dominican Republic")?.slug, "Dominican-Republic");
assert.equal(findCventLatamCountry("Mexico")?.slug, "Mexico");
assert.ok(
  buildCventCountryResultsUrl("Dominican-Republic")?.includes(
    "/venues/results/Dominican-Republic"
  )
);
assert.equal(resolveCventLatamCountries(["Mexico", "DR"]).length, 1); // DR not exact
assert.equal(resolveCventLatamCountries(["Mexico", "Dominican Republic"]).length, 2);

const hotel = classifyCventVenueUrl(
  "https://www.cvent.com/venues/punta-cana/hotel/foo/venue-a1cc1426-a2df-43de-b5bf-e8e376219d03"
);
assert.equal(hotel.hotelLike, true);
const other = classifyCventVenueUrl(
  "https://www.cvent.com/venues/foo/restaurant/bar/venue-a1cc1426-a2df-43de-b5bf-e8e376219d03"
);
assert.equal(other.hotelLike, false);
assert.equal(
  extractCventVenueUuid(
    "https://www.cvent.com/venues/x/hotel/y/venue-a1cc1426-a2df-43de-b5bf-e8e376219d03"
  ),
  "a1cc1426-a2df-43de-b5bf-e8e376219d03"
);

const meta = parseCventResultsMeta(
  String.raw`{\"totalCount\":291,\"currentPage\":1} 1-25 of 291 events and meeting venues`
);
assert.equal(meta.totalCount, 291);

assert.match(CVENT_VENUE_CLIENT_VERSION, /v4/);
const venue = parseCventVenueHtml(
  fixture,
  "https://www.cvent.com/venues/irapuato/hotel/comfort-inn-irapuato/venue-f264d80b-e323-4365-842d-c91a18430d72"
);
assert.equal(venue.guestRooms, 110);
assert.equal(venue.title, "Comfort Inn Irapuato");
assert.ok(venue.venueUuid);
assert.ok(venue.website?.includes("choicehotels.com"));

const censusRows = [
  {
    id: "recEXIST",
    fields: {
      "Property Name": "Comfort Inn Irapuato",
      City: "Irapuato",
      Country: "Mexico",
      "Property Identity Key": "ind_choice_mx_mx092",
    },
  },
];
const matched = matchCventVenueToCensus(venue, censusRows, {
  harvestCountry: "Mexico",
});
assert.equal(matched.ok, true);
assert.ok(nameSimilarity("Comfort Inn Irapuato", "Comfort Inn Irapuato") > 0.9);

const update = buildCventLatamUpdatePatch(
  censusRows[0].fields,
  venue,
  venue.sourceUrl,
  { today: "2026-08-08" }
);
assert.equal(update.ok, true);
assert.equal(update.patch["Rooms / Keys"], 110);
assert.equal(update.patch.Latitude, undefined);

const insert = buildCventCensusOnlyInsertFields(
  {
    ...venue,
    title: "Brand New Cvent Hotel Test",
    venueUuid: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    addressParts: { ...venue.addressParts, city: "Punta Cana", country: "Dominican Republic" },
  },
  {
    harvestCountry: "Dominican Republic",
    sourceUrl: "https://www.cvent.com/venues/x/hotel/y/venue-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    today: "2026-08-08",
  }
);
assert.equal(insert.ok, true);
assert.match(insert.identity_key, /^cvent_/);
assert.equal(
  insert.fields["Production Use Status"],
  INTERNAL_ONLY_INSERT_DEFAULTS["Production Use Status"]
);
assert.equal(insert.fields["Human Review Required"], true);
assert.equal(insert.fields.Latitude, undefined);
assert.ok(!Object.prototype.hasOwnProperty.call(insert.fields, "Opening Date"));

const near = hasNearDuplicateCensusRow(venue, censusRows, {
  harvestCountry: "Mexico",
});
assert.equal(near.near, true);

console.log(
  JSON.stringify(
    {
      ok: true,
      countries_seeded: CVENT_LATAM_CARIBBEAN_COUNTRIES.length,
      client: CVENT_VENUE_CLIENT_VERSION,
      insert_key: insert.identity_key,
      update_keys: Object.keys(update.patch || {}).sort(),
    },
    null,
    2
  )
);
