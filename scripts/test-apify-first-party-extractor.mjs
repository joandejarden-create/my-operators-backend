/**
 * Apify first-party extractor gates — no live Actor spend.
 */
import test from "node:test";
import assert from "node:assert/strict";

import {
  SOURCE_CLASS,
  APIFY_USAGE_STATUS,
  LIVE_SOURCE_PRIORITY,
  APIFY_HOTEL_ACTOR_CATALOG,
  COMPANIES_WITHOUT_FIRST_PARTY_ACTOR,
  buildApifyProvenance,
  normalizeApifyHotelRow,
  evaluateActorApproval,
  buildApifyHarvestPatch,
  APPROVAL_THRESHOLDS,
  isUsableOfficialPhone,
} from "../lib/research-engine-v2/apify-first-party-extractor-v1.js";
import { uniqueHighMatches } from "../lib/research-engine-v2/apify-first-party-acquisition-v1.js";
import { ADAPTIVE_PHASES } from "../lib/research-engine-v2/adaptive-overnight-engine-v1.js";
import { MAP_MASTER } from "../lib/research-engine-v2/master-census-enrichment-v1.js";
import { buildCanonicalBrandDictionary } from "../lib/research-engine-v2/census-brand-canonical-dictionary.js";

const hilton = APIFY_HOTEL_ACTOR_CATALOG.find((a) =>
  a.ACTOR_ID.includes("hilton-honors-directory")
);
const ihg = APIFY_HOTEL_ACTOR_CATALOG.find((a) => a.ACTOR_ID.startsWith("axlymxp/"));
const osm = APIFY_HOTEL_ACTOR_CATALOG.find((a) => a.ACTOR_ID.includes("hotels-lodging"));
const graphql = APIFY_HOTEL_ACTOR_CATALOG.find((a) =>
  a.ACTOR_ID.includes("marriott-hotel-search")
);

test("Apify is extraction method, not the origin", () => {
  const p = buildApifyProvenance(hilton);
  assert.equal(p.source_class, SOURCE_CLASS);
  assert.equal(p.underlying_source, "hilton.com");
  assert.equal(p.extraction_method, "Apify Actor");
  assert.equal(p.apify_is_not_the_origin, true);
  assert.match(p.chain, /hilton\.com → Apify Hilton Actor → Dealality/);
});

test("live source priority puts approved Apify extractors before custom crawl", () => {
  assert.equal(LIVE_SOURCE_PRIORITY[0], "approved_bulk_government_registries");
  assert.equal(LIVE_SOURCE_PRIORITY[1], "approved_apify_first_party_extractors");
  assert.equal(LIVE_SOURCE_PRIORITY[3], "direct_official_portfolio_crawling");
  const ids = ADAPTIVE_PHASES.map((p) => p.id);
  assert.ok(ids.indexOf("phase_1b_apify_first_party") < ids.indexOf("phase_2_live_directories"));
});

test("OSM and Google Hotels cannot become first-party approved", () => {
  const gate = evaluateActorApproval(osm, {
    SAMPLE_SIZE: 40,
    HIGH_MATCHES: 40,
    IDENTITY_ACCURACY: 1,
    TECHNICAL_STABILITY: "SUCCEEDED",
    FIELD_SEMANTICS_UNDERSTOOD: true,
    ACCESS_POLICY_OK: true,
  });
  assert.equal(gate.status, APIFY_USAGE_STATUS.CANDIDATE_ONLY);
  assert.equal(gate.ok, false);
});

test("Marriott GraphQL Actor stays usage-review until property rows exist", () => {
  const gate = evaluateActorApproval(graphql, {});
  assert.equal(gate.status, APIFY_USAGE_STATUS.USAGE_REVIEW);
});

test("IHG rooms_available is inventory and must not harvest as Rooms/Keys", () => {
  const row = normalizeApifyHotelRow(ihg, {
    name: "Holiday Inn Cancun",
    brand_name: "Holiday Inn",
    hotel_code: "CUNHI",
    country: "Mexico",
    city: "Cancun",
    rooms_available: 12,
    url: "https://www.ihg.com/holidayinn/hotels/us/en/cancun/cunhi/hoteldetail",
  });
  assert.equal(row.rooms_forbidden, true);
  assert.equal(row.rooms, null);
  const dictionary = buildCanonicalBrandDictionary({});
  const built = buildApifyHarvestPatch(
    { [MAP_MASTER.country]: "Mexico" },
    row,
    { dictionary, roomsApproved: true, coordsApproved: false }
  );
  assert.equal(built.counts.ROOMS_WRITES, 0);
  assert.equal(built.patch[MAP_MASTER.roomsKeys], undefined);
});

test("Hilton total_rooms can HIGH-fill when identity HIGH and semantics approved", () => {
  const row = normalizeApifyHotelRow(hilton, {
    property_name: "Hilton Cancun",
    brand_name: "Hilton Hotels & Resorts",
    hilton_ctyhocn: "CUNCICI",
    country: "Mexico",
    city: "Cancun",
    total_rooms: 180,
    lat: 21.13,
    lng: -86.75,
  });
  assert.equal(row.rooms, 180);
  assert.equal(row.rooms_forbidden, false);
  const dictionary = buildCanonicalBrandDictionary({});
  const built = buildApifyHarvestPatch(
    {
      [MAP_MASTER.country]: "Mexico",
      [MAP_MASTER.city]: "Cancun",
    },
    row,
    { dictionary, roomsApproved: true, coordsApproved: true }
  );
  assert.equal(built.counts.ROOMS_WRITES, 1);
  assert.equal(built.patch[MAP_MASTER.roomsKeys], 180);
  assert.equal(built.counts.COORDINATE_PATCHES, 1);
  assert.equal(built.patch[MAP_MASTER.coordinateSourceType], "official_coordinates");
  assert.equal(built.patch[MAP_MASTER.geocodeProvider], "Official Page");
});

test("NULL_FILL does not overwrite existing Rooms", () => {
  const row = normalizeApifyHotelRow(hilton, {
    property_name: "Hilton Cancun",
    brand_name: "Hilton Hotels & Resorts",
    hilton_ctyhocn: "CUNCICI",
    country: "Mexico",
    total_rooms: 200,
  });
  const dictionary = buildCanonicalBrandDictionary({});
  const built = buildApifyHarvestPatch(
    { [MAP_MASTER.roomsKeys]: 180, [MAP_MASTER.country]: "Mexico" },
    row,
    { dictionary, roomsApproved: true }
  );
  assert.equal(built.patch[MAP_MASTER.roomsKeys], undefined);
});

test("approval gate rejects small/unmatched samples", () => {
  const gate = evaluateActorApproval(hilton, {
    SAMPLE_SIZE: 5,
    HIGH_MATCHES: 1,
    IDENTITY_ACCURACY: 0.2,
    TECHNICAL_STABILITY: "SUCCEEDED",
    FIELD_SEMANTICS_UNDERSTOOD: true,
    ACCESS_POLICY_OK: true,
  });
  assert.equal(gate.ok, false);
  assert.ok(gate.reasons.includes("sample_too_small"));
});

test("approval gate can approve first-party when metrics pass", () => {
  const gate = evaluateActorApproval(hilton, {
    SAMPLE_SIZE: 40,
    HIGH_MATCHES: 20,
    IDENTITY_ACCURACY: 0.9,
    BRAND_COMPARED: 10,
    BRAND_ACCURACY: 1,
    TECHNICAL_STABILITY: "SUCCEEDED",
    FIELD_SEMANTICS_UNDERSTOOD: true,
    ACCESS_POLICY_OK: true,
    ROOMS_COMPARED: 8,
    ROOM_ACCURACY: 0.9,
    ROOMS_SEMANTICS_GUESTROOM: true,
    COORDS_COMPARED: 8,
    COORDINATE_ACCURACY: 0.9,
  });
  assert.equal(gate.ok, true);
  assert.equal(gate.status, APIFY_USAGE_STATUS.APPROVED);
  assert.equal(gate.rooms_approved, true);
  assert.equal(gate.coords_approved, true);
});

test("HIGH identity requires country + name/code match", () => {
  const actor = hilton;
  const rows = [
    normalizeApifyHotelRow(actor, {
      property_name: "Hilton Cancun",
      brand_name: "Hilton",
      hilton_ctyhocn: "CUNCICI",
      country: "Mexico",
      city: "Cancun",
    }),
  ];
  const census = [
    {
      id: "rec1",
      fields: {
        [MAP_MASTER.propertyName]: "Hilton Cancun",
        [MAP_MASTER.country]: "Mexico",
        [MAP_MASTER.city]: "Cancun",
        [MAP_MASTER.officialUrl]: "https://www.hilton.com/en/hotels/cuncici-hilton-cancun/",
      },
    },
  ];
  const matches = uniqueHighMatches(rows, census);
  assert.equal(matches.length, 1);
  assert.equal(matches[0].rec.id, "rec1");
});

test("central-reservation placeholder phones are not harvested", () => {
  assert.equal(isUsableOfficialPhone("800 000 04 04"), false);
  assert.equal(isUsableOfficialPhone("+52 998 881 0800"), true);
});

test("catalog records companies still needing custom crawl", () => {
  assert.ok(COMPANIES_WITHOUT_FIRST_PARTY_ACTOR.includes("Hyatt"));
  assert.ok(COMPANIES_WITHOUT_FIRST_PARTY_ACTOR.includes("Wyndham"));
  assert.ok(
    APIFY_HOTEL_ACTOR_CATALOG.some((a) => a.ACTOR_ID.includes("fourseasons-properties"))
  );
  assert.ok(APIFY_HOTEL_ACTOR_CATALOG.some((a) => a.ACTOR_ID.includes("accor-urls-scraper")));
  assert.ok(APPROVAL_THRESHOLDS.min_high_matches >= 5);
});
