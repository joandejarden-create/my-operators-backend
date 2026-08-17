#!/usr/bin/env node
/**
 * Unit tests for SerpApi Hotel Intelligence adapter (no live API calls).
 */
import assert from "node:assert/strict";
import {
  normalizeSerpApiHotel,
  SERPAPI_ROOMS_CAPABILITY,
  resolveGl,
} from "../lib/hotel-intelligence/providers/serpapi.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";
import { normalizeGoogleHotelProperty } from "../lib/research-engine-v2/providers/serpapi-google-hotels/normalize.js";
import { MAP_PROVIDER_IDS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";

assert.equal(SERPAPI_ROOMS_CAPABILITY, "NOT_SUPPORTED");
assert.equal(MAP_PROVIDER_IDS.serpapi, "serpapi");
assert.equal(resolveGl("Mexico"), "mx");
assert.equal(resolveGl("Dominican Republic"), "do");

const raw = {
  name: "Test Hotel Cancun",
  property_token: "CgoI_test_token_EAE",
  address: "Blvd Kukulcan Km 12, 77500 Cancún, Q.R., Mexico",
  phone: "+52 998 000 0000",
  link: "https://www.testhotel.com/",
  gps_coordinates: { latitude: 21.1, longitude: -86.8 },
  hotel_class: "4-star hotel",
  amenities: ["Pool", "Spa"],
  rooms: [{ name: "King Room" }, { name: "Suite" }],
  essential_info: ["2 bedrooms"],
};

const normalized = normalizeGoogleHotelProperty(raw, { source: "unit_test" });
assert.ok(normalized);
assert.equal(normalized.rooms_capability, "NOT_SUPPORTED");
assert.equal(normalized.rooms_keys, undefined);

const cand = normalizeSerpApiHotel(normalized);
assert.equal(cand.provider, "serpapi");
assert.equal(cand.room_count, null);
assert.equal(cand.name, "Test Hotel Cancun");
assert.equal(cand.phone, "+52 998 000 0000");
assert.equal(cand.website, "https://www.testhotel.com/");
assert.ok(cand.latitude != null);
assert.equal(cand.raw_safe.serpapi_property_token, "CgoI_test_token_EAE");
assert.equal(cand.raw_safe.google_place_id, null);
assert.equal(cand.raw_safe.rooms_capability, "NOT_SUPPORTED");

const roomsConf = scoreFieldConfidence("room_count", "serpapi");
assert.equal(roomsConf.confidence, 0);
assert.equal(roomsConf.auto_accept, false);

const geoConf = scoreFieldConfidence("latitude", "serpapi");
assert.ok(geoConf.confidence >= 0.85);

const phoneConf = scoreFieldConfidence("phone", "serpapi");
assert.ok(phoneConf.confidence >= 0.8);

console.log("ok - serpapi adapter rooms firewall + normalize");
