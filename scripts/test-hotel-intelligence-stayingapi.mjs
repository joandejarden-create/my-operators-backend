#!/usr/bin/env node
/**
 * Unit tests for StayingAPI Hotel Intelligence adapter (no live API calls).
 */
import assert from "node:assert/strict";
import {
  normalizeStayingHotel,
  STAYINGAPI_ROOMS_CAPABILITY,
} from "../lib/hotel-intelligence/providers/stayingapi.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";
import { normalizeProperty } from "../lib/research-engine-v2/providers/staying-api/normalize.js";

assert.equal(STAYINGAPI_ROOMS_CAPABILITY, "NOT_SUPPORTED");

const raw = {
  id: "stay_test_1",
  platform: "booking",
  platformListingId: "123456",
  name: "Test Hotel Cancun",
  propertyType: "hotel",
  url: "https://www.booking.com/hotel/mx/test.html",
  location: {
    address: "Blvd Kukulcan 1",
    city: "Cancun",
    region: "Quintana Roo",
    country: "Mexico",
    postalCode: "77500",
    lat: 21.1,
    lng: -86.8,
  },
  bedrooms: 40,
  maxOccupancy: 80,
  amenities: ["pool"],
  images: ["https://example.com/a.jpg"],
};

const normalized = normalizeProperty(raw);
assert.ok(normalized);
assert.equal(normalized.rooms_capability, "NOT_SUPPORTED");
assert.equal(normalized.rooms_keys, undefined);

const cand = normalizeStayingHotel(normalized);
assert.equal(cand.provider, "stayingapi");
assert.equal(cand.room_count, null);
assert.equal(cand.name, "Test Hotel Cancun");
assert.equal(cand.city, "Cancun");
assert.ok(cand.latitude != null);
assert.equal(cand.raw_safe.booking_com_id, "123456");
assert.equal(cand.raw_safe.rooms_capability, "NOT_SUPPORTED");

const roomsConf = scoreFieldConfidence("room_count", "stayingapi");
assert.equal(roomsConf.confidence, 0);
assert.equal(roomsConf.auto_accept, false);

const geoConf = scoreFieldConfidence("latitude", "stayingapi");
assert.ok(geoConf.confidence >= 0.8);

console.log("ok - stayingapi adapter rooms firewall + normalize");
