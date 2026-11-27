#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  resolveYearBuiltFromHpcFields,
  resolveLastRenovatedFromHpcFields,
  extractYearFromDateLike,
} from "../lib/hotel-intelligence/he-factory/he-hpc-historical-facts-v1.js";
import { validateHeFieldRegistry } from "../lib/hotel-intelligence/he-factory/he-field-registry-v1.js";
import { formatHotelPropertyCensusAsBrandPresenceHotel } from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { resolveCanonicalHotel } from "../lib/hotel-intelligence/he-factory/he-intelligence-factory-v1.js";

assert.equal(extractYearFromDateLike("1979-01-01"), 1979);
assert.equal(extractYearFromDateLike(2015), 2015);

const yb = resolveYearBuiltFromHpcFields({ "Opening Date": "1979-01-01" });
assert.equal(yb.yearBuilt, 1979);
assert.equal(yb.sourceField, "Opening Date");

const reno = resolveLastRenovatedFromHpcFields(
  {},
  { hotelId: "recLuxvwwxID7U2B8", includeBethesdaPack: true },
);
assert.equal(reno.lastRenovated, 2015);
assert.ok(reno.renovationEvents.length >= 2);

const dto = formatHotelPropertyCensusAsBrandPresenceHotel({
  id: "recLuxvwwxID7U2B8",
  fields: {
    "Property Name": "Bethesda Marriott",
    "Opening Date": "1979-01-01",
    "Renovation / Conversion Date": "2015-01-01",
    "Total Meeting Space (sq ft)": 18719,
    "Rooms / Keys": 407,
  },
});
assert.equal(dto.yearBuilt, 1979, "Radar DTO must map Opening Date → yearBuilt");
assert.equal(dto.lastRenovated, 2015);
assert.equal(dto.totalMeetingSpace, 18719);

const reg = validateHeFieldRegistry();
assert.equal(reg.ok, true, reg.errors.join(","));

const id = resolveCanonicalHotel({ hotelName: "Bethesda Marriott" });
assert.equal(id.ok, true);
assert.equal(id.hpcId, "recLuxvwwxID7U2B8");

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:he-intelligence-factory-v1-1",
    yearBuiltMapped: dto.yearBuilt,
    lastRenovatedMapped: dto.lastRenovated,
  }),
);
