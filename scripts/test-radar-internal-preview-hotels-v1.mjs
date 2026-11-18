#!/usr/bin/env node
/**
 * Deterministic unit gate for Radar internal-preview hotel inclusion.
 * No Airtable / no network.
 */
import assert from "node:assert/strict";
import {
  assertInternalPreviewQueryDoesNotAuthorize,
  formatHotelPropertyCensusAsBrandPresenceHotel,
  isAuthorizedForRadarInternalPreviewHotels,
  listInternalPreviewCensusIds,
  mergeInternalPreviewHotelsIntoBrandPresenceList,
  RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

const BETHESDA = "recLuxvwwxID7U2B8";

assert.equal(listInternalPreviewCensusIds().includes(BETHESDA), true);

const queryGate = assertInternalPreviewQueryDoesNotAuthorize({
  internalPreview: "1",
});
assert.equal(queryGate.queryRequestsInternalPreview, true);
assert.equal(queryGate.authorizedByQueryAlone, false);

assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(null, {}),
  false,
  "missing user never authorized"
);
assert.equal(
  isAuthorizedForRadarInternalPreviewHotels({ isAdmin: false }, {}),
  false,
  "non-admin never authorized by default"
);
assert.equal(
  isAuthorizedForRadarInternalPreviewHotels({ isAdmin: true }, {}),
  true,
  "platform admin authorized"
);

// DEV_AUTH_BYPASS local admin requires intentional owner-app header
assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(
    {
      isAdmin: true,
      userSource: "LOCAL_DEV_AUTH_BYPASS",
      memberstackId: "dev_bypass",
    },
    {}
  ),
  false,
  "local bypass without owner-app header denied"
);
assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(
    {
      isAdmin: true,
      userSource: "LOCAL_DEV_AUTH_BYPASS",
      memberstackId: "dev_bypass",
    },
    { headers: { "x-dealality-owner-app": "1" } }
  ),
  true,
  "local bypass with owner-app header allowed"
);

const hpcRecord = {
  id: BETHESDA,
  fields: {
    "Property Name": "Bethesda Marriott",
    City: "Bethesda",
    Country: "United States",
    "Current Brand": "Marriott Hotels",
    Latitude: 39.016519,
    Longitude: -77.103463,
    Market: "Bethesda / Montgomery County",
    Submarket: "Pooks Hill / Rockville Pike",
    "Rooms / Keys": 407,
  },
};

const dto = formatHotelPropertyCensusAsBrandPresenceHotel(hpcRecord);
assert.equal(dto.id, BETHESDA);
assert.equal(dto.name, "Bethesda Marriott");
assert.equal(dto.city, "Bethesda");
assert.equal(dto.country, "United States");
assert.equal(dto.brand, "Marriott Hotels");
assert.equal(dto.internalPreviewOnly, true);
assert.equal(dto.radarInclusion, "AUTHORIZED_INTERNAL_PREVIEW");
assert.ok(dto.lat && dto.lng);

const existing = [
  {
    id: "recAAAA1111111111",
    name: "Casas Del XVI",
    brand: "Independent",
    city: "Santo Domingo",
    country: "Dominican Republic",
  },
];
const merged = mergeInternalPreviewHotelsIntoBrandPresenceList(existing, [dto, dto]);
assert.equal(merged.hotels.length, 2, "no duplicate hotel records");
assert.deepEqual(merged.addedIds, [BETHESDA]);
assert.equal(merged.hotels[0].name, "Casas Del XVI", "existing names preserved");

// Alias formatter must not invent when legacy fields already correct
const legacyStyle = formatHotelPropertyCensusAsBrandPresenceHotel({
  id: "recBBBB2222222222",
  fields: {
    name: "Agualina Kite Resort",
    city: "Cabarete",
    country: "Dominican Republic",
    Affiliation: "Independent",
    Latitude: 19.76,
    Longitude: -70.42,
  },
});
assert.equal(legacyStyle.name, "Agualina Kite Resort");
assert.equal(legacyStyle.brand, "Independent");

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-internal-preview-hotels-v1",
    version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
    bethesdaAllowlisted: true,
    queryAloneUnauthorized: true,
    noDuplicateMerge: true,
  })
);
