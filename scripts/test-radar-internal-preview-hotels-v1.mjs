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
  isHpcRadarStatusVisible,
  listInternalPreviewCensusIds,
  mergeInternalPreviewHotelsIntoBrandPresenceList,
  RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
  resolveRadarStatusFromHpcFields,
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

// Radar Status mapping — Open / Pipeline visible; Hidden/blank excluded
assert.equal(resolveRadarStatusFromHpcFields({ "Radar Status": "Open" }), "Open");
assert.equal(resolveRadarStatusFromHpcFields({ "Radar Status": "Pipeline" }), "Pipeline");
assert.equal(resolveRadarStatusFromHpcFields({ "Radar Status": "Hidden" }), null);
assert.equal(resolveRadarStatusFromHpcFields({}), null);
assert.equal(isHpcRadarStatusVisible({ "Radar Status": "Open" }), true);
assert.equal(isHpcRadarStatusVisible({ "Radar Status": "Hidden" }), false);
assert.equal(isHpcRadarStatusVisible({}), false);

// Governance labels never become Radar Status
assert.equal(
  resolveRadarStatusFromHpcFields({
    "Production Use Status": "Census Only / Not Owner-Facing",
    status: "Census Only / Not Owner-Facing",
  }),
  null
);
assert.equal(
  resolveRadarStatusFromHpcFields({
    "Production Use Status": "Open",
    "Radar Status": "Hidden",
  }),
  null,
  "Production Use Status Open must not override Radar Status Hidden"
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
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Radar Status": "Open",
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
assert.equal(dto.status, "Open");
assert.equal(dto.radarStatus, "Open");
assert.equal(dto.productionUseStatus, "Census Only / Not Owner-Facing");
assert.ok(dto.lat && dto.lng);

const pipelineDto = formatHotelPropertyCensusAsBrandPresenceHotel({
  id: "recPIPE1111111111",
  fields: {
    "Property Name": "Pipeline Example",
    "Radar Status": "Pipeline",
    "Production Use Status": "Census Only / Not Owner-Facing",
    Latitude: 1,
    Longitude: 2,
  },
});
assert.equal(pipelineDto.status, "Pipeline");

const hiddenDto = formatHotelPropertyCensusAsBrandPresenceHotel({
  id: "recHIDE1111111111",
  fields: {
    "Property Name": "Hidden Example",
    "Radar Status": "Hidden",
    Latitude: 5,
    Longitude: 6,
  },
});
assert.equal(hiddenDto.status, "Hidden");

const existing = [
  {
    id: "recAAAA1111111111",
    name: "Casas Del XVI",
    brand: "Independent",
    city: "Santo Domingo",
    country: "Dominican Republic",
  },
];
const merged = mergeInternalPreviewHotelsIntoBrandPresenceList(existing, [
  dto,
  dto,
  hiddenDto,
]);
assert.equal(merged.hotels.length, 2, "no duplicate; Hidden excluded");
assert.deepEqual(merged.addedIds, [BETHESDA]);
assert.equal(merged.hotels[0].name, "Casas Del XVI", "existing names preserved");

// Same canonical id already on Hotel Census → no duplicate pin
const alreadyOnMap = mergeInternalPreviewHotelsIntoBrandPresenceList(
  [{ id: BETHESDA, name: "Bethesda from Hotel Census", status: "Open" }],
  [dto]
);
assert.equal(alreadyOnMap.hotels.length, 1);
assert.deepEqual(alreadyOnMap.addedIds, []);

const legacyStyle = formatHotelPropertyCensusAsBrandPresenceHotel({
  id: "recBBBB2222222222",
  fields: {
    name: "Agualina Kite Resort",
    city: "Cabarete",
    country: "Dominican Republic",
    Affiliation: "Independent",
    Latitude: 19.76,
    Longitude: -70.42,
    "Radar Status": "Open",
  },
});
assert.equal(legacyStyle.name, "Agualina Kite Resort");
assert.equal(legacyStyle.brand, "Independent");
assert.equal(legacyStyle.status, "Open");

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-internal-preview-hotels-v1",
    version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
    bethesdaAllowlisted: true,
    queryAloneUnauthorized: true,
    noDuplicateMerge: true,
    radarStatusModel: "Open|Pipeline|Hidden",
  })
);
