/**
 * Unit checks for HPC Brand Presence adapter (no Airtable).
 */
import assert from "node:assert/strict";
import {
  deriveHpcBrandDisplay,
  deriveHpcLifecycleStatus,
  deriveHpcProductRooms,
  formatHpcHotelRecord,
  isBrandPresenceHpcV2Enabled,
} from "../lib/hotel-census/brand-presence-hpc-adapter.js";
import { shouldUseHpcBrandPresence } from "../lib/hotel-census/brand-presence-hpc-request.js";

assert.equal(isBrandPresenceHpcV2Enabled({ BRAND_PRESENCE_HPC_V2: "1" }), true);
assert.equal(isBrandPresenceHpcV2Enabled({ BRAND_PRESENCE_HPC_V2: "0" }), false);

const envOn = { BRAND_PRESENCE_HPC_V2: "1" };
assert.equal(
  shouldUseHpcBrandPresence({ query: {}, headers: {} }, envOn),
  false,
  "flag alone must not switch Scout/Radar"
);
assert.equal(
  shouldUseHpcBrandPresence({ query: { censusSource: "hpc" }, headers: {} }, envOn),
  true
);
assert.equal(
  shouldUseHpcBrandPresence(
    { query: { product: "hotel-explorer" }, headers: {} },
    envOn
  ),
  true
);
assert.equal(
  shouldUseHpcBrandPresence(
    { query: { limit: "100000" }, headers: {} },
    envOn
  ),
  false,
  "Radar/Scout bulk fetch stays Legacy"
);
assert.equal(
  shouldUseHpcBrandPresence({ query: { censusSource: "hpc" }, headers: {} }, {
    BRAND_PRESENCE_HPC_V2: "0",
  }),
  false
);

assert.equal(
  deriveHpcLifecycleStatus({ "Future Opening Flag": true }),
  "Pipeline"
);
assert.equal(
  deriveHpcLifecycleStatus({ "Production Use Status": "Do Not Use" }),
  "Closed"
);
assert.equal(deriveHpcLifecycleStatus({}), "Open");

assert.equal(
  deriveHpcBrandDisplay({ "Affiliation Status": "Independent" }),
  "Independent"
);
assert.equal(
  deriveHpcBrandDisplay({
    "Affiliation Status": "Branded",
    "Current Brand": "Hyatt Place",
  }),
  "Hyatt Place"
);

assert.equal(
  deriveHpcProductRooms({
    "Rooms / Keys": 120,
    "Rooms Confidence": "High",
    "Rooms Source Type": "Legacy Census",
  }),
  null
);
assert.equal(
  deriveHpcProductRooms({
    "Rooms / Keys": 120,
    "Rooms Confidence": "High",
    "Rooms Source Type": "official_brand_directory",
  }),
  120
);
assert.equal(
  deriveHpcProductRooms({
    "Rooms / Keys": 41,
    "Rooms Confidence": "High",
    "Rooms Source Type": "trusted_secondary_source",
    "Rooms Source URL": "https://dados.turismo.gov.br/dataset/meios-de-hospedagem",
  }),
  41
);
assert.equal(
  deriveHpcBrandDisplay({ "Affiliation Status": "Unknown" }),
  null
);
assert.equal(
  deriveHpcBrandDisplay({}),
  null
);

const dto = formatHpcHotelRecord({
  id: "recTEST",
  fields: {
    "Property Name": "Test Hotel",
    "Dealality Hotel ID": "dhl_TEST",
    "Affiliation Status": "Independent",
    Country: "Mexico",
    City: "Cancun",
    Latitude: 21.1,
    Longitude: -86.8,
    "Rooms / Keys": 50,
    "Rooms Confidence": "Medium",
    "Rooms Source Type": "Legacy Census",
  },
});
assert.equal(dto.rooms, null);
assert.equal(dto.roomsDisplay, "Unknown");
assert.equal(dto.strNumber, null);
assert.equal(dto.chainScale, null);
assert.equal(dto._source, "hotel_property_census");
assert.equal(dto._provenanceFlags.no_legacy_fallback, true);

console.log("test:brand-presence-hpc-adapter OK");
