#!/usr/bin/env node
/**
 * Regression: city name cannot select a property; paid work requires
 * explicit hotel name + canonical ID + product + authorization match.
 */
import assert from "node:assert/strict";
import {
  assertHotelExecutionScope,
  isCityOnlyPropertySelector,
  CITY_ONLY_BLOCK_REASON,
} from "../lib/group-demand-intelligence/workflow/hotel-execution-scope-preflight-v1.js";

const westinCfg = {
  hotelId: "recdmo0CnhIZAwHGC",
  displayName: "The Westin Tampa Waterside",
  city: "Tampa",
  market: "Tampa",
  identity: { city: "Tampa", market: "Tampa", hotelName: "The Westin Tampa Waterside" },
};

assert.equal(isCityOnlyPropertySelector("Tampa", westinCfg), true);
assert.equal(isCityOnlyPropertySelector("tampa", westinCfg), true);
assert.equal(isCityOnlyPropertySelector("Tampa FL", westinCfg), true);
assert.equal(
  isCityOnlyPropertySelector("The Westin Tampa Waterside", westinCfg),
  false
);

const cityBlock = assertHotelExecutionScope({
  hotelId: "recdmo0CnhIZAwHGC",
  approvedHotelId: "recdmo0CnhIZAwHGC",
  approvedHotelName: "Tampa",
  product: "GDI",
  approvedProduct: "GDI",
  authorizationRef: "auth_gdi_westin_tampa_waterside_6usd_v1",
  approvedAuthorizationId: "auth_gdi_westin_tampa_waterside_6usd_v1",
  hotelConfig: westinCfg,
  requirePaidGate: true,
});
assert.equal(cityBlock.ok, false);
assert.ok(cityBlock.blockers.includes(CITY_ONLY_BLOCK_REASON));

const idMismatch = assertHotelExecutionScope({
  hotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelId: "recdmo0CnhIZAwHGC",
  approvedHotelName: "The Westin Tampa Waterside",
  product: "GDI",
  authorizationRef: "auth_gdi_westin_tampa_waterside_6usd_v1",
  hotelConfig: { hotelId: "recSM9ARs8U9UZ3AZ", displayName: "Tampa Airport Marriott", city: "Tampa" },
  requirePaidGate: true,
});
assert.equal(idMismatch.ok, false);
assert.ok(idMismatch.blockers.includes("canonical_hotel_id_mismatch"));

const airportSuperseded = assertHotelExecutionScope({
  hotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelName: "Tampa Airport Marriott",
  product: "GDI",
  authorizationRef: "auth_gdi_airport_withdrawn",
  hotelConfig: {
    hotelId: "recSM9ARs8U9UZ3AZ",
    displayName: "Tampa Airport Marriott",
    city: "Tampa",
  },
  requirePaidGate: true,
});
assert.equal(airportSuperseded.ok, false);
assert.ok(airportSuperseded.blockers.includes("tampa_airport_marriott_superseded"));

const airportReinstated = assertHotelExecutionScope({
  hotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelName: "Tampa Airport Marriott",
  product: "GDI",
  authorizationRef: "auth_gdi_tampa_airport_marriott_12usd_v1",
  approvedAuthorizationId: "auth_gdi_tampa_airport_marriott_12usd_v1",
  hotelConfig: {
    hotelId: "recSM9ARs8U9UZ3AZ",
    displayName: "Tampa Airport Marriott",
    city: "Tampa",
  },
  requirePaidGate: true,
  tampaAirportReinstatementAuthorized: true,
  forbiddenHotelIds: [],
});
assert.equal(airportReinstated.ok, true);
assert.deepEqual(airportReinstated.blockers, []);

// Auto-resolve reinstatement from portfolio auth + identity separation (no boolean)
const airportAuto = assertHotelExecutionScope({
  hotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelId: "recSM9ARs8U9UZ3AZ",
  approvedHotelName: "Tampa Airport Marriott",
  product: "GDI",
  authorizationRef: "auth_gdi_tampa_airport_marriott_12usd_v1",
  approvedAuthorizationId: "auth_gdi_tampa_airport_marriott_12usd_v1",
  hotelConfig: {
    hotelId: "recSM9ARs8U9UZ3AZ",
    displayName: "Tampa Airport Marriott",
    city: "Tampa",
  },
  requirePaidGate: true,
});
assert.equal(airportAuto.ok, true, "portfolio auth ref must auto-clear superseded");

const ok = assertHotelExecutionScope({
  hotelId: "recdmo0CnhIZAwHGC",
  approvedHotelId: "recdmo0CnhIZAwHGC",
  approvedHotelName: "The Westin Tampa Waterside",
  product: "GDI",
  approvedProduct: "GDI",
  authorizationRef: "auth_gdi_westin_tampa_waterside_6usd_v1",
  approvedAuthorizationId: "auth_gdi_westin_tampa_waterside_6usd_v1",
  hotelConfig: westinCfg,
  requirePaidGate: true,
  forbiddenHotelIds: ["recSM9ARs8U9UZ3AZ"],
});
assert.equal(ok.ok, true);
assert.deepEqual(ok.blockers, []);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-hotel-execution-scope-preflight-v1",
      cityOnlyBlockReason: CITY_ONLY_BLOCK_REASON,
      airportReinstatementPath: true,
    },
    null,
    2
  )
);
