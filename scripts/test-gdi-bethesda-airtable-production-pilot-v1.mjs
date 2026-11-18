#!/usr/bin/env node
import "dotenv/config";
import assert from "node:assert/strict";
import {
  BETHESDA_PILOT_ALLOWLIST,
  BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
  BETHESDA_PILOT_HOTEL_ID,
  BETHESDA_PILOT_DEFERRED_GDOS,
  validateBethesdaPilotAuthorization,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-airtable-production-pilot-v1.js";

assert.equal(BETHESDA_PILOT_ALLOWLIST.length, 4);
assert.equal(BETHESDA_PILOT_DEFERRED_GDOS.length, 3);
assert.ok(
  BETHESDA_PILOT_ALLOWLIST.every((o) =>
    ["Demand Generators", "Private Event Venues"].includes(o.tableName),
  ),
);
assert.ok(
  !BETHESDA_PILOT_ALLOWLIST.some((o) =>
    o.tableName.includes("Group Demand Opportunities"),
  ),
);

const missing = validateBethesdaPilotAuthorization({
  apply: true,
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: "wrong-token",
  hotelId: BETHESDA_PILOT_HOTEL_ID,
});
assert.equal(missing.ok, false);
assert.equal(missing.code, "INVALID_EXACT_WRITE_SET_AUTHORIZATION");

const hotelMismatch = validateBethesdaPilotAuthorization({
  apply: true,
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
  hotelId: "recOTHER",
});
assert.equal(hotelMismatch.ok, false);

const okGate = validateBethesdaPilotAuthorization({
  apply: true,
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
  hotelId: BETHESDA_PILOT_HOTEL_ID,
});
assert.equal(okGate.ok, true);

const dryGate = validateBethesdaPilotAuthorization({
  apply: false,
  authorizeProductionWrites: true,
  exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
  hotelId: BETHESDA_PILOT_HOTEL_ID,
});
assert.equal(dryGate.ok, true);
assert.equal(dryGate.apply, false);

console.log("PASS test:gdi-bethesda-airtable-production-pilot-v1");
