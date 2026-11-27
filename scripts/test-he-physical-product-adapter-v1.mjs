#!/usr/bin/env node
/**
 * Gate: HE physical-product adapter prefers HPC SoT over staged overlay.
 */
import assert from "node:assert/strict";
import {
  inventoryFromHpcFields,
  resolvePhysicalProductInventory,
  HE_PHYSICAL_PRODUCT_ADAPTER_V1,
} from "../lib/hotel-intelligence/he-physical-product-adapter-v1.js";
import {
  BETHESDA_PHYSICAL_PRODUCT_VALUES,
  HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE,
  assertBethesdaPhysicalProductInvariants,
} from "../lib/hotel-intelligence/he-physical-product-schema-v1.js";
import {
  HE50_ALLOWED_DESTINATION_TABLES,
  isFieldAllowedOnDestination,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";
import { HE50_HPC_TABLE_ID } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";

assert.equal(HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE.length, 9);
assertBethesdaPhysicalProductInvariants(BETHESDA_PHYSICAL_PRODUCT_VALUES.fields);

for (const spec of HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE) {
  assert.equal(
    isFieldAllowedOnDestination(HE50_HPC_TABLE_ID, spec.name),
    true,
    `standing allowlist missing ${spec.name}`,
  );
}
assert.ok(HE50_ALLOWED_DESTINATION_TABLES[HE50_HPC_TABLE_ID]);

const fromHpc = inventoryFromHpcFields(BETHESDA_PHYSICAL_PRODUCT_VALUES.fields, {
  hpcId: BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId,
  hotelName: "Bethesda Marriott",
});
assert.ok(fromHpc);
assert.equal(fromHpc.sourceOfTruth, "HOTEL_PROPERTY_CENSUS");
assert.equal(fromHpc.adapter, HE_PHYSICAL_PRODUCT_ADAPTER_V1);
assert.equal(fromHpc.guestRooms.totalGuestrooms.value, 407);
assert.equal(fromHpc.guestRooms.singleBedRooms.value, 171);
assert.equal(fromHpc.guestRooms.doubleBedRooms.value, 236);
assert.equal(fromHpc.guestRooms.suites.value, 8);
assert.equal(fromHpc.meetingSpaceSummary.totalMeetingSpaceSqFt.value, 18719);
assert.equal(fromHpc.meetingSpaceSummary.meetingRoomCount.value, 27);
assert.equal(fromHpc.meetingSpaceSummary.largestRoom.name, "Grand Ballroom");
assert.equal(fromHpc.meetingSpaceSummary.largestRoom.areaSqFt, 4592);
assert.notEqual(fromHpc.meetingSpaceSummary.totalMeetingSpaceSqFt.value, 19000);

const resolved = resolvePhysicalProductInventory(
  BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId,
  BETHESDA_PHYSICAL_PRODUCT_VALUES.fields,
  { hotelName: "Bethesda Marriott" },
);
assert.equal(resolved.sourceOfTruth, "HOTEL_PROPERTY_CENSUS");
assert.equal(resolved.meetingSpaceSummary.totalMeetingSpaceSqFt.value, 18719);
assert.ok(
  resolved.stagedDetailRole == null ||
    resolved.stagedDetailRole === "MEETING_ROOM_ROW_INVENTORY_AND_MARKETING_FIGURE_ONLY",
);

const empty = resolvePhysicalProductInventory("recUnknownXXXX", {}, { preferProposedOverlay: false });
assert.equal(empty, null);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:he-physical-product-adapter-v1",
      certifications: {
        PHYSICAL_PRODUCT_SCHEMA_CONTRACT: true,
        HPC_ADAPTER_SOT: true,
        MARKETING_19000_EXCLUDED: true,
        STANDING_AUTH_FIELDS_ALLOWLISTED: true,
        SUITES_NOT_ADDITIVE: true,
      },
    },
    null,
    2,
  ),
);
