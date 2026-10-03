/**
 * Tests: Hotel ADP Attributes builder idempotency + use classification.
 */
import assert from "node:assert/strict";
import {
  buildAttributeDedupeKey,
  HOTEL_ADP_ATTRIBUTE_VERSION,
} from "../lib/hotel-intelligence/adp-attributes/field-map.js";
import {
  buildAdpHotelAttributes,
  classifyAdpUse,
} from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";

{
  const a = buildAttributeDedupeKey("recLuxvwwxID7U2B8", "Rooms", "adp_attr_v1");
  const b = buildAttributeDedupeKey("recLuxvwwxID7U2B8", "Rooms", "adp_attr_v1");
  assert.equal(a, b);
  assert.equal(a, `recLuxvwwxID7U2B8::rooms::adp_attr_v1`);
  const withCat = buildAttributeDedupeKey(
    "recLuxvwwxID7U2B8",
    "Rooms",
    "adp_attr_v1",
    "Commercial"
  );
  assert.equal(withCat, `recLuxvwwxID7U2B8::commercial::rooms::adp_attr_v1`);
}

{
  const use = classifyAdpUse("Total Meeting Space Sq Ft");
  assert.equal(use.usedInAdp, true);
  assert.ok(use.adpUseType.includes("Prompt Context"));
}

{
  const use = classifyAdpUse("Public Seasonality High Season");
  assert.equal(use.usedInAdp, false);
}

{
  const profile = await buildHotelIntelligenceProfile("recLuxvwwxID7U2B8", {
    skipLiveHpc: true,
  });
  assert.equal(profile.ok, true);
  assert.equal(profile.hotelId, "recLuxvwwxID7U2B8");
  assert.equal(profile.identity.adpPropertyId, "adp_bethesda_marriott");
}

{
  const packet = await buildAdpHotelAttributes("recLuxvwwxID7U2B8", {
    skipLiveHpc: true,
  });
  assert.equal(packet.ok, true);
  assert.ok(packet.attributes.length >= 10);
  const rooms = packet.attributes.find((a) => a.attributeName === "Rooms");
  assert.ok(rooms);
  assert.equal(String(rooms.attributeValue), "407");
  assert.equal(rooms.usedInAdp, true);
  assert.ok(rooms.adpUseType.includes("Prompt Context"));

  const keys = packet.attributes.map((a) => a.dedupeKey);
  assert.equal(new Set(keys).size, keys.length, "duplicate dedupe keys");

  const packet2 = await buildAdpHotelAttributes("recLuxvwwxID7U2B8", {
    skipLiveHpc: true,
  });
  assert.deepEqual(
    packet.attributes.map((a) => a.dedupeKey).sort(),
    packet2.attributes.map((a) => a.dedupeKey).sort()
  );

  // Other US hotel must not inherit Bethesda rooms/demand blindly as Bethesda-only names
  const ren = await buildAdpHotelAttributes("recG66DQJKP2c0UNh", { skipLiveHpc: true });
  assert.equal(ren.ok, true);
  assert.notEqual(ren.hotelName, packet.hotelName);
  const renRooms = ren.attributes.find((a) => a.attributeName === "Rooms");
  if (renRooms) assert.notEqual(String(renRooms.attributeValue), "407");
}

console.log("test:hotel-adp-attributes OK", {
  attributeVersion: HOTEL_ADP_ATTRIBUTE_VERSION,
});
