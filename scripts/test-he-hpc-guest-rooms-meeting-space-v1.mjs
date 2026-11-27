#!/usr/bin/env node
import assert from "node:assert/strict";
import { buildGuestRoomsMeetingSpaceFromHpc } from "../lib/hotel-intelligence/he-hpc-guest-rooms-meeting-space-v1.js";
import { classifyGapAction } from "../lib/hotel-intelligence/he-explorer-publication-gates-v1.js";

const beth = buildGuestRoomsMeetingSpaceFromHpc(
  {
    "Property Name": "Bethesda Marriott",
    "Rooms / Keys": 407,
    "Guestroom Single Count": 171,
    "Guestroom Double Count": 236,
    "Guestroom Suites Count": 8,
    "Guestroom Mix Confidence": "Brand Materials Reference",
    "Total Meeting Space (sq ft)": 18719,
    "Meeting Rooms Count": 27,
    "Largest Meeting Room Name": "Grand Ballroom",
    "Largest Meeting Room (sq ft)": 4592,
    "Meeting Space Source URL":
      "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
    "Meeting Space Flag": true,
  },
  { hpcId: "recLuxvwwxID7U2B8" },
);

assert.ok(beth);
assert.equal(beth.persisted, true);
assert.equal(beth.guestRooms.totalGuestrooms.value, 407);
assert.equal(beth.guestRooms.suites.value, 8);
assert.equal(beth.meetingSpaceSummary.totalMeetingSpaceSqFt.value, 18719);
assert.equal(beth.meetingRooms.length, 0, "do not invent room rows");

assert.equal(buildGuestRoomsMeetingSpaceFromHpc({}), null);

const sentiment = classifyGapAction({
  tabId: "sentiment",
  fieldKey: "platformIdentities",
  classification: "EXISTING_NOT_CONNECTED",
});
assert.equal(sentiment.action, "DISABLED_BY_FOUNDER");

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:he-hpc-guest-rooms-meeting-space-v1",
  }),
);
