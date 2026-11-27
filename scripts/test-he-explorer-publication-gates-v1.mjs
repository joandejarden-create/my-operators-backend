#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  HE50_ACCEPTANCE_GATES,
  HE50_EIGHT_HOTEL_HPC_IDS,
  HE_PUBLICATION_SEQUENCE,
  buildHe50AcceptanceSnapshot,
  classifyGapAction,
} from "../lib/hotel-intelligence/he-explorer-publication-gates-v1.js";
import { HE_ACTIVE_TAB_IDS } from "../lib/hotel-intelligence/he-canonical-explorer-assets-v1.js";

assert.equal(HE_PUBLICATION_SEQUENCE.length, 10);
assert.equal(HE50_EIGHT_HOTEL_HPC_IDS.length, 8);
assert.ok(HE50_EIGHT_HOTEL_HPC_IDS.includes("recLuxvwwxID7U2B8"));
assert.equal(HE_ACTIVE_TAB_IDS.length, 11);

const snap = buildHe50AcceptanceSnapshot({
  guestSentimentDisabled: true,
  elevenTabParity: true,
});
assert.equal(snap.gates[HE50_ACCEPTANCE_GATES.GUEST_SENTIMENT_DISABLED_BY_FOUNDER], "YES");
assert.equal(snap.gates[HE50_ACCEPTANCE_GATES.ELEVEN_TAB_COMPONENT_PARITY], "YES");
assert.equal(snap.gates[HE50_ACCEPTANCE_GATES.BETHESDA_FULL_LIVE_VALIDATED], "NO");
assert.equal(snap.gates[HE50_ACCEPTANCE_GATES.FULL_HE_E2E_VALIDATED], "NO");
assert.equal(snap.hotelTabAcceptanceDenomination, 88);

assert.equal(
  classifyGapAction({ classification: "EXISTING_NOT_CONNECTED" }).action,
  "CONNECT_PERSIST_READBACK",
);
assert.equal(
  classifyGapAction({ classification: "EXISTING_VERIFIED" }).paidResearch,
  false,
);
assert.equal(
  classifyGapAction({ tabId: "sentiment", fieldKey: "platformIdentities" }).certification,
  "DISABLED_BY_FOUNDER",
);

// Never auto-YES full e2e without explicit evidence + all prerequisites.
const premature = buildHe50AcceptanceSnapshot({ fullHeE2eValidated: true });
assert.equal(premature.gates[HE50_ACCEPTANCE_GATES.FULL_HE_E2E_VALIDATED], "NO");

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:he-explorer-publication-gates-v1",
      activeTabs: HE_ACTIVE_TAB_IDS.length,
      eightHotels: HE50_EIGHT_HOTEL_HPC_IDS.length,
      acceptance88: snap.hotelTabAcceptanceDenomination,
    },
    null,
    2,
  ),
);
