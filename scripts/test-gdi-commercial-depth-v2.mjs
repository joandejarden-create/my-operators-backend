#!/usr/bin/env node
/**
 * Unit tests — GDI Commercial Depth V2 + ADP capability-layer merge.
 */
import assert from "node:assert/strict";
import {
  classifyLodgingPrimaryMotion,
  extractLodgingSignalsFromText,
  evaluateActionableNow,
  buildLodgingWhyNow,
  buildLodgingWhyHotel,
  LODGING_MOTION,
} from "../lib/group-demand-intelligence/commercial-depth-v2.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";

const cfg = {
  displayName: "Hilton New York Times Square",
  capabilityProfile: { totalGuestrooms: 478, totalMeetingSpaceSqFt: 300 },
};

assert.equal(
  classifyLodgingPrimaryMotion({ title: "FIFA World Cup 2026" }, cfg),
  LODGING_MOTION.SPORTS_HOUSING
);
assert.equal(
  classifyLodgingPrimaryMotion(
    { title: "CDA Annual Meeting 2026", organizationName: "Colonial Dames" },
    cfg
  ),
  LODGING_MOTION.ASSOCIATION_HOUSING
);
assert.notEqual(
  classifyLodgingPrimaryMotion(
    { title: "CDA Annual Meeting 2026", organizationName: "Colonial Dames" },
    cfg
  ),
  LODGING_MOTION.FULL_MEETING_RFP
);

const sig = extractLodgingSignalsFromText(
  "Official housing portal. Hotel block available. Expected attendance: 2,500. Host hotel: Example Hotel."
);
assert.equal(sig.roomBlockMentioned, true);
assert.equal(sig.housingPageFound, true);
assert.equal(sig.attendance, 2500);
assert.equal(sig.hostHotelMentioned, true);

const whyNow = buildLodgingWhyNow({ eventYear: "2026" }, { roomBlockMentioned: false });
assert.match(whyNow, /Monitor/);
assert.doesNotMatch(whyNow, /^Future cycle 2026\.?$/i);

const whyHotel = buildLodgingWhyHotel({}, cfg, LODGING_MOTION.ASSOCIATION_HOUSING);
assert.match(whyHotel, /478/);
assert.match(whyHotel, /Times Square/i);

const gateFail = evaluateActionableNow(
  { title: "NYC Hotel Week", commercialMotion: LODGING_MOTION.SOCIAL_HOUSING, hotelFitScore: 50, sources: [{ url: "https://example.com" }] },
  { roomBlockMentioned: true, housingOpen: true },
  "ORGANIZATION_PATH"
);
assert.equal(gateFail.actionable, false);
assert.ok(gateFail.fails.includes("PROMO_NOT_GROUP_OPPORTUNITY"));

const gateOk = evaluateActionableNow(
  {
    title: "Example Association 2027",
    eventStartDate: "2027-06-01",
    commercialMotion: LODGING_MOTION.ASSOCIATION_HOUSING,
    hotelFitScore: 55,
    sources: [{ url: "https://example.com/housing" }],
    primaryContact: { name: "Jane Smith" },
  },
  { roomBlockMentioned: true, housingOpen: true, housingPageFound: true },
  "NAMED_PARTIAL"
);
assert.equal(gateOk.actionable, true);

const profile = loadPropertyProfile("adp_hilton_times_square");
const universe = buildScenarioUniverse(profile);
assert.ok(universe.length >= 63, `expected >=63 scenarios, got ${universe.length}`);
assert.ok(
  universe.some((s) => s.source === "generic_property_capability"),
  "capability layer must merge onto market pack"
);

console.log("PASS test-gdi-commercial-depth-v2");
