#!/usr/bin/env node
/**
 * Offline tests — GDI Hidden Demand Source Acquisition V4
 */
import assert from "node:assert/strict";
import {
  classifySourceCandidate,
  confirmSourceAfterFetch,
  validateSourceGeography,
  validateSourceTiming,
  scoreParticipantRichness,
  shouldDeepParse,
} from "../lib/group-demand-intelligence/hidden-demand/v4-source-validate.js";
import {
  SOURCE_STATUS,
  PARTICIPANT_RICHNESS,
} from "../lib/group-demand-intelligence/hidden-demand/v4-constants.js";
import { buildNycSourceAcquisitionQueries } from "../lib/group-demand-intelligence/hidden-demand/v4-routing-queries.js";
import {
  defaultSourceValidationNextStep,
} from "../lib/group-demand-intelligence/hidden-demand/jev-v4-source-validation.js";
import { isV3QueueNoise } from "../lib/group-demand-intelligence/hidden-demand/v3-entity-clean.js";
import { mayBecomeHotelOpportunity } from "../lib/group-demand-intelligence/hidden-demand/v3-lodging-ladder.js";
import { LODGING_PROOF } from "../lib/group-demand-intelligence/hidden-demand/v3-states.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../lib/group-demand-intelligence/jev/jev-types.js";
import { passesCustomerPromotionGateV3 } from "../lib/group-demand-intelligence/hidden-demand/v3-promotion-gate.js";
import { ADDRESSABILITY } from "../lib/group-demand-intelligence/hidden-demand/v3-states.js";

// geo-first
assert.equal(
  validateSourceGeography({
    title: "FRANCHISE ISTANBUL EXPO 2027 Exhibitor List",
    snippet: "Istanbul Turkey exhibitors",
    url: "https://example.com/istanbul",
  }).status,
  SOURCE_STATUS.WRONG_GEOGRAPHY
);
assert.equal(
  validateSourceGeography({
    title: "Something",
    pageText: "Welcome to Jacob K. Javits Convention Center in New York City. Exhibitor list 2027.",
  }).ok,
  true
);
assert.equal(
  validateSourceGeography({
    title: "Javits Exhibitor Directory 2027",
    snippet: "New York",
    url: "https://example.com/exhibitors",
  }).geography,
  "NYC_PROVISIONAL"
);

// future timing
assert.equal(
  validateSourceTiming({ title: "Exhibitor List 2022", snippet: "archive 2021 2022" }).status,
  SOURCE_STATUS.STALE
);
assert.equal(validateSourceTiming({ title: "Exhibitors 2027", snippet: "Javits" }).ok, true);

// richness
assert.equal(
  scoreParticipantRichness({
    title: "Exhibitor Directory — Who's Exhibiting",
    snippet: "booth list",
  }).richness,
  PARTICIPANT_RICHNESS.HIGH
);
assert.equal(
  scoreParticipantRichness({
    title: "Become an Exhibitor — Register Now",
    snippet: "application",
  }).richness,
  PARTICIPANT_RICHNESS.NONE
);

// classify candidate — wrong geo rejected before fetch
const istanbul = classifySourceCandidate({
  url: "https://example.com/istanbul-expo-exhibitors",
  title: "Istanbul Expo 2027 Exhibitor Directory",
  snippet: "Companies exhibiting in Istanbul",
});
assert.equal(istanbul.status, SOURCE_STATUS.WRONG_GEOGRAPHY);
assert.equal(istanbul.mayFetch, false);

const nycDir = classifySourceCandidate({
  url: "https://maps.nrf.com/nrf-2027/public/exhibitors",
  title: "NRF 2027 Exhibitor Directory — New York Javits",
  snippet: "who's exhibiting booth list New York",
});
assert.equal(nycDir.status, SOURCE_STATUS.VALID_NYC_STRUCTURED);
assert.equal(nycDir.mayFetch, true);

// post-fetch confirm rejects page without NYC
const fake = confirmSourceAfterFetch(
  { ...nycDir, title: "Directory" },
  "Welcome to our Las Vegas convention exhibitor portal 2027 booth list"
);
assert.equal(fake.status, SOURCE_STATUS.WRONG_GEOGRAPHY);

const okPage = confirmSourceAfterFetch(
  nycDir,
  "NRF Retail's Big Show at Jacob Javits Center New York. Exhibitor directory booth list 2027."
);
assert.equal(okPage.status, SOURCE_STATUS.VALID_NYC_STRUCTURED);
assert.equal(shouldDeepParse(okPage), true);

// UI chrome entity rejection
assert.equal(isV3QueueNoise({ entityName: "Exhibitor Application" }), true);
assert.equal(isV3QueueNoise({ entityName: "Register Now" }), true);

// Jev choices
assert.ok(CHOICES[JEV_DECISION_TYPE.SOURCE_VALIDATION_NEXT_STEP].includes("REJECT_WRONG_GEO"));
assert.ok(isValidChoice(JEV_DECISION_TYPE.SOURCE_VALIDATION_NEXT_STEP, "FETCH_STATIC"));
assert.equal(
  defaultSourceValidationNextStep(istanbul),
  "REJECT_WRONG_GEO"
);
assert.equal(
  defaultSourceValidationNextStep(nycDir),
  "FETCH_STATIC"
);

// routing queries — no hotel brands
const qs = buildNycSourceAcquisitionQueries({ max: 40 });
assert.equal(qs.length, 40);
assert.ok(qs.every((q) => !/hilton|renaissance/i.test(q.query)));
assert.ok(qs.some((q) => /javits/i.test(q.query)));
assert.ok(qs.some((q) => /housing|hotel block/i.test(q.query)));
assert.ok(qs.some((q) => /tour/i.test(q.query)));
assert.ok(qs.some((q) => /delegation|trade mission/i.test(q.query)));

// lodging gate / promotion
assert.ok(mayBecomeHotelOpportunity(LODGING_PROOF.CONFIRMED));
assert.ok(!mayBecomeHotelOpportunity(LODGING_PROOF.PLAUSIBLE));
assert.equal(
  passesCustomerPromotionGateV3({
    company: "Acme Robotics Inc.",
    futureTiming: true,
    year: 2027,
    participationEvidence: true,
    teamSupported: true,
    lodgingProof: LODGING_PROOF.WEAK,
    hotelMatched: true,
    sourceUrl: "https://example.com",
    contactResearchAttempted: true,
    addressability: ADDRESSABILITY.NAMED_PERSON,
  }).ok,
  false
);

console.log("test-gdi-hidden-demand-source-acquisition-v4: PASS");
