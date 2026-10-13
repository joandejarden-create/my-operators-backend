#!/usr/bin/env node
/**
 * Offline tests — GDI Hidden Demand Expansion V1
 * generator-vs-opportunity, identity, multi-hotel match, isolation, drawer fields, Jev type.
 */
import assert from "node:assert/strict";
import {
  DISCOVERY_DEPTH,
  DEMAND_FAMILY,
  HIDDEN_MOTION,
  LODGING_SIGNAL_STRENGTH,
  RECLASS_LABEL,
} from "../lib/group-demand-intelligence/hidden-demand/constants.js";
import {
  computeDemandGeneratorId,
  computeHiddenDemandId,
  computeHotelMatchId,
  computeHotelOpportunityId,
} from "../lib/group-demand-intelligence/hidden-demand/identity.js";
import {
  classifyDiscoveryDepth,
  isCustomerPromotableHiddenDemand,
  reclassifyExistingOpportunity,
  inferDemandFamily,
} from "../lib/group-demand-intelligence/hidden-demand/classify.js";
import {
  evaluateHotelHiddenDemandFit,
  inferHiddenMotion,
} from "../lib/group-demand-intelligence/hidden-demand/hotel-match.js";
import {
  signalToEntities,
  dedupeHiddenDemands,
  matchHiddenDemandToHotels,
  hotelMatchToOpportunityCandidate,
} from "../lib/group-demand-intelligence/hidden-demand/index.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../lib/group-demand-intelligence/jev/jev-types.js";
import { buildHiddenDemandMarketQueries } from "../lib/group-demand-intelligence/hidden-demand/discovery-queries.js";

const hilton = {
  hotelId: "rec35fExUxCClpOP6",
  displayName: "Hilton New York Times Square",
  capabilityProfile: { totalGuestrooms: 478, totalMeetingSpaceSqFt: 300 },
};
const renaissance = {
  hotelId: "recG66DQJKP2c0UNh",
  displayName: "Renaissance New York Times Square Hotel",
  capabilityProfile: { totalGuestrooms: 310, totalMeetingSpaceSqFt: 5000 },
};

// --- generator vs opportunity ---
assert.equal(
  classifyDiscoveryDepth({ title: "NRF 2027 Retail Big Show at Javits" }),
  DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND
);
assert.equal(
  classifyDiscoveryDepth({
    title: "Acme Tech NRF Project Team 2027",
    organizationName: "Acme Tech",
    hasParentGenerator: true,
    hasSubgroupEvidence: true,
  }),
  DISCOVERY_DEPTH.TWO_LAYERS_DEEP
);
assert.equal(
  classifyDiscoveryDepth({
    title: "25-person NYC implementation team hotel block",
    organizationName: "Globex Consulting",
  }),
  DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL
);
assert.equal(isCustomerPromotableHiddenDemand(DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND), false);
assert.equal(isCustomerPromotableHiddenDemand(DISCOVERY_DEPTH.TWO_LAYERS_DEEP), true);

const reclass = reclassifyExistingOpportunity({
  title: "New York Comic Con",
  organizationName: "ReedPop",
  commercialMotion: "FULL_MEETING_RFP",
});
assert.equal(reclass.label, RECLASS_LABEL.OBVIOUS_GENERATOR_ONLY);
assert.equal(reclass.action, "DOWNGRADE_TO_GENERATOR");

// --- identity stability ---
const genId = computeDemandGeneratorId({ name: "NRF 2027", marketKey: "nyc_midtown", year: 2027 });
const hdId = computeHiddenDemandId({
  organizationName: "Acme Tech",
  projectName: "Acme Tech NRF Project Team 2027",
  demandGeneratorId: genId,
  family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
  timingKey: "2027",
});
assert.match(hdId, /^hd_/);
const hMatch = computeHotelMatchId({ hotelId: hilton.hotelId, hiddenDemandId: hdId });
const rMatch = computeHotelMatchId({ hotelId: renaissance.hotelId, hiddenDemandId: hdId });
assert.notEqual(hMatch, rMatch);
const hOpp = computeHotelOpportunityId({ hotelId: hilton.hotelId, hiddenDemandId: hdId });
const rOpp = computeHotelOpportunityId({ hotelId: renaissance.hotelId, hiddenDemandId: hdId });
assert.notEqual(hOpp, rOpp);
assert.equal(
  computeHiddenDemandId({
    organizationName: "Acme Tech",
    projectName: "Acme Tech NRF Project Team 2027",
    demandGeneratorId: genId,
    family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
    timingKey: "2027",
  }),
  hdId
);

// --- signal → entities ---
const { generators, hidden } = signalToEntities({
  hit: {
    title: "Acme Tech exhibitor at NRF 2027 Javits",
    link: "https://example.com/nrf/exhibitors/acme",
    snippet: "Acme Tech exhibitor booth staff hotel block Midtown",
  },
  family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
  marketKey: "nyc_midtown",
  year: 2027,
  lodgingSignals: { roomBlockMentioned: true, housingPageFound: true },
});
assert.ok(generators.length >= 1 || hidden.length >= 1);
assert.ok(hidden.length >= 1);
assert.equal(hidden[0].hiddenDemandId, hidden[0].hiddenDemandId);
const deduped = dedupeHiddenDemands([...hidden, ...hidden]);
assert.equal(deduped.length, hidden.length);

// --- multi-hotel match ---
const hd = {
  hiddenDemandId: hdId,
  demandGeneratorId: genId,
  organizationName: "Acme Tech",
  title: "Acme Tech NRF Project Team 2027",
  family: DEMAND_FAMILY.EXHIBITOR_VENDOR,
  lodgingSignalStrength: LODGING_SIGNAL_STRENGTH.STRONG,
  discoveryDepth: DISCOVERY_DEPTH.TWO_LAYERS_DEEP,
  destination: "New York Midtown Times Square",
  year: 2027,
  sourceUrl: "https://example.com/nrf/exhibitors/acme",
  evidence: {
    entityExists: true,
    futureTiming: true,
    travelPresence: true,
    plausibleLodging: true,
    addressableOrg: true,
  },
};
const matches = matchHiddenDemandToHotels(hd, [hilton, renaissance]);
assert.equal(matches.length, 2);
assert.equal(matches[0].hiddenDemandId, matches[1].hiddenDemandId);
assert.notEqual(matches[0].hotelOpportunityId, matches[1].hotelOpportunityId);
assert.notEqual(matches[0].fitScore, undefined);
assert.ok(matches[0].commercialMotion);
assert.ok(matches[0].thesis.includes("Hilton") || matches[0].thesis.includes("rooms"));
assert.ok(matches[1].thesis.includes("Renaissance") || matches[1].thesis.includes("rooms"));
// Hilton lodging-primary limited meetings should score at least as strong on exhibitor block
assert.ok(matches.find((m) => m.hotelId === hilton.hotelId).fitScore >= 55);

const hCand = hotelMatchToOpportunityCandidate(
  hd,
  matches.find((m) => m.hotelId === hilton.hotelId),
  hilton
);
const rCand = hotelMatchToOpportunityCandidate(
  hd,
  matches.find((m) => m.hotelId === renaissance.hotelId),
  renaissance
);
assert.equal(hCand.hiddenDemandId, rCand.hiddenDemandId);
assert.notEqual(hCand.id, rCand.id);
assert.equal(hCand.hotelId, hilton.hotelId);
assert.equal(rCand.hotelId, renaissance.hotelId);
// Isolation: no competitor fields
assert.equal(hCand.competitorFits, undefined);
assert.ok(hCand.hotelOpportunityThesis);
assert.ok(hCand.whyNow);
assert.ok(hCand.recommendedAction);
assert.ok(hCand.summaryWhyHotel);

// Hotel-specific motion can differ by meeting inventory
const hMotion = inferHiddenMotion(
  DEMAND_FAMILY.ASSOCIATION_SUBGROUP,
  LODGING_SIGNAL_STRENGTH.MEDIUM,
  hilton
);
const rMotion = inferHiddenMotion(
  DEMAND_FAMILY.ASSOCIATION_SUBGROUP,
  LODGING_SIGNAL_STRENGTH.MEDIUM,
  renaissance
);
assert.equal(hMotion, HIDDEN_MOTION.ASSOCIATION_SUBGROUP);
assert.equal(rMotion, HIDDEN_MOTION.LEADERSHIP_MEETING);

// Insufficient stretch destination
const stretch = evaluateHotelHiddenDemandFit(
  { ...hd, destination: "Brooklyn Navy Yard Queens stretch far" },
  hilton
);
assert.ok(stretch.fitScore < matches.find((m) => m.hotelId === hilton.hotelId).fitScore);

// --- family inference ---
assert.equal(inferDemandFamily("trade mission chamber delegation"), DEMAND_FAMILY.DELEGATION);
assert.equal(inferDemandFamily("escorted tour operator series"), DEMAND_FAMILY.TOUR_SERIES);

// --- queries generic ---
const qs = buildHiddenDemandMarketQueries({ marketLabel: "Bethesda", year: 2027, maxQueries: 10 });
assert.equal(qs.length, 10);
assert.ok(qs.every((q) => !/hilton|renaissance/i.test(q.query)));

// --- Jev type registered ---
assert.ok(JEV_DECISION_TYPE.HIDDEN_DEMAND_NEXT_LAYER);
assert.ok(CHOICES.HIDDEN_DEMAND_NEXT_LAYER.includes("EXHIBITOR"));
assert.ok(isValidChoice(JEV_DECISION_TYPE.HIDDEN_DEMAND_NEXT_LAYER, "CREW"));
assert.equal(isValidChoice(JEV_DECISION_TYPE.HIDDEN_DEMAND_NEXT_LAYER, "INVENT_PERSON"), false);

// --- quality gate ---
import { isPlausibleOrganizationName, passesHiddenDemandQualityGate } from "../lib/group-demand-intelligence/hidden-demand/quality-gate.js";
assert.equal(isPlausibleOrganizationName("legit"), false);
assert.equal(isPlausibleOrganizationName("http://www.w3.org/2000/svg"), false);
assert.equal(isPlausibleOrganizationName("Acme Tech"), true);
assert.equal(
  passesHiddenDemandQualityGate({
    organizationName: "require",
    evidence: { entityExists: true, addressableOrg: true },
  }).ok,
  false
);
assert.equal(
  passesHiddenDemandQualityGate({
    organizationName: "Acme Tech",
    title: "Acme Tech NRF Project Team",
    evidence: { entityExists: true, addressableOrg: true },
    sourceUrl: "https://example.com/nrf/exhibitors/acme",
  }).ok,
  true
);

// lodging motion under mega-event → keep as watch, not wipe
const overflowReclass = reclassifyExistingOpportunity({
  title: "76th Annual Meeting",
  organizationName: "Society for the Study of Social Problems",
  opportunityType: "OVERFLOW_HOUSING",
  roomDemandStatus: "OVERFLOW_ONLY",
  commercialMotion: "OVERFLOW",
});
assert.equal(overflowReclass.label, RECLASS_LABEL.DEEPER_MOTION_EXISTS);
assert.equal(overflowReclass.action, "KEEP_AS_WATCH");

console.log("test-gdi-hidden-demand-expansion-v1: PASS");
