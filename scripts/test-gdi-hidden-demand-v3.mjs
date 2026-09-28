#!/usr/bin/env node
/**
 * Offline tests — GDI Hidden Demand V3
 */
import assert from "node:assert/strict";
import {
  CANDIDATE_STATE,
  RESEARCH_TRIAGE,
  LODGING_PROOF,
  ADDRESSABILITY,
} from "../lib/group-demand-intelligence/hidden-demand/v3-states.js";
import { triageExhibitorResearchValue } from "../lib/group-demand-intelligence/hidden-demand/v3-triage.js";
import {
  classifyTravelLikelihood,
  classifyTeamSizeBand,
  classifyLodgingProof,
  candidateStateFromEvidence,
  mayBecomeHotelOpportunity,
} from "../lib/group-demand-intelligence/hidden-demand/v3-lodging-ladder.js";
import {
  extractTeamEvidenceFromText,
  extractExhibitorWhoFromText,
  classifyAddressability,
} from "../lib/group-demand-intelligence/hidden-demand/v3-team-research.js";
import {
  classifyEventGeography,
  eventEligibleForMidtownHotels,
} from "../lib/group-demand-intelligence/hidden-demand/v3-event-geography.js";
import {
  passesCustomerPromotionGateV3,
  shouldDowngradeV2CustomerRow,
  mapContactDepthBucket,
} from "../lib/group-demand-intelligence/hidden-demand/v3-promotion-gate.js";
import {
  defaultExhibitorTeamPath,
  defaultLodgingNextStep,
} from "../lib/group-demand-intelligence/hidden-demand/jev-v3-routing.js";
import { isV3QueueNoise, scoreV3QueuePriority } from "../lib/group-demand-intelligence/hidden-demand/v3-entity-clean.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../lib/group-demand-intelligence/jev/jev-types.js";
import { TRAVEL_CLASS } from "../lib/group-demand-intelligence/hidden-demand/v3-constants.js";

// --- market entity vs hotel opportunity ---
assert.equal(
  candidateStateFromEvidence({
    teamSupported: false,
    lodgingProof: LODGING_PROOF.UNKNOWN,
  }),
  CANDIDATE_STATE.MARKET_ENTITY
);
assert.equal(
  candidateStateFromEvidence({
    teamSupported: true,
    lodgingProof: LODGING_PROOF.WEAK,
  }),
  CANDIDATE_STATE.MARKET_HIDDEN_CANDIDATE
);
assert.equal(
  candidateStateFromEvidence({
    teamSupported: true,
    lodgingProof: LODGING_PROOF.PLAUSIBLE,
  }),
  CANDIDATE_STATE.LODGING_PLAUSIBLE
);
assert.equal(
  candidateStateFromEvidence({
    teamSupported: true,
    lodgingProof: LODGING_PROOF.STRONG_INFERENCE,
    hotelMatched: false,
  }),
  CANDIDATE_STATE.HOTEL_MATCH_CANDIDATE
);
assert.equal(
  candidateStateFromEvidence({
    teamSupported: true,
    lodgingProof: LODGING_PROOF.CONFIRMED,
    hotelMatched: true,
    addressability: ADDRESSABILITY.NAMED_PERSON,
  }),
  CANDIDATE_STATE.ACTIONABLE_NOW
);
assert.ok(!mayBecomeHotelOpportunity(LODGING_PROOF.PLAUSIBLE));
assert.ok(mayBecomeHotelOpportunity(LODGING_PROOF.CONFIRMED));

// --- exhibitor starts as market entity (directory alone) ---
const dirOnly = triageExhibitorResearchValue({
  entityName: "Local Consult LLC",
  sourceType: "EXHIBITOR_DIRECTORY",
  evidenceSnippet: "New York NYC based consultancy",
  futureTiming: true,
  year: 2027,
});
assert.ok(
  dirOnly.triage === RESEARCH_TRIAGE.LOW_RESEARCH_VALUE ||
    dirOnly.triage === RESEARCH_TRIAGE.STOP ||
    dirOnly.triage === RESEARCH_TRIAGE.MEDIUM_RESEARCH_VALUE
);

const intl = triageExhibitorResearchValue({
  entityName: "Acme Robotics Inc.",
  sourceType: "EXHIBITOR_DIRECTORY",
  country: "Germany",
  boothNumber: "1204",
  futureTiming: true,
  year: 2027,
  lodgingSignalStrength: "MEDIUM",
});
assert.equal(intl.triage, RESEARCH_TRIAGE.HIGH_RESEARCH_VALUE);

// --- team evidence ---
const team = extractTeamEvidenceFromText(
  `Acme Robotics Inc. See us at Booth 1204. Jane Smith, Events Manager and John Doe, Field Marketing will staff the booth for the three-day show including setup and breakdown.`,
  "Acme Robotics Inc."
);
assert.equal(team.teamSupported, true);
assert.ok(team.namedPeople.length >= 1);
assert.equal(team.companyEventPage, true);
assert.equal(team.setupBreakdown, true);

// --- travel ---
assert.equal(
  classifyTravelLikelihood({
    travelClass: TRAVEL_CLASS.INTERNATIONAL,
    teamSupported: true,
    namedPeople: 2,
  }),
  "MEDIUM"
);
assert.equal(
  classifyTravelLikelihood({
    travelClass: TRAVEL_CLASS.UNKNOWN,
    teamSupported: false,
  }),
  "UNKNOWN"
);
assert.equal(
  classifyTravelLikelihood({
    explicitTravel: true,
  }),
  "STRONG"
);

// --- lodging ladder ---
const lod1 = classifyLodgingProof({
  deepenText: "Official hotel block and housing registration for exhibitors",
});
assert.equal(lod1.lodgingProof, LODGING_PROOF.CONFIRMED);
const lod2 = classifyLodgingProof({
  travelClass: TRAVEL_CLASS.INTERNATIONAL,
  teamSupported: true,
  namedPeople: 3,
  multiDay: true,
});
assert.equal(lod2.lodgingProof, LODGING_PROOF.STRONG_INFERENCE);
const lod3 = classifyLodgingProof({
  travelClass: TRAVEL_CLASS.LONG_HAUL_AIR,
  teamSupported: true,
  namedPeople: 1,
});
assert.ok(
  lod3.lodgingProof === LODGING_PROOF.PLAUSIBLE ||
    lod3.lodgingProof === LODGING_PROOF.STRONG_INFERENCE
);
const lod4 = classifyLodgingProof({
  deepenText: "listed as exhibitor",
  teamSupported: true,
});
assert.equal(lod4.lodgingProof, LODGING_PROOF.WEAK);

// --- WHO ---
const who = extractExhibitorWhoFromText(
  "Jane Smith, Events Manager jane.smith@acme.com will attend",
  "Acme Robotics Inc."
);
assert.ok(who.some((c) => /Jane/i.test(c.name)));
assert.ok(!/CEO|President/i.test(who[0]?.role || ""));
assert.equal(classifyAddressability(who), ADDRESSABILITY.NAMED_PERSON);

// --- agency ---
const agency = extractTeamEvidenceFromText(
  "Activation produced by Brightmark Experiential Agency for Acme",
  "Acme"
);
assert.ok(agency.agencyRelationship);

// --- locality / geography ---
assert.equal(
  classifyEventGeography({
    demandGeneratorName: "FRANCHISE ISTANBUL EXPO 2027 Exhibitor List",
  }).geography,
  "NON_NYC"
);
assert.equal(
  eventEligibleForMidtownHotels({
    demandGeneratorName: "FRANCHISE ISTANBUL EXPO 2027",
  }),
  false
);
assert.equal(
  classifyEventGeography({
    demandGeneratorName: "New York Javits International Gift Fair 2027 Exhibitors",
  }).geography,
  "NYC_MIDTOWN"
);

// --- hotel match timing: no hotel opp without lodging ---
const gateFail = passesCustomerPromotionGateV3({
  company: "Acme",
  futureTiming: true,
  year: 2027,
  participationEvidence: true,
  teamSupported: true,
  lodgingProof: LODGING_PROOF.WEAK,
  hotelMatched: true,
  sourceUrl: "https://example.com",
  contactResearchAttempted: true,
  addressability: ADDRESSABILITY.NAMED_PERSON,
});
assert.equal(gateFail.ok, false);

const gateOk = passesCustomerPromotionGateV3({
  company: "Acme",
  futureTiming: true,
  year: 2027,
  participationEvidence: true,
  teamSupported: true,
  lodgingProof: LODGING_PROOF.CONFIRMED,
  hotelMatched: true,
  sourceUrl: "https://example.com",
  contactResearchAttempted: true,
  addressability: ADDRESSABILITY.NAMED_PERSON,
  timingTrigger: "exhibitor_list_published",
  recommendedAction: "Contact Jane",
  who: [{ name: "Jane Smith", role: "Events Manager" }],
});
assert.equal(gateOk.ok, true);
assert.equal(gateOk.customerState, "ACTIONABLE_NOW");

assert.equal(
  passesCustomerPromotionGateV3({
    company: "Interested in Exhibiting",
    futureTiming: true,
    year: 2027,
    participationEvidence: true,
    teamSupported: true,
    lodgingProof: LODGING_PROOF.CONFIRMED,
    hotelMatched: true,
    sourceUrl: "https://example.com",
    contactResearchAttempted: true,
    addressability: ADDRESSABILITY.NAMED_PERSON,
  }).ok,
  false
);

// --- downgrade V2 ---
const dg = shouldDowngradeV2CustomerRow(
  { opportunityType: "HIDDEN_DEMAND", gdiVersion: "gdi_hidden_demand_v2", organizationName: "Acme" },
  new Map([["acme", { candidateState: CANDIDATE_STATE.MARKET_ENTITY }]])
);
assert.equal(dg.downgrade, true);

// --- Jev routes ---
assert.equal(
  defaultExhibitorTeamPath({ hasEventPage: false }),
  "COMPANY_EVENT_PAGE"
);
assert.equal(
  defaultLodgingNextStep({ lodgingProof: "CONFIRMED" }),
  "STOP_SUFFICIENT"
);
assert.ok(CHOICES[JEV_DECISION_TYPE.EXHIBITOR_TEAM_RESEARCH_PATH].includes("COMPANY_EVENT_PAGE"));
assert.ok(CHOICES[JEV_DECISION_TYPE.LODGING_EVIDENCE_NEXT_STEP].includes("HOUSING_SOURCE"));
assert.ok(isValidChoice(JEV_DECISION_TYPE.EXHIBITOR_TEAM_RESEARCH_PATH, "NEWSROOM"));
assert.ok(isValidChoice(JEV_DECISION_TYPE.LODGING_EVIDENCE_NEXT_STEP, "STOP_PLAUSIBLE_ONLY"));

// --- noise / queue ---
  assert.equal(isV3QueueNoise({ entityName: "Abbotsford – Fall" }), true);
  assert.equal(isV3QueueNoise({ entityName: "Interested in Exhibiting" }), true);
  assert.equal(isV3QueueNoise({ entityName: "Scan Exhibitors with Colleqt" }), true);
  assert.equal(isV3QueueNoise({ entityName: "Powered by A2Z Events" }), true);
  assert.equal(isV3QueueNoise({ entityName: "Acme Robotics Inc." }), false);
  assert.ok(scoreV3QueuePriority({ entityName: "Acme Inc.", sourceType: "EXHIBITOR_DIRECTORY" }) > 40);

// --- team size band not rooms ---
assert.equal(
  classifyTeamSizeBand({ namedPeople: 2 }),
  "SMALL_TEAM"
);
assert.equal(mapContactDepthBucket(ADDRESSABILITY.COMPANY_PATH), "ORG_PATH");

console.log("test-gdi-hidden-demand-v3: PASS");
