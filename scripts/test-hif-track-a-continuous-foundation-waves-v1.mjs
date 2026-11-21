#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HIF_TRACK_A_CONTINUOUS_FOUNDATION_WAVES,
  TRACK_A_WAVE_IDS,
  isTrackAContinuousWaveId,
  classifyAgreementVsOpportunity,
  qualifiesPrincipalFirst100,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-a-continuous-foundation-waves-v1.js";
import {
  mapCommercialCategoryToFindingType,
  commercialOpportunityFieldName,
  EXISTING_FINDING_TYPES,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const batchPath = path.join(
  ROOT,
  "reports/sec-edgar-hif-v6-track-a-continuous-foundation-waves/RESEARCH_CANDIDATES.json",
);
const scorePath = path.join(
  ROOT,
  "reports/sec-edgar-hif-v6-track-a-continuous-foundation-waves/FIRST_100_SCORECARD.json",
);

assert.equal(HIF_TRACK_A_CONTINUOUS_FOUNDATION_WAVES, "hif-track-a-continuous-foundation-waves-v1");
assert.ok(TRACK_A_WAVE_IDS.includes("disc_cfa_hilton_trinidad_pos_2026"));
assert.equal(isTrackAContinuousWaveId("disc_cfa_hilton_trinidad_pos_2026"), true);
assert.equal(isTrackAContinuousWaveId("disc_track_b_other"), false);

const unresolved = classifyAgreementVsOpportunity({
  agreementType: "UNRESOLVED_INSTRUMENT",
  opportunityType: "UNCLASSIFIED",
});
assert.equal(unresolved.principalCommercialOpportunity, false);
assert.match(unresolved.note, /Do not force/);

const trinidad = classifyAgreementVsOpportunity({
  agreementType: "SHORT_TERM_HOTEL_MANAGEMENT_AGREEMENT",
  opportunityType: "MCT",
});
assert.equal(trinidad.principalCommercialOpportunity, true);

assert.equal(
  qualifiesPrincipalFirst100({
    countsTowardFirst100: true,
    priority: "PRIORITY_A",
    hotelSpecificEvidence: true,
    saleOrExpirationOnly: true,
    prospectiveCommercialDecisionEvidence: false,
    commercialRelevance: "VERIFIED_RELEVANT",
  }),
  false,
);

assert.equal(
  qualifiesPrincipalFirst100({
    countsTowardFirst100: true,
    priority: "PRIORITY_A",
    hotelSpecificEvidence: true,
    saleOrExpirationOnly: false,
    prospectiveCommercialDecisionEvidence: true,
    commercialRelevance: "VERIFIED_RELEVANT",
  }),
  true,
);

assert.equal(mapCommercialCategoryToFindingType("MANAGEMENT_CONTRACT_TARGET"), EXISTING_FINDING_TYPES.MANAGEMENT);
assert.equal(commercialOpportunityFieldName("MCT"), "COMMERCIAL_OPPORTUNITY_MCT");
assert.equal(commercialOpportunityFieldName("CONCESSION"), "COMMERCIAL_SIGNAL_CONCESSION_ADJACENT");
assert.equal(commercialOpportunityFieldName("HOTEL_DEVELOPMENT"), "COMMERCIAL_SIGNAL_HOTEL_DEVELOPMENT_ADJACENT");

const batch = JSON.parse(fs.readFileSync(batchPath, "utf8"));
assert.equal(batch.publicationState, "CUSTOMER_BLOCKED");
assert.ok(batch.batchMetrics.distinctHotelsInvestigated >= 20);
assert.equal(batch.researchCostUsd, 0);

const lakeville = batch.candidates.find((c) => c.id === "lakeville-inn-mn-bct");
const trinidadCand = batch.candidates.find((c) => c.id === "hilton-trinidad-mct");
const magdalena = batch.candidates.find((c) => c.id === "magdalena-grand-tobago-sale");
const montRoyal = batch.candidates.find((c) => c.id === "hotel-mont-royal-montreal-watch");
const pattee = batch.candidates.find((c) => c.id === "hotel-pattee-perry-completed");

assert.equal(lakeville.countsTowardFirst100, true);
assert.equal(trinidadCand.countsTowardFirst100, true);
assert.equal(trinidadCand.opportunityType, "MCT");
assert.equal(magdalena.countsTowardFirst100, false);
assert.equal(magdalena.saleOrExpirationOnly, true);
assert.equal(montRoyal.countsTowardFirst100, false);
assert.equal(pattee.disposition, "COMPLETED_AWARDED");

const score = JSON.parse(fs.readFileSync(scorePath, "utf8"));
assert.equal(score.metrics.COMMERCIALLY_RELEVANT, 2);
assert.deepEqual(score.qualifiedIds.sort(), ["hilton-trinidad-mct", "lakeville-inn-mn-bct"].sort());
assert.ok(!score.qualifiedIds.includes("magdalena-grand-tobago-sale"));

console.log("test-hif-track-a-continuous-foundation-waves-v1: OK");
