#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HIF_TRACK_A_CONTINUOUS_FOUNDATION_WAVES,
  HIF_TRACK_A_CB2,
  HIF_TRACK_A_CB2B,
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
assert.equal(HIF_TRACK_A_CB2, "hif-track-a-cb2-commercial-discovery-v1");
assert.equal(HIF_TRACK_A_CB2B, "hif-track-a-cb2b-commercial-discovery-v1");
assert.ok(TRACK_A_WAVE_IDS.includes("disc_cfa_hilton_trinidad_pos_2026"));
assert.ok(TRACK_A_WAVE_IDS.includes("disc_cb2_legacy_hotel_green_bay_2026"));
assert.ok(TRACK_A_WAVE_IDS.includes("disc_cb2b_shorebreak_huntington_2026"));
assert.ok(TRACK_A_WAVE_IDS.includes("disc_cb2b_courtyard_denver_downtown_2026"));
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

const cb2Path = path.join(
  ROOT,
  "reports/sec-edgar-hif-v6-track-a-cb2-commercial-discovery/RESEARCH_CANDIDATES.json",
);
const cb2ScorePath = path.join(
  ROOT,
  "reports/sec-edgar-hif-v6-track-a-cb2-commercial-discovery/FIRST_100_SCORECARD.json",
);
const cb2 = JSON.parse(fs.readFileSync(cb2Path, "utf8"));
const cb2Score = JSON.parse(fs.readFileSync(cb2ScorePath, "utf8"));
assert.equal(cb2.publicationState, "CUSTOMER_BLOCKED");
assert.equal(cb2.researchCostUsd, 0);
assert.equal(cb2.priorMergedPr, 541);
assert.equal(cb2.statusChecks.lakevilleInn.status, "STILL_OPEN_BCT");
assert.equal(cb2.statusChecks.hiltonTrinidad.status, "STILL_OPEN_MCT");
const legacy = cb2.candidates.find((c) => c.id === "legacy-hotel-green-bay-bct");
assert.equal(legacy.countsTowardFirst100, false);
assert.equal(legacy.openCompetitiveBrandSelection, false);
assert.equal(legacy.brandFamilyCommitmentClass, "PREFERRED_BRAND_FAMILY_WITHOUT_BINDING_COMMITMENT");
assert.equal(legacy.collectionSelectionClass, "INTRA_BRAND_FAMILY_COLLECTION_SELECTION");
assert.equal(legacy.commercialAccessibility, "ACCESS_LIMITED_PREFERRED_BRAND_FAMILY");
assert.equal(legacy.publicRfp, false);
assert.equal(legacy.agreementType, "UNRESOLVED_SOFT_BRAND_LICENSE");
const shorebreak = cb2.candidates.find((c) => c.id === "shorebreak-huntington-bct");
const courtyard = cb2.candidates.find((c) => c.id === "courtyard-denver-downtown-bct");
const murta = cb2.candidates.find((c) => c.id === "hotel-murta-xativa-concession");
assert.equal(shorebreak.countsTowardFirst100, true);
assert.equal(shorebreak.opportunityType, "BCT");
assert.equal(shorebreak.publicRfp, false);
assert.equal(shorebreak.discoveryMode, "PRIVATE");
assert.equal(courtyard.countsTowardFirst100, true);
assert.equal(courtyard.opportunityType, "BCT");
assert.equal(murta.countsTowardFirst100, false);
assert.equal(murta.opportunityType, "CONCESSION");
assert.equal(cb2.statusChecks.lakevilleInn.status, "STILL_OPEN_BCT");
assert.equal(cb2.statusChecks.hiltonTrinidad.status, "STILL_OPEN_MCT");
assert.equal(cb2.statusChecks.legacyHotelGreenBay.status, "CORRECTED_NOT_OPEN_COMPETITIVE_BCT");
assert.equal(cb2.batchMetrics.first100AfterIcQualification, 4);
assert.equal(cb2Score.metrics.COMMERCIALLY_RELEVANT, 4);
assert.equal(cb2Score.metrics.afterIcQualification, 4);
assert.ok(!cb2Score.qualifiedIds.includes("legacy-hotel-green-bay-bct"));
assert.ok(cb2Score.excludedConversionIntelligence.includes("legacy-hotel-green-bay-bct"));
assert.ok(cb2Score.qualifiedIds.includes("shorebreak-huntington-bct"));
assert.ok(cb2Score.qualifiedIds.includes("courtyard-denver-downtown-bct"));
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
assert.equal(qualifiesPrincipalFirst100(shorebreak), true);
assert.equal(qualifiesPrincipalFirst100(courtyard), true);
assert.equal(qualifiesPrincipalFirst100(legacy), false);
assert.equal(qualifiesPrincipalFirst100(murta), false);

console.log("test-hif-track-a-continuous-foundation-waves-v1: OK");
