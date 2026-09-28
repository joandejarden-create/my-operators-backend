#!/usr/bin/env node
/**
 * Offline tests — GDI Hidden Demand Source Expansion V2
 */
import assert from "node:assert/strict";
import {
  classifyStructuredSource,
  structuredSourceScore,
  buildStructuredRoutingQueries,
} from "../lib/group-demand-intelligence/hidden-demand/source-classifier.js";
import { extractEntitiesFromHtmlDirectory } from "../lib/group-demand-intelligence/hidden-demand/extract-directory.js";
import {
  extractEntitiesFromProgramText,
  extractHousingSignalsFromText,
} from "../lib/group-demand-intelligence/hidden-demand/extract-pdf.js";
import { normalizeOrganizationName, dedupeExtractedEntities } from "../lib/group-demand-intelligence/hidden-demand/entity-normalize.js";
import { qualifyEntityLodging, lodgingOriginPrior } from "../lib/group-demand-intelligence/hidden-demand/lodging-qualify.js";
import { passesStructuredEntityQualityGate } from "../lib/group-demand-intelligence/hidden-demand/structured-quality-gate.js";
import { defaultStructuredSourcePriority } from "../lib/group-demand-intelligence/hidden-demand/jev-structured-source.js";
import { entityToHiddenDemand } from "../lib/group-demand-intelligence/hidden-demand/source-expansion-v2.js";
import { matchHiddenDemandToHotels } from "../lib/group-demand-intelligence/hidden-demand/index.js";
import { JEV_DECISION_TYPE, CHOICES, isValidChoice } from "../lib/group-demand-intelligence/jev/jev-types.js";
import { SOURCE_TYPE, LODGING_SIGNAL_STRENGTH } from "../lib/group-demand-intelligence/hidden-demand/v2-constants.js";

// source detection
assert.equal(
  classifyStructuredSource({
    url: "https://example.com/exhibitors/list",
    title: "Exhibitor Directory 2027",
  }),
  SOURCE_TYPE.EXHIBITOR_DIRECTORY
);
assert.equal(
  classifyStructuredSource({
    url: "https://example.com/housing-guide.pdf",
    title: "Official Housing Guide",
  }),
  SOURCE_TYPE.HOUSING_PDF
);
assert.equal(
  classifyStructuredSource({ url: "https://example.com/blog", title: "Events in New York" }),
  SOURCE_TYPE.GENERIC_SERP
);
assert.ok(structuredSourceScore(SOURCE_TYPE.EXHIBITOR_DIRECTORY) > structuredSourceScore(SOURCE_TYPE.GENERIC_SERP));

const qs = buildStructuredRoutingQueries({ max: 5 });
assert.equal(qs.length, 5);
assert.ok(qs.every((q) => !/hilton|renaissance/i.test(q.query)));

// directory extraction
const html = `
<table><tr><th>Company</th><th>Booth</th></tr>
<tr><td>Acme Robotics Inc.</td><td>1204</td></tr>
<tr><td>Globex Systems LLC</td><td>2210</td></tr>
<tr><td>View</td><td>1</td></tr>
</table>
<script type="application/ld+json">{"@type":"Organization","name":"Initech Softwares"}</script>
`;
const dirs = extractEntitiesFromHtmlDirectory(html, {
  sourceType: SOURCE_TYPE.EXHIBITOR_DIRECTORY,
  sourceURL: "https://example.com/exhibitors",
  year: 2027,
  futureTiming: true,
});
assert.ok(dirs.some((e) => /Acme/i.test(e.entityName)));
assert.ok(dirs.some((e) => /Globex/i.test(e.entityName)));

// PDF program text
const pdfText = `
Exhibitors
Acme Robotics
Globex Systems
Sponsors
Northwind Trading
Committees
Membership Committee
Housing opens March 1, 2027. Official hotel block available. Overflow hotels listed.
`;
const { entities: pdfEnts } = extractEntitiesFromProgramText(pdfText, {
  sourceType: SOURCE_TYPE.PROGRAM_PDF,
  sourceURL: "https://example.com/program.pdf",
  year: 2027,
});
assert.ok(pdfEnts.length >= 2);
const housing = extractHousingSignalsFromText(pdfText, "https://example.com/housing.pdf");
assert.equal(housing.roomBlockMentioned, true);
assert.equal(housing.overflowMentioned, true);

// normalize + dedupe
const n1 = normalizeOrganizationName("Acme Robotics Inc.");
const n2 = normalizeOrganizationName("Acme Robotics LLC");
assert.equal(n1.normalizeKey, n2.normalizeKey);
const deduped = dedupeExtractedEntities([
  { entityName: "Acme Robotics Inc.", participationRole: "EXHIBITOR", confidence: 0.5 },
  { entityName: "Acme Robotics LLC", participationRole: "EXHIBITOR", confidence: 0.8 },
]);
assert.equal(deduped.length, 1);

// lodging
assert.equal(lodgingOriginPrior({ country: "Germany" }), "HIGH");
assert.equal(lodgingOriginPrior({ state: "NY", city: "New York" }), "LOW");
const lod = qualifyEntityLodging(
  {
    entityName: "Acme Robotics",
    country: "Germany",
    evidenceSnippet: "booth staff multi-day setup hotel block",
  },
  housing
);
assert.ok(
  lod.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.STRONG ||
    lod.lodgingSignalStrength === LODGING_SIGNAL_STRENGTH.MEDIUM
);

// quality gate
assert.equal(passesStructuredEntityQualityGate({ entityName: "Gold Sponsor", sourceType: SOURCE_TYPE.SPONSOR_DIRECTORY }).ok, false);
assert.equal(
  passesStructuredEntityQualityGate({
    entityName: "Acme Robotics Inc.",
    sourceType: SOURCE_TYPE.EXHIBITOR_DIRECTORY,
    participationRole: "EXHIBITOR",
    futureTiming: true,
    confidence: 0.7,
  }).ok,
  true
);

// Jev type
assert.ok(JEV_DECISION_TYPE.STRUCTURED_SOURCE_PRIORITY);
assert.ok(isValidChoice(JEV_DECISION_TYPE.STRUCTURED_SOURCE_PRIORITY, "EXHIBITOR_DIRECTORY"));
assert.equal(defaultStructuredSourcePriority({ demandFamily: "EXHIBITOR_VENDOR", sourcesChecked: [] }), "HOUSING_PDF");

// multi-hotel from entity
const hd = entityToHiddenDemand(
  {
    entityName: "Acme Robotics",
    participationRole: "EXHIBITOR",
    sourceType: SOURCE_TYPE.EXHIBITOR_DIRECTORY,
    sourceURL: "https://example.com/exhibitors/acme",
    futureTiming: true,
    year: 2027,
    evidenceSnippet: "international exhibitor booth staff",
    demandGeneratorName: "NRF 2027",
    family: "EXHIBITOR_VENDOR",
    confidence: 0.7,
  },
  lod,
  "nyc_midtown"
);
assert.match(hd.hiddenDemandId, /^hd_/);
const hilton = {
  hotelId: "rec35fExUxCClpOP6",
  displayName: "Hilton New York Times Square",
  capabilityProfile: { totalGuestrooms: 478, totalMeetingSpaceSqFt: 300 },
};
const ren = {
  hotelId: "recG66DQJKP2c0UNh",
  displayName: "Renaissance New York Times Square Hotel",
  capabilityProfile: { totalGuestrooms: 310, totalMeetingSpaceSqFt: 5000 },
};
const matches = matchHiddenDemandToHotels(hd, [hilton, ren]);
assert.equal(matches.length, 2);
assert.equal(matches[0].hiddenDemandId, matches[1].hiddenDemandId);
assert.notEqual(matches[0].hotelOpportunityId, matches[1].hotelOpportunityId);

console.log("test-gdi-hidden-demand-source-expansion-v2: PASS");
