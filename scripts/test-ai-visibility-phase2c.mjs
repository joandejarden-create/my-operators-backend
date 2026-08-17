#!/usr/bin/env node
/**
 * Phase 2C tests — classifier v3 golden set + geography isolation.
 * No paid provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildAiVisibilityEntityIndex,
  extractMentions,
  buildObservationFromExtractions,
  computeAiPresenceRate,
  normalizePromptGeography,
  filterObservationsByGeography,
  calculateVisibilityMetrics,
  resolveCountryGeography,
  auditCanonicalGeographySources,
  buildPeerSetDescriptor,
  detectResponseSections,
  RESOLVER_VERSION,
  RECOMMENDATION_CLASSIFIER_VERSION,
  GEOGRAPHY_MODEL_VERSION,
  CITATION_ASSOC_VERSION,
  METRIC_VERSION,
  loadRuntimeAliasOverlay,
} from "../lib/ai-visibility/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.join(__dirname, "..", "fixtures", "ai-visibility");

function readFx(name) {
  return JSON.parse(fs.readFileSync(path.join(FIXTURES, name), "utf8"));
}

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("AI Visibility Phase 2C tests\n");

const universe = readFx("phase2c-entity-universe.json");
const overlay = loadRuntimeAliasOverlay();
const index = buildAiVisibilityEntityIndex({
  brands: universe.entities.filter((e) => e.entityType === "brand"),
  operators: universe.entities.filter((e) => e.entityType === "operator"),
  applyOverlay: true,
  runtimeOverlay: overlay,
});

console.log("Versions");
test("phase 2C version constants", () => {
  assert.equal(RESOLVER_VERSION, "ai_visibility_entity_resolver_v2");
  assert.equal(
    RECOMMENDATION_CLASSIFIER_VERSION,
    "ai_visibility_recommendation_classifier_v3"
  );
  assert.equal(CITATION_ASSOC_VERSION, "ai_visibility_citation_assoc_v1");
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
  assert.equal(GEOGRAPHY_MODEL_VERSION, "ai_visibility_geography_v1");
});

console.log("\nGolden classification set");
const golden = readFx("phase2c-classification-golden.json");
let correct = 0;
const misses = [];
for (const c of golden.cases) {
  const mentions = extractMentions({
    responseId: `resp_${c.id}`,
    text: c.text,
    entityIndex: index.aliasIndex,
    promptIntentTerritory: c.promptIntentTerritory,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  let role = hits[0]?.role || null;
  if (c.expectedRole === "first_recommendation") {
    const firstHits = hits.filter((m) => m.role === "first_recommendation");
    role = firstHits[0]?.role || role;
    // De-dupe: at most one first_recommendation mention for entity
    assert.ok(
      firstHits.length <= 1,
      `${c.id} first_recommendation inflation (${firstHits.length})`
    );
  }
  if (role === c.expectedRole) correct += 1;
  else misses.push({ id: c.id, expected: c.expectedRole, actual: role, entity: c.entityName });
}
const accuracy = correct / golden.cases.length;
test(`golden set size >= 60 (${golden.cases.length})`, () => {
  assert.ok(golden.cases.length >= 60);
});
test(`golden accuracy >= 90% (got ${(accuracy * 100).toFixed(1)}%)`, () => {
  if (misses.length) {
    console.error("    misses:", JSON.stringify(misses.slice(0, 12), null, 2));
  }
  assert.ok(accuracy >= 0.9, `accuracy ${accuracy}`);
});

console.log("\nFirst-recommendation de-dupe");
test("multiple Curio mentions → one first_recommendation role", () => {
  const text = `## Recommended shortlist
1. Curio Collection by Hilton
Curio Collection by Hilton has Mexico City hotels.
Curio Collection by Hilton loyalty is competitive.
`;
  const mentions = extractMentions({
    responseId: "resp_dedupe",
    text,
    entityIndex: index.aliasIndex,
    promptIntentTerritory: "Brand Selection",
  });
  const firsts = mentions.filter(
    (m) =>
      m.canonicalEntityName === "Curio Collection by Hilton" &&
      m.role === "first_recommendation"
  );
  assert.equal(firsts.length, 1);
  const obs = buildObservationFromExtractions({
    observationId: "o1",
    promptId: "p1",
    success: true,
    mentions,
  });
  assert.equal(obs.recommendedEntityIds[0], "receQkxgjlezsc1xg");
});

console.log("\nAssociated option vs recommendation");
test("branded residences association language", () => {
  const mentions = extractMentions({
    responseId: "resp_assoc",
    text: "Brands commonly associated with branded residences include Autograph Collection.",
    entityIndex: index.aliasIndex,
    promptIntentTerritory: "Branded Residences",
  });
  const m = mentions.find((x) => x.canonicalEntityName === "Autograph Collection");
  assert.equal(m?.role, "associated_option");
  assert.equal(m?.explicitRecommendation, false);
});

console.log("\nGeography model");
test("Mexico → CALA commercial region via COUNTRY_CONFIGS", () => {
  const r = resolveCountryGeography("Mexico");
  assert.equal(r.commercialRegion, "CALA");
  assert.equal(r.radarSubregion, "North America");
});
test("normalizePromptGeography country scope", () => {
  const g = normalizePromptGeography({
    country: "Mexico",
    region: "CALA",
    geography: "Mexico",
  });
  assert.equal(g.geographyScope, "country");
  assert.equal(g.regionName, "CALA");
  assert.equal(g.countryName, "Mexico");
});
test("normalizePromptGeography Caribbean subregion", () => {
  const g = normalizePromptGeography({
    geography: "Caribbean",
    region: "CALA",
  });
  assert.equal(g.geographyScope, "subregion");
  assert.equal(g.subregionName, "Caribbean");
  assert.equal(g.regionName, "CALA");
});
test("normalizePromptGeography global explicit", () => {
  const g = normalizePromptGeography({ geography: "Global", geographyScope: "global" });
  assert.equal(g.geographyScope, "global");
});
test("auditCanonicalGeographySources reports gaps", () => {
  const a = auditCanonicalGeographySources();
  assert.ok(a.REGIONS_FOUND.includes("CALA"));
  assert.ok(a.GAPS.length >= 1);
});

console.log("\nRegional metric isolation");
test("CALA vs Europe isolation + global isolation", () => {
  const obs = [
    {
      observationId: "a",
      promptId: "p_mx",
      success: true,
      presentEntityIds: ["receQkxgjlezsc1xg"],
      recommendedEntityIds: ["receQkxgjlezsc1xg"],
      firstPartyCitationEntityIds: [],
      geography: normalizePromptGeography({ country: "Mexico", region: "CALA" }),
    },
    {
      observationId: "b",
      promptId: "p_eu",
      success: true,
      presentEntityIds: ["recEJCTDj1zrsjPM6"],
      recommendedEntityIds: ["recEJCTDj1zrsjPM6"],
      firstPartyCitationEntityIds: [],
      geography: {
        geographyScope: "region",
        regionName: "Europe",
        countryName: null,
        subregionName: null,
        marketName: null,
      },
    },
    {
      observationId: "c",
      promptId: "p_gl",
      success: true,
      presentEntityIds: ["receQkxgjlezsc1xg"],
      recommendedEntityIds: [],
      firstPartyCitationEntityIds: [],
      geography: { geographyScope: "global", regionName: null },
    },
  ];
  const cala = filterObservationsByGeography(obs, { region: "CALA" });
  const europe = filterObservationsByGeography(obs, { region: "Europe" });
  const global = filterObservationsByGeography(obs, { geographyScope: "global" });
  assert.equal(cala.length, 1);
  assert.equal(europe.length, 1);
  assert.equal(global.length, 1);
  assert.equal(cala[0].observationId, "a");
  assert.equal(europe[0].observationId, "b");
  assert.equal(global[0].observationId, "c");

  const calaPresence = computeAiPresenceRate(cala, "receQkxgjlezsc1xg");
  const europePresence = computeAiPresenceRate(europe, "receQkxgjlezsc1xg");
  assert.equal(calaPresence.value, 1);
  assert.equal(europePresence.value, 0);

  const metrics = calculateVisibilityMetrics({
    entityId: "receQkxgjlezsc1xg",
    region: "CALA",
    observations: obs,
    computeAiPresenceRate,
  });
  assert.equal(metrics.observationCount, 1);
  assert.equal(metrics.presence.value, 1);
});

test("country rollup into CALA", () => {
  const obs = [
    {
      observationId: "mx",
      promptId: "p1",
      success: true,
      presentEntityIds: ["x"],
      recommendedEntityIds: [],
      firstPartyCitationEntityIds: [],
      geography: normalizePromptGeography({ country: "Mexico" }),
    },
  ];
  const rolled = filterObservationsByGeography(obs, { region: "CALA", allowCountryRollup: true });
  assert.equal(rolled.length, 1);
});

test("peer set descriptor shape", () => {
  const ps = buildPeerSetDescriptor({
    peerSetId: "peers_upper_upscale_cala",
    name: "Upper-Upscale Brands — CALA",
    entityType: "brand",
    geographyScope: "region",
    region: "CALA",
    entityIds: ["a", "b"],
  });
  assert.equal(ps.region, "CALA");
  assert.equal(ps.entityIds.length, 2);
});

test("evidence geography structural fields on observation", () => {
  const geo = normalizePromptGeography({ country: "Mexico", region: "CALA" });
  const obs = buildObservationFromExtractions({
    observationId: "o",
    promptId: "p",
    success: true,
    mentions: [],
    citations: [],
    geography: geo,
    intentTerritory: "Brand Selection",
  });
  assert.equal(obs.geography.geographyScope, "country");
  assert.equal(obs.intentTerritory, "Brand Selection");
});

test("section detection returns roles", () => {
  const sections = detectResponseSections(
    "## Recommended shortlist\n\n1. Autograph Collection\n\n## Sources\n\nAccording to Hilton."
  );
  assert.ok(sections.some((s) => s.sectionRole === "recommendation"));
});

console.log(`\nGolden accuracy: ${(accuracy * 100).toFixed(1)}% (${correct}/${golden.cases.length})`);
console.log(`Results: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
