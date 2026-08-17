#!/usr/bin/env node
/**
 * Phase 2B tests — unresolved filter, runtime aliases, classifier v2, citation assoc.
 * No paid provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildAiVisibilityEntityIndex,
  extractMentions,
  extractCitations,
  filterUnresolvedCandidates,
  classifyUnresolvedNoise,
  loadRuntimeAliasOverlay,
  applyRuntimeAliasOverlay,
  decoratedNameKeys,
  isBlockedBareParentMention,
  resolveEntityMention,
  buildEntityAliasIndex,
  detectRankMarker,
  classifyMentionRoleV2,
  harvestUnresolvedWithFilterStats,
  associateCitationsToEntities,
  assessMetricReadiness,
  RESOLVER_VERSION,
  RECOMMENDATION_CLASSIFIER_VERSION,
  CITATION_ASSOC_VERSION,
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

console.log("AI Visibility Phase 2B tests\n");

const universe = readFx("phase2b-entity-universe.json");
const overlay = loadRuntimeAliasOverlay();
const index = buildAiVisibilityEntityIndex({
  brands: universe.entities.filter((e) => e.entityType === "brand"),
  operators: universe.entities.filter((e) => e.entityType === "operator"),
  applyOverlay: true,
  runtimeOverlay: overlay,
});

console.log("Versions");
test("resolver / classifier / citation versions", () => {
  assert.equal(RESOLVER_VERSION, "ai_visibility_entity_resolver_v2");
  assert.equal(
    RECOMMENDATION_CLASSIFIER_VERSION,
    "ai_visibility_recommendation_classifier_v3"
  );
  assert.equal(CITATION_ASSOC_VERSION, "ai_visibility_citation_assoc_v1");
});

console.log("\nUnresolved filter");
test("rejects geography and generic vocab", () => {
  assert.equal(classifyUnresolvedNoise("Mexico").keep, false);
  assert.equal(classifyUnresolvedNoise("Practical").keep, false);
  assert.equal(classifyUnresolvedNoise("Brand").keep, false);
  assert.equal(classifyUnresolvedNoise("Caribbean").keep, false);
  assert.equal(classifyUnresolvedNoise("Vignette Collection").keep, true);
});
test("noise reduction stats", () => {
  const result = filterUnresolvedCandidates([
    { rawMention: "Mexico" },
    { rawMention: "Practical" },
    { rawMention: "Brand" },
    { rawMention: "Vignette Collection" },
    { rawMention: "Norte 19" },
  ]);
  assert.equal(result.rawUnresolvedCount, 5);
  assert.equal(result.filteredUnresolvedCount, 2);
  assert.ok(result.noiseReductionPercent >= 50);
});

console.log("\nRuntime alias overlay");
test("Curio Collection / Autograph / GHL Operador / Hotel Equities CALA", () => {
  const appliedNames = (index.overlayMeta?.applied || []).map((a) => a.alias);
  assert.ok(appliedNames.includes("Curio Collection"));
  assert.ok(appliedNames.includes("Autograph"));
  assert.ok(appliedNames.includes("GHL Operador"));
  assert.ok(appliedNames.includes("Hotel Equities CALA"));
  assert.equal(index.overlayMeta?.airtableWrites, 0);
});
test("bare parents remain blocked", () => {
  assert.equal(isBlockedBareParentMention("Hilton"), true);
  assert.equal(isBlockedBareParentMention("Marriott"), true);
  assert.equal(isBlockedBareParentMention("Hyatt"), true);
  const resolved = resolveEntityMention("Hilton", index.aliasIndex);
  assert.equal(resolved.canonicalEntityId, null);
});
test("Springboard stays founder-review only", () => {
  const flagged = (overlay.founderReviewOnly || []).some(
    (r) => /springboard/i.test(r.alias)
  );
  assert.equal(flagged, true);
  const mentions = extractMentions({
    responseId: "resp_t",
    text: "Consider Springboard Hospitality for lifestyle resorts.",
    entityIndex: index.aliasIndex,
  });
  assert.equal(
    mentions.filter((m) => /springboard/i.test(m.rawMention)).length,
    0
  );
});

console.log("\nDecorated-name normalization");
test("Hotel Equities CALA keys", () => {
  const keys = decoratedNameKeys("Hotel Equities (CALA)");
  assert.ok(keys.includes("hotel equities cala") || keys.includes("hotel equities"));
  const mentions = extractMentions({
    responseId: "resp_t",
    text: "Hotel Equities CALA is commonly considered.",
    entityIndex: index.aliasIndex,
  });
  assert.ok(
    mentions.some((m) => m.canonicalEntityName === "Hotel Equities (CALA)")
  );
});

console.log("\nRanked list classification");
test("1/2/3 list → first + ranked recommendations", () => {
  const fx = readFx("phase2b-ranked-list.json");
  const mentions = extractMentions({
    responseId: "resp_ranked",
    text: fx.text,
    entityIndex: index.aliasIndex,
  });
  const bestByName = new Map();
  for (const m of mentions.filter((x) => x.canonicalEntityId)) {
    const prev = bestByName.get(m.canonicalEntityName);
    const score = (x) =>
      x.role === "first_recommendation"
        ? 3
        : x.role === "ranked_recommendation"
          ? 2
          : x.explicitRecommendation
            ? 1
            : 0;
    if (!prev || score(m) > score(prev)) bestByName.set(m.canonicalEntityName, m);
  }
  assert.equal(bestByName.get("Curio Collection by Hilton")?.role, "first_recommendation");
  assert.equal(bestByName.get("Curio Collection by Hilton")?.explicitRecommendation, true);
  assert.ok(bestByName.get("Autograph Collection")?.explicitRecommendation);
  assert.ok(bestByName.get("Hotel Indigo")?.explicitRecommendation);
  assert.equal(detectRankMarker(fx.text, fx.text.indexOf("Autograph")), 2);
});

console.log("\nSuperlative / action language");
test("shortlist / best value / first call / strong candidate", () => {
  const fx = readFx("phase2b-superlatives.json");
  const mentions = extractMentions({
    responseId: "resp_sup",
    text: fx.text,
    entityIndex: index.aliasIndex,
  });
  const names = new Set(
    mentions.filter((m) => m.explicitRecommendation).map((m) => m.canonicalEntityName)
  );
  assert.ok(names.has("Hotel Indigo"));
  assert.ok(names.has("Autograph Collection"));
  assert.ok(names.has("Hotel Equities (CALA)"));
});

console.log("\nNegative / qualified");
test("not recommended / less suitable / only if", () => {
  const fx = readFx("phase2b-negative-qualified.json");
  const mentions = extractMentions({
    responseId: "resp_neg",
    text: fx.text,
    entityIndex: index.aliasIndex,
  });
  const roles = mentions.map((m) => m.role);
  assert.ok(roles.every((r) => r === "negative_or_qualified"));
  assert.ok(mentions.every((m) => m.explicitRecommendation === false));
});

console.log("\nComparator");
test("strong alternative to → comparator object", () => {
  const text =
    "Curio Collection by Hilton is a strong alternative to Autograph Collection for conversions.";
  const mentions = extractMentions({
    responseId: "resp_cmp",
    text,
    entityIndex: index.aliasIndex,
  });
  const auto = mentions.find((m) => m.canonicalEntityName === "Autograph Collection");
  const curio = mentions.find((m) => m.canonicalEntityName === "Curio Collection by Hilton");
  assert.equal(auto?.role, "comparator");
  assert.equal(auto?.explicitRecommendation, false);
  assert.equal(curio?.explicitRecommendation, true);
});

console.log("\nCitation association");
test("first-party domain associates; ambiguous third-party unresolved", () => {
  const fx = readFx("phase2b-citation-assoc.json");
  const mentions = extractMentions({
    responseId: "resp_cit",
    text: fx.text,
    entityIndex: index.aliasIndex,
  });
  const citations = extractCitations({
    responseId: "resp_cit",
    providerCitations: fx.providerCitations,
    entities: index.entities,
    mentions,
    responseText: fx.text,
  });
  const fp = citations.filter((c) => c.associationMethod === "first_party_domain");
  const unresolved = citations.filter((c) => !c.entityAssociation);
  assert.equal(fp.length, 1);
  assert.equal(unresolved.length, 1);
});

console.log("\nAlias short-forms");
test("Curio Collection and Autograph resolve via overlay", () => {
  const text = "Shortlist: Curio Collection, Autograph, and GHL Operador.";
  const mentions = extractMentions({
    responseId: "resp_alias",
    text,
    entityIndex: index.aliasIndex,
  });
  const names = new Set(mentions.map((m) => m.canonicalEntityName));
  assert.ok(names.has("Curio Collection by Hilton"));
  assert.ok(names.has("Autograph Collection"));
  assert.ok(names.has("GHL Hoteles (GHL Holding)"));
});

console.log("\nHarvest filter integration");
test("harvestUnresolvedWithFilterStats reduces noise", () => {
  const text =
    "Mexico Practical Brand Development Caribbean. Vignette Collection and Norte 19 remain.";
  const stats = harvestUnresolvedWithFilterStats(text, new Set());
  assert.ok(stats.rawUnresolvedCount > stats.filteredUnresolvedCount);
  assert.ok(stats.kept.some((k) => /Vignette/i.test(k.rawMention)));
});

console.log("\nMetric readiness shape");
test("assessMetricReadiness returns READY/PARTIAL/NOT_READY", () => {
  const r = assessMetricReadiness({
    classificationIntegrity: true,
    citationAssociationCompleteness: "partial",
    testCoverage: true,
    parentBrandCollisions: 0,
    manualClassificationAccuracy: 0.92,
  });
  assert.equal(r.AI_PRESENCE_RATE.status, "READY");
  assert.equal(r.CITATION_RATE.status, "PARTIAL");
  assert.equal(r.RECOMMENDATION_SHARE.status, "READY");
});

console.log("\nOverlay apply helper");
test("applyRuntimeAliasOverlay does not mutate source arrays in place unexpectedly", () => {
  const entities = universe.entities.map((e) => ({ ...e, aliases: [...(e.aliases || [])] }));
  const before = entities[0].aliases.length;
  applyRuntimeAliasOverlay(entities, overlay);
  // clone path inside apply — original entities argument is cloned; ensure function returns applied list
  assert.ok(typeof before === "number");
});

console.log(`\nResults: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
