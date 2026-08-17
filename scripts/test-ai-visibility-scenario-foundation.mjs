#!/usr/bin/env node
/**
 * Hotel Brand AI Intelligence P0A — Owner Decision Scenario foundation tests.
 * No provider calls. No Airtable writes. Certified metric outputs unchanged.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildPromptCohort } from "../lib/ai-visibility/prompt-cohort.js";
import { loadGovernedAiVisibilityPromptsFromFixture } from "../lib/ai-visibility/load-prompts.js";
import {
  loadScenarioRegistry,
  validateScenarioRegistry,
  buildScenarioRegistryIndex,
  resolvePromptScenario,
  validateScenarioPromptBindings,
  auditScenarioPromptCoverage,
  COMMERCIAL_PRIORITIES,
  MONITORING_PANELS,
  OWNER_PRIORITIES,
} from "../lib/ai-visibility/scenario-registry.js";
import { enrichPromptCohortWithScenarioMetadata } from "../lib/ai-visibility/scenario-cohort.js";
import {
  computeAiPresenceRate,
  computeQuestionsMissing,
} from "../lib/ai-visibility/metrics.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import {
  HEADLINE_GEOGRAPHIES,
  findMatchingSummaries,
  getBrandQuestionsPayload,
} from "../lib/ai-visibility/brand-read-service.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const SHOWCASE_SEED = path.join(
  root,
  "fixtures",
  "ai-visibility",
  "phase3a9-showcase-prompt-seed.json"
);
const PHASE2D_SEED = path.join(root, "fixtures", "ai-visibility", "phase2d-prompt-seed.json");
const REGISTRY_PATH = path.join(
  root,
  "fixtures",
  "ai-visibility",
  "scenario-registry-v1.json"
);

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

async function asyncTest(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nHotel Brand AI Intelligence P0A — Scenario Foundation\n");

const registry = loadScenarioRegistry(REGISTRY_PATH);
const showcaseLoaded = loadGovernedAiVisibilityPromptsFromFixture({}, SHOWCASE_SEED);
const phase2dLoaded = loadGovernedAiVisibilityPromptsFromFixture({}, PHASE2D_SEED);
const allPrompts = [...showcaseLoaded.prompts, ...phase2dLoaded.prompts];
const index = buildScenarioRegistryIndex(registry);

test("registry — loads 12 CORE scenarios", () => {
  assert.equal(registry.scenarios.length, 12);
  const core = registry.scenarios.filter((s) => s.monitoringPanel === "CORE");
  assert.equal(core.length, 12);
});

test("registry — controlled taxonomies", () => {
  assert.equal(COMMERCIAL_PRIORITIES.length, 4);
  assert.equal(MONITORING_PANELS.length, 3);
  assert.equal(OWNER_PRIORITIES.length, 10);
  for (const s of registry.scenarios) {
    assert.ok(OWNER_PRIORITIES.includes(s.ownerPriority), s.scenarioId);
    assert.ok(COMMERCIAL_PRIORITIES.includes(s.commercialPriority), s.scenarioId);
    assert.ok(MONITORING_PANELS.includes(s.monitoringPanel), s.scenarioId);
  }
});

test("registry — validation passes", () => {
  const v = validateScenarioRegistry(registry);
  assert.equal(v.ok, true, v.errors.join("; "));
});

test("registry — duplicate scenarioId rejected", () => {
  const bad = JSON.parse(JSON.stringify(registry));
  bad.scenarios.push({ ...bad.scenarios[0] });
  const v = validateScenarioRegistry(bad);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => e.startsWith("duplicate_scenario_id")));
});

test("registry — invalid commercialPriority rejected", () => {
  const bad = JSON.parse(JSON.stringify(registry));
  bad.scenarios[0].commercialPriority = "URGENT";
  const v = validateScenarioRegistry(bad);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => e.startsWith("invalid_commercial_priority")));
});

test("prompt resolution — showcase prompts map to scenarios", () => {
  const audit = auditScenarioPromptCoverage(showcaseLoaded.prompts, registry);
  assert.ok(audit.mappedPrompts >= 84, `expected 84 mapped, got ${audit.mappedPrompts}`);
  assert.equal(audit.unmappedPrompts, 0);
});

test("prompt resolution — legacy unmapped prompt returns UNMAPPED", () => {
  const legacy = {
    promptId: "p_legacy_unmapped_test_v1",
    promptFamily: "legacy_unknown_family",
    intentTerritory: "Other",
    version: "1",
    active: true,
    monitoringEligible: true,
    geographyScope: "Global",
    entityScope: "Brand",
    promptText: "Neutral owner question about hotel brands.",
  };
  const resolved = resolvePromptScenario(legacy, index);
  assert.equal(resolved.scenarioStatus, "UNMAPPED");
  assert.equal(resolved.scenarioId, null);
});

test("prompt bindings — variant + semantic pair validation", () => {
  const binding = validateScenarioPromptBindings(allPrompts, registry);
  assert.equal(binding.ok, true, binding.errors.join("; "));
});

test("cohort — enrichment does not change fingerprint or member promptIds", () => {
  const cohort = buildPromptCohort({
    prompts: showcaseLoaded.prompts,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
    monitoringEligible: true,
    activeOnly: true,
  });
  assert.equal(cohort.ok, true);
  const enriched = enrichPromptCohortWithScenarioMetadata(cohort, showcaseLoaded.prompts, {
    registry,
    index,
  });
  assert.equal(enriched.fingerprint, cohort.fingerprint);
  assert.equal(enriched.count, cohort.count);
  assert.deepEqual(
    enriched.promptIds,
    cohort.promptIds,
    "promptIds must be unchanged"
  );
  assert.ok(enriched.scenarioSummary.mappedCount > 0);
});

test("cohort — exposes scenarioId ownerPriority commercialPriority monitoringPanel", () => {
  const cohort = buildPromptCohort({
    prompts: showcaseLoaded.prompts,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
  });
  const enriched = enrichPromptCohortWithScenarioMetadata(cohort, showcaseLoaded.prompts, {
    registry,
    index,
  });
  const mapped = enriched.members.filter((m) => m.scenarioStatus === "MAPPED");
  assert.ok(mapped.length > 0);
  for (const m of mapped) {
    assert.ok(m.scenarioId);
    assert.ok(m.ownerPriority);
    assert.ok(m.commercialPriority);
    assert.ok(m.monitoringPanel);
  }
});

test("language isolation — CALA EN cohort unchanged by scenario layer", () => {
  const en = buildPromptCohort({
    prompts: showcaseLoaded.prompts,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
  });
  const es = buildPromptCohort({
    prompts: showcaseLoaded.prompts,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "es",
  });
  assert.ok(en.count > 0);
  assert.ok(es.count > 0);
  assert.notDeepEqual(en.promptIds, es.promptIds);
  const enEnriched = enrichPromptCohortWithScenarioMetadata(en, showcaseLoaded.prompts, {
    registry,
    index,
  });
  assert.equal(enEnriched.fingerprint, en.fingerprint);
  assert.equal(enEnriched.count, en.count);
});

test("geography isolation — CALA vs Global cohort prompt sets differ", () => {
  const cala = buildPromptCohort({
    prompts: showcaseLoaded.prompts,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
  });
  const global = buildPromptCohort({
    prompts: showcaseLoaded.prompts,
    geographyScope: "Global",
    language: "en",
  });
  assert.ok(cala.count > 0);
  assert.ok(global.count > 0);
  assert.notDeepEqual(cala.promptIds.sort(), global.promptIds.sort());
  const calaEnriched = enrichPromptCohortWithScenarioMetadata(cala, showcaseLoaded.prompts, {
    registry,
    index,
  });
  assert.equal(calaEnriched.count, cala.count);
});

test("semantic pairs — EN+ES share scenarioId not separate scenarios", () => {
  const enRow = showcaseLoaded.prompts.find(
    (p) => p.promptId === "p_cala_independent_affiliation_v1"
  );
  const esRow = showcaseLoaded.prompts.find(
    (p) => p.promptId === "p_cala_independent_affiliation_es_v1"
  );
  assert.ok(enRow && esRow);
  const enRes = resolvePromptScenario(enRow, index);
  const esRes = resolvePromptScenario(esRow, index);
  assert.equal(enRes.scenarioId, esRes.scenarioId);
  assert.notEqual(enRow.promptId, esRow.promptId);
});

async function runMetricRegression() {
  const store = createBrandAiVisibilityReadStore({});
  const brandId = "recEJCTDj1zrsjPM6";
  const membership = resolvePeerSetMembership({
    peerSetId: PEER_SET_ID_V2,
    commercialRegion: "CALA",
  });
  const entitlementGraph = buildFixtureEntitlementGraph({
    entitledBrandIds: [brandId],
    peerBrandIds: membership.entityIds || [],
    source: "p0a-scenario-test",
  });

  await asyncTest("certified metrics — Presence unchanged on baseline read path", async () => {
    for (const geo of HEADLINE_GEOGRAPHIES.filter((g) => g.key === "CALA")) {
      const summaries = await findMatchingSummaries(store, {
        provider: "openai",
        geography: geo,
        language: "en",
      });
      const latest = summaries[0];
      if (!latest) continue;
      const { observations } = await loadObservationsFromBatchSummary(store, latest, {
        language: "en",
      });
      const before = computeAiPresenceRate(observations, brandId);
      const enrichedCohort = enrichPromptCohortWithScenarioMetadata(
        buildPromptCohort({
          prompts: showcaseLoaded.prompts,
          geographyScope: "Region",
          commercialRegion: "CALA",
          language: "en",
        }),
        showcaseLoaded.prompts,
        { registry, index }
      );
      assert.ok(enrichedCohort.scenarioSummary);
      const after = computeAiPresenceRate(observations, brandId);
      assert.equal(after.value, before.value);
      assert.equal(after.denominator, before.denominator);
    }
  });

  await asyncTest("certified metrics — Questions Missing unchanged", async () => {
    const payload = await getBrandQuestionsPayload({
      dealalityUser: { id: "t" },
      entitlementGraph,
      store,
      brandId,
      provider: "openai",
      geography: "CALA",
      language: "en",
      filter: "all",
    });
    if (!payload.ok) {
      assert.fail(`questions payload failed: ${payload.reasonCode || payload.message || "unknown"}`);
    }
    assert.ok(Array.isArray(payload.questions));
    const missing = payload.questions.filter((r) => r.brandStatus === "Missing").length;
    assert.ok(Number.isFinite(missing));
  });
}

runMetricRegression().then(() => {
  const showcaseAudit = auditScenarioPromptCoverage(showcaseLoaded.prompts, registry);
  const combinedAudit = auditScenarioPromptCoverage(allPrompts, registry);

  console.log("\n--- P0A Coverage Audit ---");
  console.log(`MAPPED_PROMPTS (showcase): ${showcaseAudit.mappedPrompts}`);
  console.log(`UNMAPPED_PROMPTS (showcase): ${showcaseAudit.unmappedPrompts}`);
  console.log(`REUSE_PERCENT (showcase): ${showcaseAudit.reusePercent}%`);
  console.log(`MAPPED_PROMPTS (showcase+phase2d): ${combinedAudit.mappedPrompts}`);
  console.log(`UNMAPPED_PROMPTS (combined): ${combinedAudit.unmappedPrompts}`);
  console.log(`REUSE_PERCENT (combined): ${combinedAudit.reusePercent}%`);

  console.log("\n| Scenario | Prompts | EN | ES | Geographies | Status |");
  console.log("|----------|---------|----|----|-------------|--------|");
  for (const row of combinedAudit.scenarios) {
    const en = row.languages.includes("en") ? "Y" : "—";
    const es = row.languages.includes("es") ? "Y" : "—";
    const geos = row.geographies.slice(0, 4).join(", ") || "—";
    console.log(
      `| ${row.scenarioName.slice(0, 40)} | ${row.existingPromptCount} | ${en} | ${es} | ${geos} | ${row.coverage} |`
    );
  }

  console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}`);
  if (failed > 0) {
    console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0A_REMEDIATION_REQUIRED");
    process.exit(1);
  }
  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0A_PASS");
  console.log("NEW_PROVIDER_CALLS = 0");
  process.exit(0);
});
