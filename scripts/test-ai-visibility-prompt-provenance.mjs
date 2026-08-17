#!/usr/bin/env node
/**
 * Hotel Brand AI Intelligence — observed demand + prompt provenance.
 * No provider calls. No monitoring. No Census. No Recommendation metrics.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildPromptCohort } from "../lib/ai-visibility/prompt-cohort.js";
import { loadGovernedAiVisibilityPromptsFromFixture } from "../lib/ai-visibility/load-prompts.js";
import {
  loadScenarioRegistry,
  buildScenarioRegistryIndex,
  resolvePromptScenario,
} from "../lib/ai-visibility/scenario-registry.js";
import { computeAiPresenceRate, computeQuestionsMissing } from "../lib/ai-visibility/metrics.js";
import {
  PROMPT_PROVENANCE_VERSION,
  PROMPT_ORIGINS,
  CLIENT_PROMPT_ORIGINS,
  DEMAND_TIER_METHOD,
  SOURCE_NAMESPACES,
  GAP_DEMAND_PRIORITIZATION_HOOK,
  EXISTING_OBSERVED_SIGNAL_SOURCES,
  validateDemandEvidence,
  validateProvenanceRecord,
  resolvePromptProvenance,
  attachPromptProvenance,
  toClientPromptOrigin,
  shouldShowExecutivePromptMix,
  buildPromptOriginSummary,
  buildLibraryPromptOriginSummary,
  auditPromptUniverse,
  estimateProvenanceMonitoringCost,
  loadObservedDemandSeed,
  loadDemandSignalRegistry,
  loadPromptProvenanceOverlay,
  enrichRowWithPromptOrigin,
} from "../lib/ai-visibility/prompt-provenance.js";
import { getPromptCoreFieldSpecs } from "../lib/ai-visibility/airtable-schema-proposal.js";
import { KNOWN_AI_VISIBILITY_PROVIDER_IDS } from "../lib/ai-visibility/provider-dimension.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const SHOWCASE_SEED = path.join(root, "fixtures", "ai-visibility", "phase3a9-showcase-prompt-seed.json");
const PHASE2D_SEED = path.join(root, "fixtures", "ai-visibility", "phase2d-prompt-seed.json");
const BRAND_JS = path.join(root, "public", "js", "ai-visibility", "ai-visibility-brand.js");
const BRAND_HTML = path.join(root, "public", "ai-visibility-brand.html");

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

console.log("\nHotel Brand AI Intelligence — Prompt Provenance / Observed Demand\n");

const showcase = loadGovernedAiVisibilityPromptsFromFixture({}, SHOWCASE_SEED);
const phase2d = loadGovernedAiVisibilityPromptsFromFixture({}, PHASE2D_SEED);
const allPrompts = [...showcase.prompts, ...phase2d.prompts];
const registry = loadScenarioRegistry();
const scenarioIndex = buildScenarioRegistryIndex(registry);
const overlay = loadPromptProvenanceOverlay();
const seed = loadObservedDemandSeed();
const signals = loadDemandSignalRegistry();
const audit = auditPromptUniverse(allPrompts, { overlay, registry, scenarioIndex });
const cost = estimateProvenanceMonitoringCost({
  existingPrompts: audit.TOTAL_ACTIVE_PROMPTS,
  proposedObserved: 0,
  proposedDerived: 0,
});

const obs = [
  {
    promptId: "p_cala_collection_affiliation_v1",
    success: true,
    presentEntityIds: ["recEJCTDj1zrsjPM6"],
  },
  {
    promptId: "p_cala_collection_affiliation_es_v1",
    success: true,
    presentEntityIds: [],
  },
];

test("version + enums", () => {
  assert.equal(PROMPT_PROVENANCE_VERSION, "ai_visibility_prompt_provenance_v1");
  assert.deepEqual([...PROMPT_ORIGINS], [
    "OBSERVED",
    "DERIVED",
    "SCENARIO",
    "LEGACY_UNCLASSIFIED",
  ]);
  assert.ok(!CLIENT_PROMPT_ORIGINS.includes("LEGACY_UNCLASSIFIED"));
});

test("OBSERVED requires provenance evidence", () => {
  const v = validateProvenanceRecord({ promptOrigin: "OBSERVED" });
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => e.includes("observed_requires")));
});

test("OBSERVED passes with evidence object", () => {
  const evidence = {
    demandSignalId: "ds_test_1",
    sourceType: "LICENSED_SEO_DATASET",
    sourceName: "Test licensed dataset",
    queryText: "hotel franchise vs management agreement",
    evidenceReference: "fixture://test",
    sourceConfidence: "STRONG_OBSERVED",
    originSourceNamespace: SOURCE_NAMESPACES.PROMPT_ORIGIN,
  };
  assert.equal(validateDemandEvidence(evidence).ok, true);
  const v = validateProvenanceRecord({
    promptOrigin: "OBSERVED",
    originSourceType: "LICENSED_SEO_DATASET",
    demandEvidence: [evidence],
  });
  assert.equal(v.ok, true, v.errors.join("; "));
});

test("DERIVED requires observed parent", () => {
  const v = validateProvenanceRecord({ promptOrigin: "DERIVED" });
  assert.equal(v.ok, false);
  assert.ok(v.errors.includes("derived_requires_observed_parent"));
});

test("DERIVED parent linkage pass", () => {
  const v = validateProvenanceRecord(
    {
      promptOrigin: "DERIVED",
      derivedFromObservedPromptId: "p_obs_parent",
    },
    {
      promptsById: new Map([
        ["p_obs_parent", { promptId: "p_obs_parent", promptOrigin: "OBSERVED" }],
      ]),
    }
  );
  assert.equal(v.ok, true, v.errors.join("; "));
});

test("DERIVED cannot use scenario parent", () => {
  const v = validateProvenanceRecord(
    {
      promptOrigin: "DERIVED",
      derivedFromObservedPromptId: "p_scen",
    },
    {
      promptsById: new Map([
        ["p_scen", { promptId: "p_scen", promptOrigin: "SCENARIO" }],
      ]),
    }
  );
  assert.equal(v.ok, false);
  assert.ok(v.errors.includes("derived_parent_must_be_observed"));
});

test("SCENARIO does not require demand evidence", () => {
  const v = validateProvenanceRecord({ promptOrigin: "SCENARIO", scenarioId: "scenario_x" });
  assert.equal(v.ok, true, v.errors.join("; "));
});

test("origin independent of scenarioId — observed + scenario can coexist", () => {
  const evidence = {
    demandSignalId: "ds_coexist",
    sourceType: "SEARCH_CONSOLE",
    sourceName: "Dealality Search Console",
    evidenceReference: "sc://q1",
    sourceConfidence: "DIRECT_MEASURED",
  };
  const v = validateProvenanceRecord({
    promptOrigin: "OBSERVED",
    scenarioId: "scenario_independent_uu_conversion_v1",
    demandEvidence: [evidence],
    originSourceType: "SEARCH_CONSOLE",
  });
  assert.equal(v.ok, true, v.errors.join("; "));
  const rec = resolvePromptProvenance(
    {
      promptId: "p_coexist_test",
      promptFamily: "showcase_conversion_independent_affiliation",
      intentTerritory: "Conversion",
      version: "1",
      active: true,
      monitoringEligible: true,
      geographyScope: "Region",
      commercialRegion: "CALA",
      entityScope: "Brand",
      promptText: "Neutral conversion affiliation question.",
    },
    {
      overlay: {
        byPromptId: new Map([
          [
            "p_coexist_test",
            {
              promptId: "p_coexist_test",
              promptOrigin: "OBSERVED",
              originSourceType: "SEARCH_CONSOLE",
              demandEvidence: [evidence],
            },
          ],
        ]),
      },
      scenarioIndex,
    }
  );
  assert.equal(rec.promptOrigin, "OBSERVED");
  assert.equal(rec.scenarioId, "scenario_independent_uu_conversion_v1");
});

test("unknown origin remains safe — LEGACY_UNCLASSIFIED", () => {
  const rec = resolvePromptProvenance({
    promptId: "p_legacy_unmapped_test_v1",
    promptFamily: "legacy_unknown_family",
    intentTerritory: "Other",
    version: "1",
    active: true,
    monitoringEligible: true,
    geographyScope: "Global",
    entityScope: "Brand",
    promptText: "Neutral owner question about hotel brands.",
  });
  assert.equal(rec.promptOrigin, "LEGACY_UNCLASSIFIED");
  const client = toClientPromptOrigin(rec);
  assert.equal(client.showOriginBadge, false);
  assert.equal(client.promptOrigin, null);
});

test("demand tier cannot be assigned without methodology", () => {
  const v = validateProvenanceRecord({
    promptOrigin: "SCENARIO",
    demandTier: "HIGH",
  });
  assert.equal(v.ok, false);
  assert.ok(v.errors.includes("demand_tier_requires_methodology"));
});

test("no fake volume field", () => {
  const v = validateProvenanceRecord({
    promptOrigin: "SCENARIO",
    monthlySearchVolume: 2300,
  });
  assert.equal(v.ok, false);
  assert.ok(v.errors.some((e) => e.startsWith("forbidden_volume_field")));
});

test("response citation source ≠ prompt origin source", () => {
  const v = validateProvenanceRecord({
    promptOrigin: "OBSERVED",
    originSourceNamespace: SOURCE_NAMESPACES.RESPONSE_CITATION,
    demandEvidence: [
      {
        demandSignalId: "ds_x",
        sourceType: "PUBLIC_QUESTION_SOURCE",
        sourceName: "Forum",
        evidenceReference: "http://example.test",
        sourceConfidence: "WEAK",
        sourceNamespace: SOURCE_NAMESPACES.RESPONSE_CITATION,
      },
    ],
  });
  assert.equal(v.ok, false);
  assert.ok(
    v.errors.some(
      (e) =>
        e.includes("citation_source") || e.includes("prompt_origin_must_not_use_citation")
    )
  );
  assert.equal(SOURCE_NAMESPACES.PROMPT_ORIGIN, "PROMPT_ORIGIN_SOURCE");
  assert.equal(SOURCE_NAMESPACES.RESPONSE_CITATION, "RESPONSE_CITATION_SOURCE");
});

test("legacy prompts remain compatible — IDs unchanged after attach", () => {
  const before = allPrompts.map((p) => p.promptId).sort();
  const after = allPrompts.map((p) => attachPromptProvenance(p).promptId).sort();
  assert.deepEqual(after, before);
  assert.ok(allPrompts.every((p) => PROMPT_ORIGINS.includes(p.promptOrigin)));
});

test("UI hides empty provenance mix", () => {
  assert.equal(shouldShowExecutivePromptMix({ observed: 0, derived: 0, scenario: 24 }), false);
  assert.equal(shouldShowExecutivePromptMix({ observed: 1, derived: 0, scenario: 24 }), false);
  assert.equal(shouldShowExecutivePromptMix({ observed: 10, derived: 0, scenario: 24 }), true);
  const html = fs.readFileSync(BRAND_HTML, "utf8");
  assert.ok(html.includes('id="aivExecPromptMix"'));
  assert.ok(html.includes("hidden"));
  const js = fs.readFileSync(BRAND_JS, "utf8");
  assert.ok(js.includes("renderExecutivePromptMix"));
  assert.ok(js.includes("promptOriginBadgeHtml"));
  assert.ok(js.includes('summary.showPromptMix === true'));
  assert.ok(!js.includes("0 observed prompts"));
});

test("detail origin badges present in client", () => {
  const js = fs.readFileSync(BRAND_JS, "utf8");
  assert.ok(js.includes("aiv-origin-badge"));
  assert.ok(js.includes("Observed"));
  assert.ok(js.includes("Derived"));
  assert.ok(js.includes("Scenario"));
  assert.ok(js.includes("LEGACY_UNCLASSIFIED"));
});

test("scenario-mapped library prompts resolve to SCENARIO not OBSERVED", () => {
  const mapped = allPrompts.filter((p) => {
    const s = resolvePromptScenario(p, scenarioIndex);
    return s.scenarioStatus === "MAPPED";
  });
  assert.ok(mapped.length > 0);
  for (const p of mapped.slice(0, 20)) {
    assert.equal(p.promptOrigin, "SCENARIO", p.promptId);
    assert.equal(p.demandTier, "UNKNOWN");
  }
  assert.equal(audit.OBSERVED, 0);
  assert.equal(audit.DERIVED, 0);
});

test("do not infer origin from wording", () => {
  const rec = resolvePromptProvenance({
    promptId: "p_wording_trap",
    promptFamily: "legacy_unknown_family",
    intentTerritory: "Brand Selection",
    version: "1",
    active: true,
    monitoringEligible: true,
    geographyScope: "Global",
    entityScope: "Brand",
    promptText: "What are the best hotel brands for independent hotels?",
  });
  assert.equal(rec.promptOrigin, "LEGACY_UNCLASSIFIED");
});

test("cohort fingerprint unchanged by provenance fields", () => {
  const raw = JSON.parse(fs.readFileSync(SHOWCASE_SEED, "utf8")).prompts;
  const stripped = raw.map((p) => {
    const { promptOrigin, provenance, demandTier, samplingPriority, ...rest } = p;
    return rest;
  });
  const withProv = stripped.map((p) => attachPromptProvenance(p));
  const a = buildPromptCohort({
    prompts: stripped,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
  });
  const b = buildPromptCohort({
    prompts: withProv,
    geographyScope: "Region",
    commercialRegion: "CALA",
    language: "en",
  });
  assert.equal(a.ok, true);
  assert.equal(b.ok, true);
  assert.equal(a.fingerprint, b.fingerprint);
  assert.equal(a.count, b.count);
});

test("PRESENCE / QM certified inputs unchanged", () => {
  const entityId = "recEJCTDj1zrsjPM6";
  const presenceA = computeAiPresenceRate(obs, entityId);
  const presenceB = computeAiPresenceRate(
    obs.map((o) => ({ ...o, promptOrigin: "SCENARIO" })),
    entityId
  );
  assert.equal(presenceA.value, presenceB.value);
  const qmA = computeQuestionsMissing(obs, entityId);
  const qmB = computeQuestionsMissing(
    obs.map((o) => ({ ...o, promptOrigin: "SCENARIO" })),
    entityId
  );
  assert.equal(qmA.value, qmB.value);
});

test("P0C demand hook does not recalculate gaps", () => {
  assert.equal(GAP_DEMAND_PRIORITIZATION_HOOK.ENABLED, false);
  assert.equal(GAP_DEMAND_PRIORITIZATION_HOOK.RECALCULATES_P0C, false);
});

test("Airtable proposal includes origin fields; live apply not implied", () => {
  const names = getPromptCoreFieldSpecs().map((f) => f.name);
  assert.ok(names.includes("Prompt Origin"));
  assert.ok(names.includes("Demand Tier"));
  assert.ok(!names.includes("Search Volume"));
  assert.ok(!names.includes("Monthly Search Volume"));
});

test("observed seed partial — signals stored, live overlay empty", () => {
  assert.equal(seed.seedStatus, "OBSERVED_DEMAND_SEED_PARTIAL");
  assert.equal((seed.includedThemes || []).length, 9);
  assert.equal(seed.promptMixEligible, false);
  assert.equal(seed.activationStatus, "NOT_ATTACHED_TO_LIVE_PROMPTS");
  assert.ok((seed.candidateThemes || []).length >= 10);
  assert.equal((signals.signals || []).length, 13);
  assert.equal((overlay.classifications || []).length, 0);
  assert.equal(audit.OBSERVED, 0);
  assert.equal(overlay.observedDemandSeedStatus, "OBSERVED_DEMAND_SEED_PARTIAL");
});

test("existing product sources stay unused; DataForSEO sample is file-store only", () => {
  const dfs = EXISTING_OBSERVED_SIGNAL_SOURCES.find((s) => String(s.SOURCE).includes("DataForSEO"));
  assert.ok(dfs);
  assert.equal(dfs.USABLE, "FILE_STORE_ONLY");
  assert.equal(dfs.LIVE_MONITORING, "NO");
  assert.equal(dfs.SIGNALS_FOUND, 13);
  assert.equal(dfs.DISTINCT_THEMES, 9);
  const others = EXISTING_OBSERVED_SIGNAL_SOURCES.filter((s) => s !== dfs);
  assert.ok(others.every((s) => s.USABLE === "NO"));
  assert.equal(DEMAND_TIER_METHOD.exactVolumeInvented, false);
  assert.equal(DEMAND_TIER_METHOD.numericConfidence, false);
});

test("cost model — incremental prompts 0, no execution", () => {
  assert.equal(cost.TOTAL_INCREMENT, 0);
  assert.equal(cost.PROVIDER_CALLS, 0);
  assert.equal(cost.MONITORING_RUNS, 0);
  assert.equal(cost.PROVIDERS, KNOWN_AI_VISIBILITY_PROVIDER_IDS.length);
  assert.equal(
    cost.CALLS_PER_FULL_RUN,
    audit.TOTAL_ACTIVE_PROMPTS * KNOWN_AI_VISIBILITY_PROVIDER_IDS.length
  );
});

test("library prompt origin summary hides mix while observed=0", () => {
  const summary = buildLibraryPromptOriginSummary({ key: "CALA", commercialRegion: "CALA" });
  assert.equal(summary.observed, 0);
  assert.equal(summary.derived, 0);
  assert.equal(summary.showPromptMix, false);
  assert.ok(summary.scenario > 0);
});

test("client origin enrich on question row", () => {
  const mapped = allPrompts.find((p) => p.promptOrigin === "SCENARIO");
  assert.ok(mapped);
  const row = enrichRowWithPromptOrigin(
    { promptId: mapped.promptId, question: mapped.promptText, PROMPT_FAMILY: mapped.promptFamily },
    { prompt: mapped }
  );
  assert.equal(row.promptOrigin, "SCENARIO");
  assert.equal(row.showOriginBadge, true);
  assert.equal(row.originBadge, "Scenario");
});

test("4-provider architecture unchanged", () => {
  assert.deepEqual([...KNOWN_AI_VISIBILITY_PROVIDER_IDS], [
    "openai",
    "gemini",
    "perplexity",
    "claude",
  ]);
});

console.log(`\nUniverse TOTAL=${audit.TOTAL_ACTIVE_PROMPTS} OBSERVED=${audit.OBSERVED} DERIVED=${audit.DERIVED} SCENARIO=${audit.SCENARIO} LEGACY=${audit.LEGACY_UNCLASSIFIED}`);
console.log(
  `Cost existing=${cost.EXISTING_PROMPTS} increment=${cost.TOTAL_INCREMENT} calls/run=${cost.CALLS_PER_FULL_RUN} usd/run=${cost.COST_PER_FULL_RUN.toFixed(2)}`
);
console.log(`\nPrompt provenance tests: ${passed} passed, ${failed} failed\n`);

if (failed) process.exit(1);
process.stdout.write(
  JSON.stringify(
    {
      audit,
      cost,
      seedStatus: seed.seedStatus,
    },
    null,
    2
  ) + "\n"
);
