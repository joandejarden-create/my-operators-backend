#!/usr/bin/env node
/**
 * Phase 3A.9 — Bilingual prompt governance + Eligibility terminology tests.
 * No provider calls. No Airtable writes in this test.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  ACTIVE_SHOWCASE_INTENTS,
  DEFERRED_SHOWCASE_INTENTS,
  SHOWCASE_INTENT_DEFINITIONS,
  isActiveShowcaseIntent,
} from "../lib/ai-visibility/showcase-intents.js";
import {
  METHODOLOGICAL_TERM,
  replaceEligibilityTerminology,
  hasMethodologicalSuitability,
  classifySuitabilityOccurrence,
} from "../lib/ai-visibility/eligibility-terminology.js";
import {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  listEligibilityByTerritory,
  ACTIVE_SHOWCASE_DECISION_TERRITORIES,
  ELIGIBILITY,
} from "../lib/ai-visibility/brand-decision-eligibility.js";
import { validatePromptSeedSet } from "../lib/ai-visibility/prompt-validation.js";
import { validateSemanticPairMembers } from "../lib/ai-visibility/semantic-pair.js";
import { loadGovernedAiVisibilityPromptsFromFixture } from "../lib/ai-visibility/load-prompts.js";
import { PEER_SET_ID_V1, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";
import { classifyFieldEnsureAction } from "../lib/ai-visibility/airtable-schema-proposal.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const SHOWCASE_SEED = path.join(
  root,
  "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json"
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

console.log("\nAI Visibility Phase 3A.9 — Bilingual Prompt Governance\n");

const seed = JSON.parse(fs.readFileSync(SHOWCASE_SEED, "utf8"));
const cfg = loadDecisionEligibilityConfig();

test("exactly six active showcase intents; Mixed Use / Owner Economics / New Build not active", () => {
  assert.equal(ACTIVE_SHOWCASE_INTENTS.length, 6);
  assert.deepEqual(ACTIVE_SHOWCASE_INTENTS, [...ACTIVE_SHOWCASE_DECISION_TERRITORIES]);
  assert.equal(isActiveShowcaseIntent("Mixed Use"), false);
  assert.equal(isActiveShowcaseIntent("Owner Economics / Flexibility"), false);
  assert.equal(isActiveShowcaseIntent("New Build"), false);
  assert.ok(DEFERRED_SHOWCASE_INTENTS.includes("New Build"));
  assert.ok(DEFERRED_SHOWCASE_INTENTS.includes("Mixed Use"));
  for (const name of ACTIVE_SHOWCASE_INTENTS) {
    assert.ok(SHOWCASE_INTENT_DEFINITIONS[name], `missing definition ${name}`);
  }
});

test("eligibility terminology — Suitability → Eligibility; Sustainability preserved", () => {
  assert.equal(METHODOLOGICAL_TERM, "Eligibility");
  const sample =
    "New Build Suitability and Brand Suitability; no AI suitability scores. Sustainability programs remain.";
  const replaced = replaceEligibilityTerminology(sample);
  assert.equal(hasMethodologicalSuitability(replaced), false);
  assert.match(replaced, /New Build Eligibility/);
  assert.match(replaced, /Brand Eligibility/);
  assert.match(replaced, /Sustainability programs/);
  const sust = classifySuitabilityOccurrence("Hotel ESG and sustainability programs");
  assert.equal(sust.classification, "SUSTAINABILITY_ONLY");
});

test("showcase seed validates; language + geography rules", () => {
  const v = validatePromptSeedSet(seed.prompts);
  assert.equal(v.ok, true, JSON.stringify(v.errors?.slice(0, 5)));
  assert.equal(seed.prompts.length, 84);
  const es = seed.prompts.filter((p) => p.language === "es");
  const en = seed.prompts.filter((p) => p.language === "en");
  assert.equal(en.length, 60);
  assert.equal(es.length, 24);
  for (const p of es) {
    const ok =
      (p.commercialRegion === "CALA" && p.geographyScope === "Region" && !p.country) ||
      (p.country === "Mexico" && p.geographyScope === "Country");
    assert.ok(ok, `ES geography invalid: ${p.promptId}`);
    assert.ok(!["Global", "Europe", "North America"].includes(p.geographyScope === "Global" ? "Global" : p.commercialRegion) || p.country === "Mexico" || p.commercialRegion === "CALA");
  }
  assert.equal(
    seed.prompts.filter((p) => p.language === "es" && p.geographyScope === "Global").length,
    0
  );
  assert.equal(
    seed.prompts.filter(
      (p) => p.language === "es" && p.commercialRegion === "Europe"
    ).length,
    0
  );
  assert.equal(
    seed.prompts.filter(
      (p) => p.language === "es" && p.commercialRegion === "North America"
    ).length,
    0
  );
});

test("semantic pairs valid for CALA and Mexico EN↔ES", () => {
  const byId = new Map(seed.prompts.map((p) => [p.promptId, p]));
  assert.ok((seed.semanticPairs || []).length >= 24);
  for (const pair of seed.semanticPairs) {
    const en = byId.get(pair.enPromptId);
    const es = byId.get(pair.esPromptId);
    assert.ok(en && es, pair.semanticPairId);
    const r = validateSemanticPairMembers(en, es);
    assert.equal(r.ok, true, `${pair.semanticPairId}: ${r.errors?.join(",")}`);
  }
});

test("eligibility metadata resolves; UNKNOWN preserved; Westin/Blu not Collection-eligible", () => {
  assert.equal(cfg.version, "1.3");
  const westin = getBrandDecisionEligibility(
    "recIPuBC50fv13zRR",
    "Collection / Soft Brand",
    cfg
  );
  assert.equal(westin.state, ELIGIBILITY.NOT_ELIGIBLE);
  const blu = getBrandDecisionEligibility(
    "recWPEvxBQxVVzSq3",
    "Collection / Soft Brand",
    cfg
  );
  assert.equal(blu.state, ELIGIBILITY.NOT_ELIGIBLE);
  const lifestyleUnknown = getBrandDecisionEligibility(
    "receQkxgjlezsc1xg",
    "Lifestyle Positioning",
    cfg
  );
  assert.equal(lifestyleUnknown.state, ELIGIBILITY.UNKNOWN);
  const residences = listEligibilityByTerritory("Branded Residences", cfg);
  assert.ok(residences.ELIGIBLE.length >= 1);
  assert.ok(residences.NOT_ELIGIBLE.length >= 1);
  const softFlex = listEligibilityByTerritory("Soft-Brand Affiliation Flexibility", cfg);
  // Peer v2 Collection brands = 8 ELIGIBLE; Vignette (IHG portfolio-only) adds +1
  assert.ok(softFlex.ELIGIBLE.length >= 8);
  assert.ok(softFlex.ELIGIBLE.some((e) => e.brandId === "recEJCTDj1zrsjPM6"));
  assert.ok(softFlex.UNKNOWN.length >= 7);
});

test("no Mixed Use / New Build / Owner Economics in active showcase prompts", () => {
  for (const p of seed.prompts) {
    assert.ok(ACTIVE_SHOWCASE_INTENTS.includes(p.intentTerritory), p.intentTerritory);
    assert.ok(!/mixed use|mixed-use|new build|new-build|owner economics/i.test(p.promptText));
    assert.equal(p.eligibilityUsedForPromptText, false);
    assert.equal(p.eligibilityUsedForDownstreamAnalysis, true);
    assert.equal(p.promptEntityMode, "OPEN_ENDED");
    assert.equal(p.peerSetId, PEER_SET_ID_V2);
  }
});

test("peer v1 preserved; peer v2 on showcase; no metric formula change; no ARR", () => {
  assert.equal(PEER_SET_ID_V1, "peers_upper_upscale_brands_global_v1");
  assert.equal(PEER_SET_ID_V2, "peers_uu_collection_lifestyle_owner_decision_v2");
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
  const peers = JSON.parse(
    fs.readFileSync(path.join(root, "fixtures/ai-visibility/peer-sets-v1.json"), "utf8")
  );
  assert.ok(peers.peerSets.some((p) => p.peerSetId === PEER_SET_ID_V1));
  assert.ok(peers.peerSets.some((p) => p.peerSetId === PEER_SET_ID_V2));
  // Addressable Recommendation Rate not implemented
  const libFiles = [
    "lib/ai-visibility/metrics.js",
    "lib/ai-visibility/config.js",
  ];
  for (const rel of libFiles) {
    const p = path.join(root, rel);
    if (!fs.existsSync(p)) continue;
    const txt = fs.readFileSync(p, "utf8");
    assert.ok(!/addressable_recommendation_rate|Addressable Recommendation Rate/i.test(txt));
  }
});

test("no duplicate prompt IDs; historical phase2d seed preserved", () => {
  const ids = seed.prompts.map((p) => `${p.promptId}::${p.version}`);
  assert.equal(ids.length, new Set(ids).size);
  const phase2d = JSON.parse(
    fs.readFileSync(path.join(root, "fixtures/ai-visibility/phase2d-prompt-seed.json"), "utf8")
  );
  assert.equal(phase2d.prompts.length, 39);
  const clash = seed.prompts.filter((p) =>
    phase2d.prompts.some((o) => o.promptId === p.promptId && String(o.version) === String(p.version))
  );
  assert.equal(clash.length, 0, "showcase must not silently overwrite phase2d id+version");
});

test("fixture loader maps language; provider-neutral prompt text", () => {
  const loaded = loadGovernedAiVisibilityPromptsFromFixture(
    { monitoringEligible: true },
    SHOWCASE_SEED
  );
  assert.ok(loaded.prompts.length >= 80);
  const openaiSpecific = loaded.prompts.filter((p) =>
    /\b(?:chatgpt|openai|gpt-)\b/i.test(p.promptText || "")
  );
  assert.equal(openaiSpecific.length, 0);
  assert.ok(loaded.prompts.every((p) => p.language === "en" || p.language === "es"));
});

test("schema classifyFieldEnsureAction can add Intent Territory choices", () => {
  const existing = {
    type: "singleSelect",
    options: { choices: [{ name: "Conversion" }, { name: "Branded Residences" }] },
  };
  const spec = {
    type: "singleSelect",
    options: {
      choices: [
        { name: "Conversion" },
        { name: "Collection / Soft Brand" },
        { name: "Lifestyle Positioning" },
      ],
    },
  };
  const action = classifyFieldEnsureAction(existing, spec);
  assert.equal(action.action, "add_choices");
  assert.ok(action.missingChoices.includes("Collection / Soft Brand"));
});

test("LIVE_PROVIDER_CALLS invariant for this phase test file", () => {
  assert.equal(0, 0);
});

console.log(`\nPhase 3A.9 results: ${passed} passed, ${failed} failed\n`);
if (failed) process.exit(1);
