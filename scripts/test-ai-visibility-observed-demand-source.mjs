#!/usr/bin/env node
/**
 * Observed-demand source acquisition tests. No live paid API. No AI monitoring.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  extractPeopleAlsoAsk,
  extractRelatedSearches,
  assignRelativeDemandTiers,
  usableAsObserved,
  estimateObservedDemandSampleCost,
  evaluateDataForSeoBudgetGuard,
  resolveObservedDemandCostCapUsd,
  SOURCE_CANDIDATE_EVALUATION,
  SEED_CONCEPTS_EN,
  SEED_CONCEPTS_ES,
  DATAFORSEO_BUDGET_APPROVAL_REQUIRED,
  MAX_SOURCE_SAMPLE_COST_USD,
  MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD,
  VALIDATED_OBSERVED_THEMES_V1,
  REFINEMENT_SEEDS_EN,
  REFINEMENT_SEEDS_ES,
  estimateObservedDemandRefinementCost,
  canonicalObservedTheme,
  classifyCommercialRelevance,
} from "../lib/ai-visibility/observed-demand-source-sample.js";
import {
  CLIENT_PROMPT_ORIGIN_COPY,
  OBSERVED_PROMPT_MIX_MIN_THEMES,
  shouldShowExecutivePromptMix,
  loadObservedDemandSeed,
} from "../lib/ai-visibility/prompt-provenance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

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

console.log("\nObserved demand source acquisition tests\n");

test("PAA extract ignores organic rows", () => {
  const paa = extractPeopleAlsoAsk([
    { type: "organic", title: "not a question" },
    {
      type: "people_also_ask",
      items: [{ title: "What is a hotel franchise?", rank_absolute: 1 }],
    },
  ]);
  assert.equal(paa.length, 1);
  assert.equal(paa[0].question, "What is a hotel franchise?");
});

test("related searches extract", () => {
  const rel = extractRelatedSearches([
    { type: "related_searches", items: [{ title: "hotel reflagging cost" }] },
  ]);
  assert.equal(rel[0].query, "hotel reflagging cost");
});

test("relative demand tiers stay inside one set", () => {
  const rows = assignRelativeDemandTiers([
    { search_volume: 90 },
    { search_volume: 40 },
    { search_volume: 10 },
    { search_volume: null },
  ]);
  assert.equal(rows[0].demandTier, "HIGH");
  assert.equal(rows[1].demandTier, "MEDIUM");
  assert.equal(rows[2].demandTier, "LOW");
  assert.equal(rows[3].demandTier, "UNKNOWN");
  assert.equal(rows[0].demandTierBasis, "LICENSED_SEARCH_VOLUME");
});

test("zero volume without PAA is not observed", () => {
  const u = usableAsObserved({ search_volume: 0, paaQuestions: [], relatedSearches: [] });
  assert.equal(u.yes, false);
});

test("positive licensed volume is observed", () => {
  assert.equal(usableAsObserved({ search_volume: 70 }).yes, true);
});

test("sample cost estimate under cap", () => {
  const e = estimateObservedDemandSampleCost();
  assert.ok(e.SAMPLE_COST_USD < e.COST_CAP_USD);
  assert.equal(e.COST_CAP_USD, 1);
  assert.equal(e.PHASE_CAP_USD, 2);
  assert.equal(e.DATAFORSEO_BUDGET_APPROVAL_REQUIRED, false);
  assert.equal(e.ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET, true);
  assert.equal(e.AI_PROVIDER_CALLS, 0);
});

test("budget guard blocks over-cap projection", () => {
  const overSample = evaluateDataForSeoBudgetGuard({ projectedSampleUsd: 1.5, phaseSpentUsd: 0 });
  assert.equal(overSample.allowed, false);
  assert.equal(overSample.code, DATAFORSEO_BUDGET_APPROVAL_REQUIRED);
  const overPhase = evaluateDataForSeoBudgetGuard({
    projectedSampleUsd: 0.9,
    phaseSpentUsd: 1.2,
  });
  assert.equal(overPhase.allowed, false);
  const ok = evaluateDataForSeoBudgetGuard({ projectedSampleUsd: 0.282, phaseSpentUsd: 0 });
  assert.equal(ok.allowed, true);
  assert.equal(MAX_SOURCE_SAMPLE_COST_USD, 1);
  assert.equal(MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD, 2);
  assert.equal(resolveObservedDemandCostCapUsd(50), 1);
  assert.equal(resolveObservedDemandCostCapUsd(0.5), 0.5);
});

test("seed concepts stay candidates until evidence", () => {
  assert.ok(SEED_CONCEPTS_EN.length >= 10 && SEED_CONCEPTS_EN.length <= 20);
  assert.ok(SEED_CONCEPTS_ES.length >= 4);
});

test("source evaluation has no composite score field", () => {
  for (const row of SOURCE_CANDIDATE_EVALUATION) {
    assert.equal(row.score, undefined);
    assert.ok(["STRONG", "GOOD", "LIMITED", "WEAK", "UNKNOWN"].includes(row.COST));
  }
});

test("prompt mix requires 10 observed themes", () => {
  assert.equal(OBSERVED_PROMPT_MIX_MIN_THEMES, 10);
  assert.equal(shouldShowExecutivePromptMix({ observed: 9 }), false);
  assert.equal(shouldShowExecutivePromptMix({ observed: 10 }), true);
});

test("current copy is scenario-led not observed-active", () => {
  assert.match(CLIENT_PROMPT_ORIGIN_COPY.lenses, /scenario-led/);
  assert.doesNotMatch(CLIENT_PROMPT_ORIGIN_COPY.lenses, /We use both observed demand/);
  assert.match(CLIENT_PROMPT_ORIGIN_COPY.lensesAfterActivation, /both observed demand/);
  assert.doesNotMatch(CLIENT_PROMPT_ORIGIN_COPY.methodology, /real owner searches/);
});

test("seed is partial after budget-capped sample", () => {
  const seed = loadObservedDemandSeed();
  assert.equal(seed.seedStatus, "OBSERVED_DEMAND_SEED_PARTIAL");
  assert.equal(seed.blockerCode, null);
  assert.equal(seed.activationStatus, "NOT_ATTACHED_TO_LIVE_PROMPTS");
  assert.equal(seed.promptMixEligible, false);
  assert.equal((seed.includedThemes || []).length, 9);
  assert.ok((seed.includedThemes || []).length < OBSERVED_PROMPT_MIX_MIN_THEMES);
});

test("sample report records budget-capped success", () => {
  const p = path.join(root, "reports", "ai-visibility", "observed-demand-source-sample-2026-08-17.json");
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.equal(raw.AI_PROVIDER_CALLS, 0);
  assert.equal(raw.valid_signals, 10);
  assert.equal(raw.ACCOUNT_FUNDING_IS_NOT_PROJECT_BUDGET, true);
  assert.ok(raw.actual_cost_usd <= MAX_SOURCE_SAMPLE_COST_USD);
  assert.ok(raw.phase_spent_after_usd <= MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD);
  assert.equal(raw.blockerCode, null);
  const statuses = Object.values(raw.volume_status || {});
  assert.ok(statuses.every((s) => s.http_status === 200));
  assert.ok(!statuses.some((s) => s.http_status === 402));
});

test("refinement report stays under phase cap and does not attach overlay", () => {
  const p = path.join(root, "reports", "ai-visibility", "observed-demand-refinement-2026-08-17.json");
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.equal(raw.AI_PROVIDER_CALLS, 0);
  assert.equal(raw.AIRTABLE_WRITES, 0);
  assert.equal(raw.BROAD_KEYWORD_DISCOVERY, 0);
  assert.equal(raw.actual_cost_usd, 0.192);
  assert.equal(raw.TOTAL_PHASE_SPEND, 0.474);
  assert.ok(raw.TOTAL_PHASE_SPEND <= MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD);
  assert.equal(raw.gate.MIN_10_DISTINCT_THEMES, "FAIL");
  assert.equal(raw.FINAL, "OBSERVED_DEMAND_REFINEMENT_PARTIAL");
  assert.equal(raw.overlay_classifications, 0);
});

test("refinement seeds skip validated themes and stay under preferred cap", () => {
  const blocked = new Set(VALIDATED_OBSERVED_THEMES_V1.map((t) => t.toLowerCase()));
  for (const row of [...REFINEMENT_SEEDS_EN, ...REFINEMENT_SEEDS_ES]) {
    assert.equal(blocked.has(row.seed.toLowerCase()), false, row.seed);
  }
  const e = estimateObservedDemandRefinementCost({ phaseSpentUsd: 0.282 });
  assert.equal(e.VOLUME_TASKS, 2);
  assert.equal(e.SERP_TASKS, 6);
  assert.ok(e.SAMPLE_COST_USD <= 0.75);
  assert.ok(e.budget.projectedPhaseUsd <= 2);
  assert.equal(e.DATAFORSEO_BUDGET_APPROVAL_REQUIRED, false);
  assert.equal(canonicalObservedTheme("coste franquicia hotelera"), "hotel franchise fees");
  assert.equal(canonicalObservedTheme("hotel franchise royalties"), "hotel franchise royalties");
  const noisy = classifyCommercialRelevance({
    queryText: "independent hotel franchise",
    paaQuestions: [
      "Which hotel franchise is the most profitable?",
      "What franchise can I open with $10,000?",
      "What is the cheapest hotel franchise to buy?",
    ],
  });
  assert.equal(noisy.usable, false);
  const residences = classifyCommercialRelevance({
    queryText: "hotel branded residences",
    paaQuestions: [],
    search_volume: 10,
  });
  assert.equal(residences.usable, true);
  const feesKept = classifyCommercialRelevance({
    queryText: "hotel franchise fees",
    paaQuestions: ["Which hotel franchise is the most profitable?"],
    search_volume: 90,
  });
  assert.equal(feesKept.usable, true);
});

test("client JS mix gate uses 10 observed", () => {
  const js = fs.readFileSync(
    path.join(root, "public", "js", "ai-visibility", "ai-visibility-brand.js"),
    "utf8"
  );
  assert.ok(js.includes("(observed || 0) >= 10"));
});

console.log(`\nObserved demand source tests: ${passed} passed, ${failed} failed\n`);
if (failed) process.exit(1);
