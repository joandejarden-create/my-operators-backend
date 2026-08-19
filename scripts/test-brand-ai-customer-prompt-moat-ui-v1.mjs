#!/usr/bin/env node
/**
 * Brand AI Customer Prompt Moat UI Remediation V1
 * PROVIDER_CALLS = 0
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { buildOwnerIntentBenchmarksForBrand } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-customer-service.js";
import { verifyFrozenBaseline } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-longitudinal-recertification.js";
import {
  auditPayloadForCanonicalPromptLeaks,
  buildCustomerSafeObservationContext,
  redactAiVsDealalityRow,
  redactCustomerEvidence,
  redactCustomerQuestionsPayload,
  redactCustomerWatchlistRow,
  resolveCustomerDecisionContext,
  loadForbiddenCanonicalPromptStrings,
  isInternalPromptAccess,
  CUSTOMER_DECISION_CONTEXT,
} from "../lib/ai-visibility/customer-prompt-disclosure.js";
import { getCustomerScenarioDisplayLabel } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-customer-service.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BRAND_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
  "utf8"
);
const BRAND_HTML = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

console.log("\nBrand AI Customer Prompt Moat UI V1\n");

const forbidden = loadForbiddenCanonicalPromptStrings();
assert.ok(forbidden.length >= 5, "expected forbidden prompt fixture strings");

await test("decision context map covers core scenarios", () => {
  assert.ok(CUSTOMER_DECISION_CONTEXT[S.SOFT_BRAND]);
  assert.ok(CUSTOMER_DECISION_CONTEXT[S.BRANDED_RESIDENCES]);
  assert.match(resolveCustomerDecisionContext(S.LIFESTYLE), /individuality/i);
});

await test("customer-safe watchlist row redacts QUESTION", () => {
  const canonical = forbidden[0];
  const row = redactCustomerWatchlistRow(
    {
      QUESTION: canonical,
      question: canonical,
      promptText: canonical,
      PROMPT_FAMILY: "Soft Brand Affiliation",
      promptId: "prompt_test_1",
      REGION: "CALA",
      LANGUAGE: "en",
    },
    { viewerContext: {} }
  );
  assert.equal(row.QUESTION, undefined);
  assert.equal(row.question, undefined);
  assert.equal(row.promptText, undefined);
  assert.ok(row.ownerIntent);
  assert.ok(row.decisionContext);
  assert.equal(row.geography, "Caribbean & Latin America");
  const audit = auditPayloadForCanonicalPromptLeaks(row, { forbiddenStrings: forbidden });
  assert.equal(audit.leakCount, 0, audit.leaks.join(" | "));
});

await test("ai vs dealality row redacts raw question", () => {
  const canonical = forbidden[1] || forbidden[0];
  const row = redactAiVsDealalityRow(
    {
      question: canonical,
      promptText: canonical,
      promptId: "prompt_test_2",
      intentTerritory: "Soft Brand Affiliation",
      aiPattern: "Autograph Collection",
      dealalityContext: "Upper Upscale · Soft Collection",
    },
    { viewerContext: {} }
  );
  assert.equal(row.question, undefined);
  assert.equal(row.promptText, undefined);
  assert.ok(row.ownerIntent);
  assert.ok(row.decisionContext);
  assert.equal(row.aiRepresentation, "Autograph Collection");
});

await test("customer evidence redacts promptText", () => {
  const canonical = forbidden[0];
  const ev = redactCustomerEvidence(
    {
      promptId: "prompt_test_3",
      promptText: canonical,
      intentTerritory: "Conversion Suitability",
      commercialRegion: "CALA",
      language: "en",
      drilldownTrace: { promptText: canonical },
    },
    { viewerContext: {} }
  );
  assert.equal(ev.promptText, undefined);
  assert.equal(ev.drilldownTrace.promptText, undefined);
  assert.ok(ev.ownerIntent);
  assert.ok(ev.decisionContext);
});

await test("internal admin preserves prompt access", () => {
  const canonical = forbidden[0];
  const row = redactCustomerWatchlistRow(
    { QUESTION: canonical, promptText: canonical },
    { viewerContext: { internalAdmin: true } }
  );
  assert.equal(row.QUESTION, canonical);
  assert.ok(isInternalPromptAccess({ internalAdmin: true }));
});

await test("questions payload API leak audit", () => {
  const canonical = forbidden[0];
  const payload = redactCustomerQuestionsPayload(
    {
      questions: [{ question: canonical, promptText: canonical, promptId: "p1" }],
      questionsMissingWatchlist: {
        rows: [{ QUESTION: canonical, PROMPT_FAMILY: "Branded Residences", REGION: "CALA" }],
      },
    },
    { viewerContext: {} }
  );
  const audit = auditPayloadForCanonicalPromptLeaks(payload, { forbiddenStrings: forbidden });
  assert.equal(audit.leakCount, 0, audit.leaks.join(" | "));
});

await test("certified benchmark freeze unchanged", () => {
  verifyFrozenBaseline();
  const all = buildOwnerIntentBenchmarksForBrand(IDS.AUTOGRAPH, { allProvidersMode: true });
  const soft = all.ownerIntentBenchmarks.find((r) => r.intentLabel === "Soft Brand Affiliation");
  assert.equal(soft?.indexValue, 103);
  const ascend = buildOwnerIntentBenchmarksForBrand(IDS.ASCEND, { allProvidersMode: true });
  const ascendRow = ascend.ownerIntentBenchmarks.find((r) => r.scenarioId === S.SOFT_BRAND);
  assert.equal(ascendRow?.indexValue, 67);
});

await test("frontend DOM contract — no raw Question column in watchlist", () => {
  assert.match(BRAND_HTML, /Owner Intent/);
  assert.match(BRAND_HTML, /Decision Context/);
  assert.doesNotMatch(BRAND_HTML, /<th[^>]*>Question<\/th>/);
  assert.doesNotMatch(BRAND_JS, /r\.QUESTION[^S_]/);
  assert.match(BRAND_JS, /watchlistOwnerIntent/);
  assert.match(BRAND_JS, /decisionContext/);
  assert.doesNotMatch(BRAND_JS, /ev\.promptText/);
  assert.match(BRAND_HTML, /About Owner Intent/);
  assert.match(BRAND_HTML, /About Decision Context/);
  assert.match(BRAND_HTML, /How Dealality Measures AI/);
  assert.match(BRAND_JS, /BENCHMARK_STILL_DEVELOPING/);
  assert.match(BRAND_JS, /disclosureInfoIconHtml/);
});

await test("proper-case owner intent labels", () => {
  const soft = getCustomerScenarioDisplayLabel(S.SOFT_BRAND);
  assert.equal(soft, "Soft Brand Affiliation");
  assert.doesNotMatch(soft, /_/);
  const ctx = buildCustomerSafeObservationContext({
    PROMPT_FAMILY: "SOFT_BRAND_AFFILIATION",
    promptId: null,
  });
  assert.doesNotMatch(ctx.ownerIntent, /SOFT_BRAND/);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");

if (failed === 0) {
  console.log("BRAND_AI_CUSTOMER_PROMPT_MOAT_UI_PASS");
  process.exit(0);
}
console.log("BRAND_AI_CUSTOMER_PROMPT_MOAT_UI_REMEDIATION_REQUIRED");
process.exit(1);
