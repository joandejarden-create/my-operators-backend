#!/usr/bin/env node
/**
 * Operator Presence Validation V1 tests — no live provider calls.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  preflightOperatorPresenceValidation,
  buildOperatorExecutionSlots,
  buildLiveValidationCases,
  analyzeOperatorPresenceCorpus,
  createOperatorPresenceWaveId,
} from "../lib/ai-visibility/operator-intelligence/presence-validation-wave.js";
import {
  classifyOperatorPresence,
  OPERATOR_AI_UNIVERSE,
  PRIMARY_OPERATOR_COUNT,
} from "../lib/ai-visibility/operator-intelligence/index.js";
import { costOperatorFoundationWave } from "../lib/ai-visibility/operator-intelligence/cost-model.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

function id(name) {
  return OPERATOR_AI_UNIVERSE.find((o) => o.founderName === name).canonicalId;
}

console.log("\nOperator Presence Validation V1\n");

await test("preflight matrix = 84 with expected provider split", () => {
  const pf = preflightOperatorPresenceValidation({ requireEnv: false });
  assert.equal(pf.plannedCalls, 84);
  assert.equal(pf.byProvider.openai, 30);
  assert.equal(pf.byProvider.gemini, 12);
  assert.equal(pf.byProvider.perplexity, 30);
  assert.equal(pf.byProvider.claude, 12);
  assert.equal(pf.matrixOk, true);
});

await test("monitored operator count = 9", () => {
  const pf = preflightOperatorPresenceValidation({ requireEnv: false });
  assert.equal(pf.primaryOperatorCount, 9);
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
});

await test("cost cap gate passes", () => {
  const cost = costOperatorFoundationWave();
  assert.ok(cost.projectedConservativeCost <= 60);
  assert.equal(cost.totalCalls, 84);
  assert.equal(cost.marginalCostAddOperator, 0);
});

await test("Remington monitored not unmonitored competitor", () => {
  const r = classifyOperatorPresence({
    text: "Remington Hospitality is a third-party operator owners consider in CALA.",
  });
  assert.ok(r.presentOperatorIds.includes(id("Remington CALA")));
  assert.equal(r.observedCompetitors.some((c) => c.canonicalEntityId === id("Remington CALA")), false);
});

await test("bare Remington negative", () => {
  const r = classifyOperatorPresence({
    text: "Remington is a well-known firearms brand.",
  });
  assert.equal(r.presentOperatorIds.length, 0);
});

await test("Remington source-only negative", () => {
  const r = classifyOperatorPresence({
    text: "See https://www.remingtonhospitality.com/ for info.",
    citations: [{ domain: "remingtonhospitality.com" }],
  });
  assert.equal(r.presentOperatorIds.includes(id("Remington CALA")), false);
});

await test("Marriott brand-only negative", () => {
  const r = classifyOperatorPresence({
    text: "Guests earn Marriott Bonvoy points at this property.",
  });
  assert.equal(r.presentOperatorIds.includes(id("Marriott International")), false);
});

await test("Hilton brand-only negative", () => {
  const r = classifyOperatorPresence({
    text: "Stay at a Hilton Garden Inn and earn Hilton Honors.",
  });
  assert.equal(r.presentOperatorIds.includes(id("Hilton")), false);
});

await test("IHG brand-only negative", () => {
  const r = classifyOperatorPresence({
    text: "Book an IHG hotel and collect IHG One Rewards points.",
  });
  assert.equal(r.presentOperatorIds.includes(id("IHG")), false);
});

await test("multi-operator extraction", () => {
  const analysis = analyzeOperatorPresenceCorpus([
    {
      responseId: "r1",
      provider: "openai",
      promptId: "op_p_core_third_party_cala_en_v1",
      scenarioId: "op_scenario_third_party_management_v1",
      rawText:
        "Owners often consider Aimbridge Hospitality and Hotel Equities as third-party operators in Latin America.",
      citations: [],
    },
  ]);
  assert.ok(analysis.summary.responsesWithAnyMonitoredOperator >= 1);
  assert.ok(analysis.summary.totalOperatorPresenceSpans >= 2);
});

await test("live validation case builder produces cases", () => {
  const analysis = analyzeOperatorPresenceCorpus([
    {
      responseId: "r2",
      provider: "perplexity",
      promptId: "op_p_core_third_party_cala_en_v1",
      scenarioId: "op_scenario_third_party_management_v1",
      rawText: "GHL Hoteles is a management company in Colombia.",
      citations: [],
    },
  ]);
  const cases = buildLiveValidationCases(
    analysis.extractions.map((e) => ({
      ...e,
      rawText: "GHL Hoteles is a management company in Colombia.",
      citations: [],
    }))
  );
  assert.ok(cases.length >= PRIMARY_OPERATOR_COUNT);
});

await test("wave id format", () => {
  const w = createOperatorPresenceWaveId();
  assert.match(w, /^aiv_operator_presence_validation_/);
});

await test("execution slots count", () => {
  assert.equal(buildOperatorExecutionSlots().length, 84);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed > 0 ? 1 : 0);
