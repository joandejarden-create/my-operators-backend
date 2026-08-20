#!/usr/bin/env node
/**
 *   npm run test:adp-governed-presence-index-activation-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { FINAL_NEW_SCENARIOS } from "../lib/ai-demand-positioning/prompt-universe/scenario-expansion-catalog-v1.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import { buildOwnerPayload, computeIntentPresenceIndexLegacy } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { presenceIndexFromRates } from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import {
  assertNumericIndexHasRates,
  GOVERNED_INDEX_FORMULA,
  SCORE_CAP,
} from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { estimateCost } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";

function countByIntent(scenarios) {
  const out = {};
  for (const s of scenarios) out[s.intent] = (out[s.intent] || 0) + 1;
  return out;
}

function main() {
  const waterstone = loadPropertyProfile("adp_waterstone_boca_raton");
  const scenarios = buildScenarioUniverse(waterstone);
  const byIntent = countByIntent(scenarios);
  assert.equal(scenarios.length, 78);
  assert.equal(FINAL_NEW_SCENARIOS.length, 13);
  assert.equal(byIntent[TRAVELER_INTENTS.BUSINESS], 12);
  assert.equal(byIntent[TRAVELER_INTENTS.LEISURE], 12);
  assert.equal(byIntent[TRAVELER_INTENTS.COUPLES], 9);
  assert.equal(byIntent[TRAVELER_INTENTS.FAMILY], 9);
  assert.equal(byIntent[TRAVELER_INTENTS.GROUP_MEETING], 13);
  assert.equal(byIntent[TRAVELER_INTENTS.WELLNESS], 8);
  assert.equal(byIntent[TRAVELER_INTENTS.ADVENTURE], 7);
  assert.equal(byIntent[TRAVELER_INTENTS.CELEBRATION], 8);

  const cost = estimateCost(78);
  assert.equal(cost.providerCount * cost.scenarioCount, 312);

  const parity = presenceIndexFromRates(0.5, 0.5);
  assert.equal(parity.index, 100);
  const above = presenceIndexFromRates(0.6, 0.5);
  assert.equal(above.index, 120);
  const below = presenceIndexFromRates(0.4, 0.5);
  assert.equal(below.index, 80);
  const uncapped = presenceIndexFromRates(0.62, 0.15);
  assert.ok(uncapped.index > 200);
  assert.equal(SCORE_CAP, "NONE");
  assert.ok(GOVERNED_INDEX_FORMULA.includes("SUBJECT_AI_PRESENCE_RATE"));

  const periods = loadAllPeriods("adp_waterstone_boca_raton");
  const period = [...periods].reverse().find((p) => (p.observations || []).some((o) => o.parsed));
  const owner = buildOwnerPayload(period, scenarios, waterstone, { allPeriods: periods });
  assert.ok(!owner.aiConsiderationIndex);
  assert.ok(!owner.aci);
  assert.ok(owner.intentPresenceIndex);
  assert.ok(typeof computeIntentPresenceIndexLegacy === "function");

  let mixed = 0;
  let numericWithoutRates = 0;
  for (const row of Object.values(owner.intentPresenceIndex)) {
    assert.equal(row.customerRender, "governed");
    assert.notEqual(row.methodology, "legacy_declared_comp");
    const gate = assertNumericIndexHasRates(row);
    if (!gate.ok) numericWithoutRates += 1;
    if (row.index != null && row.avgCompRate >= 30 && row.scoreCap === 200) mixed += 1;
  }
  assert.equal(numericWithoutRates, 0);
  assert.equal(mixed, 0);

  const ui = readFileSync(join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js"), "utf8");
  assert.ok(ui.includes("adpGovernedIndexTip"));
  assert.ok(ui.includes("60%"));
  assert.ok(ui.includes("50%"));
  assert.ok(ui.includes("120"));
  assert.ok(ui.includes("Your AI Presence"));
  assert.ok(ui.includes("CORE Benchmark"));
  assert.ok(!ui.includes("Capped at 200"));
  assert.equal(ui.includes("aiConsiderationIndex"), false);

  console.log("test:adp-governed-presence-index-activation-v2 — registry PASS");
}

async function cambridgeCheck() {
  const cambridge = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridge.ok);
  assert.equal(cambridge.payload.demandCapture.overallRate, 100);
  for (const row of Object.values(cambridge.payload.intentPresenceIndex || {})) {
    assert.equal(row.index, null);
    assert.equal(row.developing, true);
  }
  console.log("test:adp-governed-presence-index-activation-v2 — Cambridge PASS");
}

main();
cambridgeCheck().catch((err) => {
  console.error(err);
  process.exit(1);
});
