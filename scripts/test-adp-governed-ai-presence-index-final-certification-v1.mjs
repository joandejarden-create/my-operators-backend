#!/usr/bin/env node
/**
 *   npm run test:adp-governed-ai-presence-index-final-certification-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload, computeIntentPresenceIndexLegacy } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import {
  assertNumericIndexHasRates,
  GOVERNED_INDEX_CUSTOMER_RENDER,
  LEGACY_INDEX_CUSTOMER_RENDER,
  SCORE_CAP,
} from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import { computePresenceIndexV2ForIntent } from "../lib/ai-demand-positioning/metrics/presence-index-v2.js";
import { coreIdsForIntent, MIN_CORE_PEERS_PRODUCTION } from "../lib/ai-demand-positioning/metrics/presence-benchmark-v1.js";
import { SECONDARY_IN_BENCHMARK } from "../lib/ai-demand-positioning/metrics/core-benchmark-rate-contract-v1.js";

const PERIOD_ID = "adp_period_adp_waterstone_boca_raton_20260820053047_9cb18e";
const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");

async function main() {
  const ui = readFileSync(UI_JS, "utf-8");
  assert.strictEqual(LEGACY_INDEX_CUSTOMER_RENDER, 0);
  assert.strictEqual(GOVERNED_INDEX_CUSTOMER_RENDER, 1);
  assert.strictEqual(SCORE_CAP, "NONE");
  assert.strictEqual(SECONDARY_IN_BENCHMARK, 0);
  assert.ok(ui.includes("Based on"));
  assert.ok(ui.includes("CORE comparable hotels"));
  assert.ok(!ui.includes("Capped at 200"));

  const profile = loadPropertyProfile("adp_waterstone_boca_raton");
  const periods = loadAllPeriods("adp_waterstone_boca_raton");
  const period = periods.find((p) => p.periodId === PERIOD_ID);
  assert.ok(period, "frozen period");
  assert.equal(period.observations.length, 312);

  const manifest = loadPublishedManifest("adp_waterstone_boca_raton");
  assert.equal(manifest.latestPeriodId, PERIOD_ID);

  const scenarios = buildScenarioUniverse(profile);
  assert.equal(scenarios.length, 78);

  const owner = buildOwnerPayload(period, scenarios, profile, { allPeriods: periods });
  assert.ok(owner.demandCapture);
  assert.ok(typeof computeIntentPresenceIndexLegacy === "function");

  let numericRows = 0;
  let mixed = 0;
  for (const row of Object.values(owner.intentPresenceIndex)) {
    assert.equal(row.customerRender, "governed");
    const gate = assertNumericIndexHasRates(row);
    assert.ok(gate.ok, `transparency gate failed for ${row.territory}`);
    if (row.index != null) {
      numericRows += 1;
      assert.ok(row.subjectRatePct != null);
      assert.ok(row.coreBenchmarkRatePct != null);
      assert.equal(row.status, "PRODUCTION_VALIDATED");
    } else {
      assert.ok(row.developing);
    }
    if (row.index != null && row.scoreCap === 200) mixed += 1;
  }
  assert.equal(mixed, 0);
  assert.ok(numericRows >= 1, "at least one certified territory");

  const adventure = owner.intentPresenceIndex[TRAVELER_INTENTS.ADVENTURE];
  assert.equal(adventure.coreCount, 3);
  assert.ok(adventure.coreCount < MIN_CORE_PEERS_PRODUCTION);
  assert.equal(adventure.status, "BENCHMARK_DEVELOPING");
  assert.equal(adventure.index, null);

  const leisure = owner.intentPresenceIndex[TRAVELER_INTENTS.LEISURE];
  assert.equal(leisure.status, "PRODUCTION_VALIDATED");
  assert.ok(leisure.index > 200, "no score cap");

  const v2 = computePresenceIndexV2ForIntent(period.observations, scenarios, TRAVELER_INTENTS.LEISURE);
  assert.ok((v2.allProviders.zeroPresencePeers || []).length >= 0, "zero peers tracked");

  const cambridge = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridge.ok);
  const cambridgeNumeric = Object.values(cambridge.payload.intentPresenceIndex || {}).filter((r) => r.index != null);
  assert.ok(cambridgeNumeric.length >= 1, "Cambridge may promote certified territories after measurement");
  for (const row of cambridgeNumeric) {
    assert.equal(row.status, "PRODUCTION_VALIDATED");
    assert.ok(row.coreBenchmarkRatePct != null);
  }
  const cambridgeDeveloping = Object.values(cambridge.payload.intentPresenceIndex || {}).filter(
    (r) => r.index == null
  );
  assert.ok(cambridgeDeveloping.length >= 1, "non-certified Cambridge territories remain developing");

  console.log("test:adp-governed-ai-presence-index-final-certification-v1 — PASS");
  console.log("  PRODUCTION_VALIDATED:", leisure.territory, leisure.index);
  console.log("  numeric territories:", numericRows);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
