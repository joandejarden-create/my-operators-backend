#!/usr/bin/env node
/**
 *   npm run test:adp-competitive-set-customer-copy-simplification-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");

const FORBIDDEN_UI = [
  "CORE appended",
  "CORE in top",
  "CORE not surfaced",
  "hotels visible",
  "Showing \" + rows.length + \" hotels (",
  "IDENTIFIABLE_CORE_TOTAL",
  "CORE_IN_TOP_10",
];

async function main() {
  const ui = readFileSync(UI, "utf8");
  const html = readFileSync(HTML, "utf8");

  for (const phrase of FORBIDDEN_UI) {
    assert.ok(!ui.includes(phrase), `customer-visible technical phrase leaked: ${phrase}`);
  }

  assert.ok(ui.includes("renderCompBenchmarkCount"));
  assert.ok(ui.includes("highlightCompCoreRows"));
  assert.ok(ui.includes("data-adp-comp-core-highlight"));
  assert.ok(ui.includes("Based on \" + coreCount + \" CORE comparable hotels"));
  assert.ok(ui.includes("data-adp-core-scroll"));
  assert.ok(html.includes("plus all comparable hotels used in the selected territory analysis"));

  let internalReconciliationPreserved = 0;
  let benchmarkCoreCountLinkPreserved = 0;

  const propertyIds = [
    "adp_waterstone_boca_raton",
    "adp_renaissance_times_square",
    "adp_cambridge_beaches_bermuda",
    "adp_now_now_noho",
  ];

  for (const propertyId of propertyIds) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile, { allPeriods: [period] });
    const block = payload.competitiveRankingByTerritory;

    assert.equal(block.byTerritory.overall.reconciliation, null, `${propertyId} overall has no reconciliation`);

    for (const [key, ranking] of Object.entries(block.byTerritory)) {
      if (key === "overall" || ranking.viewType === "overall") continue;
      const rec = ranking.reconciliation;
      if (!rec?.CORE_COUNT) continue;
      internalReconciliationPreserved++;
      assert.equal(rec.COUNT_MATCH, true, `${propertyId} ${key} internal reconciliation`);
      assert.ok(Number.isFinite(rec.CORE_IN_TOP_10));
      assert.ok(Number.isFinite(rec.CORE_APPENDED));
      assert.ok(Number.isFinite(rec.IDENTIFIABLE_CORE_TOTAL));
      benchmarkCoreCountLinkPreserved++;
    }
  }

  assert.ok(internalReconciliationPreserved > 0, "internal reconciliation preserved");
  assert.ok(benchmarkCoreCountLinkPreserved > 0, "territory CORE counts available for benchmark link");

  console.log("test:adp-competitive-set-customer-copy-simplification-v1 PASS");
  console.log("  CUSTOMER_VISIBLE_CORE_APPEND_LANGUAGE: 0");
  console.log("  CUSTOMER_VISIBLE_TOP10_RECONCILIATION_LANGUAGE: 0");
  console.log("  CUSTOMER_VISIBLE_ROW_RECONCILIATION_COUNT: 0");
  console.log("  INTERNAL_CORE_RECONCILIATION_PRESERVED: YES");
  console.log("  BENCHMARK_CORE_COUNT_LINK_PRESERVED: YES");
  console.log("  final: ADP_COMPETITIVE_SET_CUSTOMER_COPY_SIMPLIFICATION_V1_PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
