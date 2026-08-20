#!/usr/bin/env node
/**
 *   npm run test:adp-multi-property-governed-metric-backfill-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import {
  discoverAdpPropertyIds,
  auditProperty,
  compareWaterstoneRegression,
} from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { LEGACY_INDEX_CUSTOMER_RENDER } from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import {
  EXECUTIVE_METRICS_CARD_COUNT,
  AI_DEMAND_CAPTURE_CUSTOMER_RENDER,
} from "../lib/ai-demand-positioning/customer/adp-customer-display-contract-v1.js";

const REPORT = join(process.cwd(), "reports/ai-demand-positioning/multi-property-governed-metric-backfill-v2.json");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

async function main() {
  const report = JSON.parse(readFileSync(REPORT, "utf-8"));
  assert.equal(report.execution.PROVIDER_CALLS, 0);
  assert.equal(report.execution.SPEND, 0);
  assert.equal(report.fiveCardContract.PROPERTIES_WITH_HIDDEN_CARDS, 0);
  assert.equal(report.governanceGap.WATERSTONE_SPECIFIC_METRIC_LOGIC, 0);
  assert.strictEqual(LEGACY_INDEX_CUSTOMER_RENDER, 0);
  assert.strictEqual(EXECUTIVE_METRICS_CARD_COUNT, 5);
  assert.strictEqual(AI_DEMAND_CAPTURE_CUSTOMER_RENDER, 0);

  const ids = discoverAdpPropertyIds();
  assert.ok(ids.length >= 4, "at least four ADP properties");

  const waterstone = auditProperty("adp_waterstone_boca_raton");
  const regression = compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE);
  assert.equal(regression.PHASE1_METRIC_DIFF, 0);
  assert.equal(regression.INDEX_DIFF, 0);
  assert.equal(waterstone.numericIndexTerritories.length, 4);

  const cambridge = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridge.ok);
  for (const row of Object.values(cambridge.payload.intentPresenceIndex || {})) {
    assert.equal(row.index, null);
    assert.ok(row.developing);
  }

  const nowNow = auditProperty("adp_now_now_noho");
  assert.ok(nowNow.phase1.aiConsiderationRate != null);
  assert.equal(nowNow.fiveCardContract.cardCount, 5);
  assert.equal(nowNow.fiveCardContract.cards.numberOneAppearanceRate, "Insufficient ranked responses");

  const renaissance = auditProperty("adp_renaissance_times_square");
  assert.equal(renaissance.governedCoreEligible, true);
  assert.ok(renaissance.phase1.aiScenarioPresence != null);
  assert.equal(renaissance.numericIndexTerritories.length, 0, "numeric index still deferred");

  assert.ok(report.propertyUniverse.WITH_USABLE_PARSED_OBSERVATIONS >= 4);
  assert.equal(report.waterstoneRegression.CERTIFIED_TERRITORIES, 4);

  console.log("test:adp-multi-property-governed-metric-backfill-v2 — PASS");
  console.log("  properties:", ids.length);
  console.log("  usable:", report.summary.WITH_USABLE_OBSERVATIONS);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
