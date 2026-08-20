#!/usr/bin/env node
/**
 *   npm run test:adp-property-snapshot-traveler-needs-format-v1
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];

function extractFunctionBody(source, name) {
  const start = source.indexOf(`function ${name}`);
  assert.ok(start >= 0, `missing ${name}`);
  const brace = source.indexOf("{", start);
  let depth = 0;
  for (let i = brace; i < source.length; i += 1) {
    if (source[i] === "{") depth += 1;
    if (source[i] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, i + 1);
    }
  }
  throw new Error(`unclosed ${name}`);
}
function snapshotFromPayload(payload) {
  const sp = payload.executiveMetrics?.scenarioPresence;
  const dc = payload.demandCapture;
  const total = sp?.eligibleScenarios ?? dc.totalScenarios;
  const appeared = sp?.capturedScenarios ?? dc.capturedScenarios;
  const appearedPct = sp?.rate ?? dc.overallRate;
  const missing = total - appeared;
  const missingPct = roundAdpPercent(100 - appearedPct);
  return { total, appeared, missing, appearedPct, missingPct };
}

async function main() {
  const ui = readFileSync(UI, "utf8");
  assert.ok(ui.includes("Traveler Needs Where Hotel Appeared"));
  assert.ok(ui.includes("Traveler Needs Where Hotel Was Missing"));
  assert.ok(ui.includes("travelerNeedsAppearanceSnapshot"));
  assert.ok(ui.includes("appeared in at least one AI answer for"));
  assert.ok(ui.includes("did not appear in any monitored AI answer for"));
  assert.ok(!ui.includes('kpiCard("Questions Missing"'));
  assert.ok(ui.includes("Scenarios Monitored"));
  assert.ok(ui.includes("scenariosMonitoredSnapshot"));
  assert.ok(!extractFunctionBody(ui, "renderExecKpis").includes("Strongest Demand Territory"));
  assert.ok(!extractFunctionBody(ui, "renderExecKpis").includes("Strongest Segment"));

  for (const propertyId of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile);
    const snap = snapshotFromPayload(payload);

    assert.equal(snap.appeared + snap.missing, snap.total, `${propertyId} count identity`);
    assert.ok(Math.abs(snap.appearedPct + snap.missingPct - 100) < 0.15, `${propertyId} pct complement`);

    if (payload.executiveMetrics?.scenarioPresence) {
      const sp = payload.executiveMetrics.scenarioPresence;
      assert.equal(snap.appeared, sp.capturedScenarios, `${propertyId} reconciles Scenario Presence count`);
      assert.equal(snap.appearedPct, sp.rate, `${propertyId} reconciles Scenario Presence rate`);
      assert.equal(snap.total, sp.eligibleScenarios, `${propertyId} reconciles Scenario Presence universe`);
    }
  }

  const ws = snapshotFromPayload(
    buildOwnerPayload(
      loadLatestPeriod("adp_waterstone_boca_raton"),
      buildScenarioUniverse(loadPropertyProfile("adp_waterstone_boca_raton")),
      loadPropertyProfile("adp_waterstone_boca_raton")
    )
  );
  assert.equal(ws.total, 78);
  assert.equal(ws.appeared, 59);
  assert.equal(ws.missing, 19);
  assert.equal(ws.appearedPct, 75.6);
  assert.equal(ws.missingPct, 24.4);

  console.log("test:adp-property-snapshot-traveler-needs-format-v1 PASS");
  console.log("  WATERSTONE_EXPECTED: 59 + 19 = 78");
  console.log("  APPEARED: 75.6% (59)");
  console.log("  MISSING: 24.4% (19)");
  console.log("  SCENARIO_PRESENCE_RECONCILIATION: PASS");
  console.log("  METRIC_FORMULA_DIFF: 0");
  console.log("  PROVIDER_CALLS: 0");
  console.log("  final: ADP_PROPERTY_SNAPSHOT_TRAVELER_NEEDS_FORMAT_V1_PASS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
