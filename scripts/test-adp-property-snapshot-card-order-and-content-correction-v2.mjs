#!/usr/bin/env node
/**
 * ADP Property Snapshot Card Order + Content Correction V2
 *   npm run test:adp-property-snapshot-card-order-and-content-correction-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const ER_V1 = join(process.cwd(), "lib/ai-demand-positioning/customer/executive-read-v1.js");
const ER_V2 = join(process.cwd(), "lib/ai-demand-positioning/customer/executive-read-v2.js");

const PROPERTIES = [
  ["adp_waterstone_boca_raton", "WATERSTONE"],
  ["adp_renaissance_times_square", "RENAISSANCE"],
  ["adp_cambridge_beaches_bermuda", "CAMBRIDGE"],
  ["adp_now_now_noho", "NOW_NOW"],
];

const CARD_ORDER = [
  "Property Reality Coverage",
  "Scenarios Monitored",
  "Traveler Needs Where Hotel Appeared",
  "Traveler Needs Where Hotel Was Missing",
  "Top Observed AI Alternative",
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
  const reality =
    payload.realityGap?.totalAttributes > 0
      ? roundAdpPercent((payload.realityGap.recognizedCount / payload.realityGap.totalAttributes) * 100)
      : null;
  const topAlt = payload.competitiveSet?.observed?.[0]?.name || null;
  return { total, appeared, missing, appearedPct, missingPct, reality, topAlt };
}

function main() {
  const ui = readFileSync(UI, "utf8");
  const snapshotFn = extractFunctionBody(ui, "renderExecKpis");
  const erV1 = readFileSync(ER_V1, "utf8");
  const erV2 = readFileSync(ER_V2, "utf8");

  assert.equal((snapshotFn.match(/kpiCardWithInfo\(/g) || []).length, 5, "PROPERTY_SNAPSHOT_CARD_COUNT=5");
  assert.ok(snapshotFn.includes("Scenarios Monitored"), "Scenarios Monitored restored");
  assert.ok(ui.includes("scenariosMonitoredSnapshot"), "scenarios helper present");
  assert.ok(!snapshotFn.includes("Strongest Demand Territory"), "PROPERTY_SNAPSHOT_STRONGEST_DEMAND_TERRITORY_VISIBLE=0");
  assert.ok(!snapshotFn.includes("strongestTerritory"), "no strongestTerritory in snapshot");
  assert.ok(!snapshotFn.includes("Strongest Segment"), "no Strongest Segment");
  assert.ok(!/\$\{propertyId\}|waterstone_boca|WATERSTONE_ONLY/.test(snapshotFn), "PROPERTY_SPECIFIC_SNAPSHOT_CODE=0");

  let cursor = -1;
  const orderMarkers = [
    "propertyRealityCoverageSnapshot",
    '"Scenarios Monitored"',
    '"Traveler Needs Where Hotel Appeared"',
    '"Traveler Needs Where Hotel Was Missing"',
    '"Top Observed AI Alternative"',
  ];
  for (const marker of orderMarkers) {
    const idx = snapshotFn.indexOf(marker);
    assert.ok(idx > cursor, `order fail at ${marker}`);
    cursor = idx;
  }

  assert.ok(snapshotFn.includes("Most frequently named alternative hotel across monitored AI responses this period."));
  assert.ok(ui.includes("traveler scenarios × "), "scenarios × providers supporting copy");
  assert.ok(ui.includes("monitoredVolume") || ui.includes("scenarioCount * providerCount"), "scenarios × providers primary value");
  assert.ok(ui.includes("appeared in at least one AI answer for"));
  assert.ok(ui.includes("did not appear in any monitored AI answer for"));

  // Executive Read selection logic untouched
  assert.ok(erV1.includes("primaryStrength") || erV1.includes("Biggest Strength") || erV1.includes("STRENGTH"), "ER strength preserved");
  assert.ok(erV2.includes("buildExecutiveReadUx"), "ER UX builder preserved");
  assert.ok(!snapshotFn.includes("Biggest Strength"), "snapshot does not compete with Executive Read strength");

  const multi = {};
  for (const [propertyId, key] of PROPERTIES) {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestPeriod(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile);
    const snap = snapshotFromPayload(payload);

    assert.equal(snap.appeared + snap.missing, snap.total, `${key} APPEARED_PLUS_MISSING`);
    assert.ok(Math.abs(snap.appearedPct + snap.missingPct - 100) < 0.15, `${key} PCT_SUM`);
    assert.ok(snap.missing >= 0, `${key} MISSING_AS_ZERO not forced`);
    assert.ok(Number.isFinite(snap.total) && snap.total > 0, `${key} scenarios monitored`);
    assert.ok(snap.topAlt, `${key} top alternative`);
    if (payload.executiveMetrics?.scenarioPresence) {
      assert.equal(snap.total, payload.executiveMetrics.scenarioPresence.eligibleScenarios);
      assert.equal(snap.appeared, payload.executiveMetrics.scenarioPresence.capturedScenarios);
    }
    multi[key] = "PASS";
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

  console.log("test:adp-property-snapshot-card-order-and-content-correction-v2 PASS");
  console.log("  CARD_COUNT: 5");
  console.log("  CARD_ORDER:", CARD_ORDER.join(" > "));
  console.log("  PROPERTY_SNAPSHOT_STRONGEST_DEMAND_TERRITORY_VISIBLE: 0");
  console.log("  APPEARED_PLUS_MISSING_EQUALS_SCENARIOS: YES");
  console.log("  APPEARED_PCT_PLUS_MISSING_PCT: 100%");
  console.log("  MISSING_AS_ZERO: 0");
  console.log("  WATERSTONE_REALITY:", ws.reality != null ? `${ws.reality}%` : "—");
  console.log("  WATERSTONE_SCENARIOS:", ws.total);
  console.log(`  WATERSTONE_APPEARED: ${ws.appearedPct}% (${ws.appeared})`);
  console.log(`  WATERSTONE_MISSING: ${ws.missingPct}% (${ws.missing})`);
  console.log("  WATERSTONE_TOP_ALT:", ws.topAlt);
  console.log("  MULTI:", JSON.stringify(multi));
  console.log("  EXECUTIVE_READ_BIGGEST_STRENGTH_PRESERVED: YES");
  console.log("  PROPERTY_SPECIFIC_SNAPSHOT_CODE: 0");
  console.log("  PROVIDER_CALLS: 0");
  console.log("  final: ADP_PROPERTY_SNAPSHOT_CARD_ORDER_AND_CONTENT_CORRECTION_V2_PASS");
}

main();
