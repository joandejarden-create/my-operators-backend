#!/usr/bin/env node
/**
 * ADP Surgical Phase 1 Reintroduction V2 regression tests.
 *   npm run test:adp-surgical-phase1-v2
 */

import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import {
  buildOptionalExecutiveMetrics,
  attachOptionalExecutiveMetrics,
} from "../lib/ai-demand-positioning/metrics/optional-executive-metrics.js";

const UI_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");
const INVENTORY = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/legacy-section-inventory-v1.json"
);
const CAMBRIDGE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/cambridge-beaches-legacy-baseline-v1.json"
);
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

function getPath(obj, path) {
  return path.split(".").reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

function assertNear(actual, expected, label, tolerance = 0.3) {
  assert.ok(
    actual != null && Math.abs(Number(actual) - Number(expected)) <= tolerance,
    `${label}: expected ~${expected}, got ${actual}`
  );
}

function legacySignature(payload) {
  return JSON.stringify({
    demandCapture: payload.demandCapture,
    lostDemand: {
      totalLost: payload.lostDemand?.totalLost,
      highRelevanceLost: payload.lostDemand?.highRelevanceLost,
    },
    realityGap: {
      gapScore: payload.realityGap?.gapScore,
      totalAttributes: payload.realityGap?.totalAttributes,
      gapCount: payload.realityGap?.gapCount,
    },
    competitiveSet: {
      declaredCount: payload.competitiveSet?.declaredCount,
      observedCount: payload.competitiveSet?.observedCount,
      overlapRate: payload.competitiveSet?.overlapRate,
    },
    whiteSpace: {
      totalOpportunities: payload.whiteSpace?.totalOpportunities,
      highOpportunities: payload.whiteSpace?.highOpportunities,
    },
    evidence: {
      citationRate: payload.evidence?.citationRate,
      totalObservations: payload.evidence?.totalObservations,
      totalWithSources: payload.evidence?.totalWithSources,
    },
  });
}

function assertLegacyBaseline(payload, baselineFixture) {
  for (const [path, expected] of Object.entries(baselineFixture.legacyFields)) {
    assert.strictEqual(getPath(payload, path), expected, `legacy ${path}`);
  }
}

async function main() {
  const inventory = JSON.parse(readFileSync(INVENTORY, "utf-8"));
  assert.strictEqual(inventory.allExistingSectionsCaptured, true);

  const cambridgeBaseline = JSON.parse(readFileSync(CAMBRIDGE_BASELINE, "utf-8"));
  const waterstoneBaseline = JSON.parse(readFileSync(WATERSTONE_BASELINE, "utf-8"));

  // --- Old snapshot without executiveMetrics on disk ---
  const cambridgeSnapshotRaw = loadPublishedReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridgeSnapshotRaw, "cambridge snapshot");
  assert.ok(!loadPublishedReport("adp_cambridge_beaches_bermuda")?.executiveMetrics, "snapshot file unchanged");
  assertLegacyBaseline(cambridgeSnapshotRaw, cambridgeBaseline);

  const cambridgeRead = await getPublishedOwnerReport("adp_cambridge_beaches_bermuda");
  assert.ok(cambridgeRead.ok);
  assertLegacyBaseline(cambridgeRead.payload, cambridgeBaseline);

  // --- Legacy payload without executiveMetrics path ---
  const profileC = loadPropertyProfile("adp_cambridge_beaches_bermuda");
  const periodC = loadLatestPeriod("adp_cambridge_beaches_bermuda");
  const scenariosC = buildScenarioUniverse(profileC);
  const payloadWithOptional = buildOwnerPayload(periodC, scenariosC, profileC, {
    allPeriods: loadAllPeriods("adp_cambridge_beaches_bermuda"),
  });
  const { executiveMetrics: _strip, ...legacyOnly } = payloadWithOptional;
  assertLegacyBaseline(legacyOnly, cambridgeBaseline);

  // --- Waterstone legacy unchanged + optional metrics ---
  const waterstoneSnapshot = loadPublishedReport("adp_waterstone_boca_raton");
  assertLegacyBaseline(waterstoneSnapshot, waterstoneBaseline);

  const profileW = loadPropertyProfile("adp_waterstone_boca_raton");
  const periodsW = loadAllPeriods("adp_waterstone_boca_raton");
  const periodW = loadLatestPeriod("adp_waterstone_boca_raton");
  const scenariosW = buildScenarioUniverse(profileW);
  const waterstonePayload = buildOwnerPayload(periodW, scenariosW, profileW, { allPeriods: periodsW });

  // Additive attach must not mutate legacy snapshot fields.
  const enrichedWaterstone = attachOptionalExecutiveMetrics(
    waterstoneSnapshot,
    periodW,
    scenariosW,
    profileW,
    { allPeriods: periodsW }
  );
  for (const key of Object.keys(waterstoneSnapshot)) {
    assert.deepStrictEqual(enrichedWaterstone[key], waterstoneSnapshot[key], `legacy field preserved: ${key}`);
  }

  assert.ok(waterstonePayload.executiveMetrics, "waterstone optional metrics present");
  const em = waterstonePayload.executiveMetrics;
  const tol = waterstoneBaseline.tolerance;
  const exp = waterstoneBaseline.optionalExecutiveMetricsExpectations;

  // Fresh build: same-period published snapshot still matches live payload headlines.
  if (
    waterstonePayload.period?.periodId === waterstoneSnapshot.period?.periodId &&
    waterstonePayload.period?.scenarioCount === waterstoneSnapshot.period?.scenarioCount
  ) {
    assert.strictEqual(waterstonePayload.demandCapture.overallRate, waterstoneSnapshot.demandCapture.overallRate);
    assert.strictEqual(waterstonePayload.evidence.citationRate, waterstoneSnapshot.evidence.citationRate);
    assert.strictEqual(waterstonePayload.lostDemand.totalLost, waterstoneSnapshot.lostDemand.totalLost);
    assertNear(em.considerationRate.rate, exp["considerationRate.rate"], "considerationRate", tol);
    assertNear(em.scenarioPresence.rate, exp["scenarioPresence.rate"], "scenarioPresence", tol);
    assertNear(em.rankMetrics.numberOneAppearanceRate, exp["rankMetrics.numberOneAppearanceRate"], "#1", tol);
    assertNear(em.rankMetrics.topThreeAppearanceRate, exp["rankMetrics.topThreeAppearanceRate"], "top3", tol);
    assert.strictEqual(em.rankMetrics.rankEligibleN, exp["rankMetrics.rankEligibleN"]);
  } else {
    assert.ok(Number.isFinite(em.considerationRate.rate));
    assert.ok(Number.isFinite(em.scenarioPresence.rate));
  }
  assert.ok(em.competitorPresentScenarios.scenarioCount > 0);

  // --- Partial executiveMetrics ---
  const partialPayload = attachOptionalExecutiveMetrics(
    { ok: true, demandCapture: waterstoneSnapshot.demandCapture },
    periodW,
    scenariosW,
    profileW,
    { allPeriods: periodsW }
  );
  assert.ok(partialPayload.executiveMetrics?.considerationRate);

  // --- Malformed executiveMetrics must not break attach ---
  const malformed = attachOptionalExecutiveMetrics(
    { ok: true, demandCapture: { overallRate: 1 }, executiveMetrics: { considerationRate: { rate: "bad" } } },
    periodW,
    scenariosW,
    profileW
  );
  assert.strictEqual(malformed.executiveMetrics.considerationRate.rate, "bad");

  // --- buildOptionalExecutiveMetrics null-safe ---
  assert.strictEqual(buildOptionalExecutiveMetrics(null, scenariosW, profileW), null);

  // --- UI safety strings ---
  const ui = readFileSync(UI_JS, "utf-8") + readFileSync(HTML, "utf-8");
  assert.strictEqual(ui.includes("Headline KPIs unavailable"), false);
  assert.ok(ui.includes("renderExecutiveMetricEnhancements"));
  assert.ok(ui.includes("AI Demand Positioning Metrics"));
  assert.ok(ui.includes("Property Reality Coverage"));
  assert.ok(ui.includes("Strongest Demand Territory"));
  assert.ok(ui.includes("Top Observed AI Alternative"));
  assert.ok(!ui.includes('kpiCard("AI Demand Capture"'));
  assert.ok(ui.includes("renderExecKpis"));

  // --- Blocked metrics absent from optional contract ---
  assert.strictEqual(em.aiConsiderationIndex, undefined);
  assert.strictEqual(em.competitiveWinRate, undefined);
  assert.strictEqual(em.opportunityScore, undefined);

  console.log("test:adp-surgical-phase1-v2 — PASS");
  console.log("  Cambridge demandCapture:", cambridgeRead.payload.demandCapture.overallRate);
  console.log("  Cambridge citationRate:", cambridgeRead.payload.evidence.citationRate);
  console.log("  Waterstone considerationRate:", em.considerationRate.rate);
  console.log("  Waterstone scenarioPresence:", em.scenarioPresence.rate);
  console.log("  Inventory sections:", inventory.renderOrder.length);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
