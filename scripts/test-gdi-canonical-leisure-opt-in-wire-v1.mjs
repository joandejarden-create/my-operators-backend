#!/usr/bin/env node
/**
 * $0 — Canonical Full E2E leisure opt-in wire.
 * All six modules selectable when enabled; never silent default; paid OFF.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveCanonicalLeisureOptIn,
  buildDefaultCanonicalLeisureApplicabilityMatrix,
  buildCanonicalLeisureEligibilityMatrix,
  runCanonicalLeisureOptInStage,
  listLeisureModuleIds,
  GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-leisure-opt-in-wire-v1.js";
import { runCanonicalFullE2eResearch } from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/gdi-canonical-leisure-opt-in-wire-v1",
);
fs.mkdirSync(REPORT, { recursive: true });

const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const ALL_SIX = listLeisureModuleIds();

check("registry_exposes_all_six_modules", () => {
  assert.deepEqual(ALL_SIX.sort(), [
    "leisure_attractions",
    "leisure_destination",
    "leisure_distribution",
    "leisure_events",
    "leisure_sports",
    "leisure_universities",
  ]);
});

check("default_opt_in_is_off", () => {
  const r = resolveCanonicalLeisureOptIn({});
  assert.equal(r.enabled, false);
  assert.equal(r.reason, "LEISURE_OPT_IN_OFF_BY_DEFAULT");
  assert.match(r.note, /not specialized leisure-module execution/i);
});

check("explicit_enable_or_module_list_turns_on", () => {
  assert.equal(resolveCanonicalLeisureOptIn({ enableLeisureModules: true }).enabled, true);
  assert.equal(
    resolveCanonicalLeisureOptIn({ leisureModules: ["leisure_events"] }).enabled,
    true,
  );
  assert.equal(
    resolveCanonicalLeisureOptIn({ leisureModules: ["leisure_events"] }).forceModules[0],
    "leisure_events",
  );
});

check("eligibility_matrix_covers_hotels_and_all_six", () => {
  const matrix = buildDefaultCanonicalLeisureApplicabilityMatrix();
  assert.equal(matrix.contract, GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1);
  assert.equal(matrix.defaults.optInRequired, true);
  assert.equal(matrix.defaults.silentRunAtEveryHotel, false);
  assert.equal(matrix.defaults.paidProductionOff, true);
  assert.equal(matrix.moduleIds.length, 6);

  const forceAll = matrix.hotels.find((h) => h.hotelId === "force-all-six-demo");
  assert.ok(forceAll);
  assert.equal(forceAll.selectedModuleIds.length, 6);
  assert.ok(forceAll.moduleEligibility.every((m) => m.eligible === true));

  const laluna = matrix.hotels.find((h) => h.hotelId === "recaokE8psIbuVeuH");
  assert.ok(laluna.selectedModuleIds.includes("leisure_distribution"));
  assert.ok(laluna.selectedModuleIds.includes("leisure_events"));

  const bethesda = matrix.hotels.find((h) => h.hotelId === "recLuxvwwxID7U2B8");
  assert.equal(bethesda.fourteenSegmentOverlapIsNotLeisureExecution, true);

  fs.writeFileSync(
    path.join(REPORT, "LEISURE_ELIGIBILITY_MATRIX.json"),
    `${JSON.stringify(matrix, null, 2)}\n`,
  );
});

check("all_six_selectable_via_force_on_any_hotel", () => {
  const matrix = buildCanonicalLeisureEligibilityMatrix([
    {
      hotelId: "recLuxvwwxID7U2B8",
      name: "Bethesda Marriott",
      forceModules: ALL_SIX,
    },
  ]);
  assert.deepEqual(matrix.hotels[0].selectedModuleIds.sort(), [...ALL_SIX].sort());
});

await checkAsync("opt_in_stage_runs_all_six_mock_live_zero_paid", async () => {
  const stage = await runCanonicalLeisureOptInStage({
    hotelId: "recaokE8psIbuVeuH",
    enableLeisureModules: true,
    leisureModules: ALL_SIX,
    writeLeisureArtifacts: true,
    outDir: path.join(REPORT, "all-six-mock"),
  });
  assert.equal(stage.enabled, true);
  assert.equal(stage.specializedLeisureExecution, true);
  assert.equal(stage.spentUsd, 0);
  assert.equal(stage.paidCalls || 0, 0);
  assert.equal(stage.paidProductionOff, true);
  assert.equal(stage.moduleIds.length, 6);
  assert.equal(stage.status, "MOCK_LIVE_COMPLETE");
  assert.ok(stage.fourteenSegmentCalls.length >= 6);
  assert.equal(stage.attribution.distinctFromCanonicalFourteenSegmentDefault, true);
  assert.equal(stage.stagedPlanWriteApplied, false);
});

await checkAsync("canonical_entry_skips_leisure_by_default", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    outDir: path.join(REPORT, "canonical-default-no-leisure"),
  });
  assert.equal(r.leisureOptIn?.enabled, false);
  assert.equal(r.leisureOptIn?.status, "SKIPPED_OPT_IN_OFF");
  assert.equal(r.leisureOptIn?.specializedLeisureExecution, false);
  assert.equal(Number(r.leisureOptIn?.spentUsd || 0), 0);
});

await checkAsync("canonical_entry_runs_leisure_when_explicitly_enabled", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    enableLeisureModules: true,
    leisureModules: ["leisure_events", "leisure_attractions", "leisure_distribution"],
    outDir: path.join(REPORT, "canonical-leisure-opt-in"),
  });
  assert.equal(r.ok !== false || r.chain != null, true);
  assert.equal(r.leisureOptIn?.enabled, true);
  assert.equal(r.leisureOptIn?.specializedLeisureExecution, true);
  assert.equal(Number(r.leisureOptIn?.spentUsd || 0), 0);
  assert.equal(Number(r.leisureOptIn?.paidCalls || 0), 0);
  assert.ok(r.leisureOptIn?.moduleIds?.includes("leisure_events"));
  assert.ok(r.summaryPath);
  const summary = JSON.parse(fs.readFileSync(r.summaryPath, "utf8"));
  assert.equal(summary.leisureOptIn?.enabled, true);
  assert.equal(summary.leisureOptIn?.paidProductionOff, true);
});

const failed = results.filter((r) => !r.ok);
const out = {
  contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
};
fs.writeFileSync(
  path.join(REPORT, "TEST_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`,
);
console.log(JSON.stringify(out, null, 2));
if (failed.length) process.exit(1);
