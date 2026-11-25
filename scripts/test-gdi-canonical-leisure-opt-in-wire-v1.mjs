#!/usr/bin/env node
/**
 * $0 — Canonical Full E2E leisure STANDARD stage.
 * Always assess all six; execute applicable only; paid OFF by default.
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
  LEISURE_MODULE_OUTCOME,
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
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

check("default_standard_stage_is_on", () => {
  const r = resolveCanonicalLeisureOptIn({});
  assert.equal(r.enabled, true);
  assert.equal(r.standardStage, true);
  assert.match(r.reason, /STANDARD_STAGE/);
});

check("explicit_disable_only_for_isolated_tests", () => {
  assert.equal(
    resolveCanonicalLeisureOptIn({ disableLeisureStandardStage: true }).enabled,
    false,
  );
});

check("wyndham_id_reconciliation_not_hilton", () => {
  assert.equal(
    WYNDHAM_HOTEL_ID_RECONCILIATION.certifiedWyndhamSanDiegoBayside,
    CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  );
  assert.equal(
    WYNDHAM_HOTEL_ID_RECONCILIATION.erroneousCloseoutId,
    HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  );
  assert.equal(
    WYNDHAM_HOTEL_ID_RECONCILIATION.relationship,
    "SEPARATE_HOTELS_NOT_ALIAS",
  );
  assert.equal(CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID, "recnPB3Q02NO8Ma1o");
  assert.equal(HILTON_NY_TIMES_SQUARE_HOTEL_ID, "rec35fExUxCClpOP6");
});

check("eligibility_matrix_covers_hotels_and_all_six", () => {
  const matrix = buildDefaultCanonicalLeisureApplicabilityMatrix();
  assert.equal(matrix.defaults.standardStage, true);
  assert.equal(matrix.defaults.optInRequired, false);
  assert.equal(matrix.defaults.alwaysAssessAllSix, true);
  assert.equal(matrix.defaults.silentRunAtEveryHotel, false);
  assert.equal(matrix.moduleIds.length, 6);

  const wyndham = matrix.hotels.find(
    (h) => h.hotelId === CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  );
  assert.ok(wyndham);
  assert.equal(wyndham.name, "Wyndham San Diego Bayside");
  assert.equal(wyndham.wyndhamIdGuard, "CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE");

  const hilton = matrix.hotels.find(
    (h) => h.hotelId === HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  );
  assert.ok(hilton);
  assert.equal(hilton.name, "Hilton New York Times Square");
  assert.equal(hilton.wyndhamIdGuard, "ERRONEOUS_IF_LABELED_WYNDHAM");

  const forceAll = matrix.hotels.find((h) => h.hotelId === "force-all-six-demo");
  assert.equal(forceAll.selectedModuleIds.length, 6);

  fs.writeFileSync(
    path.join(REPORT, "LEISURE_ELIGIBILITY_MATRIX.json"),
    `${JSON.stringify(matrix, null, 2)}\n`,
  );
});

check("all_six_selectable_via_force_on_any_hotel", () => {
  const matrix = buildCanonicalLeisureEligibilityMatrix([
    {
      hotelId: CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
      name: "Wyndham San Diego Bayside",
      forceModules: ALL_SIX,
    },
  ]);
  assert.deepEqual(matrix.hotels[0].selectedModuleIds.sort(), [...ALL_SIX].sort());
});

await checkAsync("standard_stage_runs_all_six_mock_live_zero_paid", async () => {
  const stage = await runCanonicalLeisureOptInStage({
    hotelId: "recaokE8psIbuVeuH",
    leisureModules: ALL_SIX,
    writeLeisureArtifacts: true,
    outDir: path.join(REPORT, "all-six-mock"),
  });
  assert.equal(stage.enabled, true);
  assert.equal(stage.standardStage, true);
  assert.equal(stage.specializedLeisureExecution, true);
  assert.equal(stage.spentUsd, 0);
  assert.equal(stage.moduleIds.length, 6);
  assert.ok(stage.moduleOutcomes.every((m) => m.outcome));
  const executed = stage.moduleOutcomes.filter(
    (m) => m.outcome === LEISURE_MODULE_OUTCOME.APPLICABLE_EXECUTED,
  );
  assert.equal(executed.length, 6);
  assert.equal(stage.stagedPlanWriteApplied, false);
});

await checkAsync("canonical_entry_runs_leisure_by_default", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    // leisure intentionally NOT flagged — must still run as standard
    disableCadStage: true,
    outDir: path.join(REPORT, "canonical-default-leisure"),
  });
  assert.equal(r.leisureOptIn?.enabled, true);
  assert.equal(r.leisureOptIn?.standardStage, true);
  assert.notEqual(r.leisureOptIn?.status, "SKIPPED_OPT_IN_OFF");
  assert.equal(Number(r.leisureOptIn?.spentUsd || 0), 0);
});

await checkAsync("canonical_entry_can_explicitly_disable_leisure", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    disableLeisureStandardStage: true,
    disableCadStage: true,
    outDir: path.join(REPORT, "canonical-leisure-disabled"),
  });
  assert.equal(r.leisureOptIn?.enabled, false);
  assert.equal(r.leisureOptIn?.status, "SKIPPED_EXPLICITLY_DISABLED");
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
