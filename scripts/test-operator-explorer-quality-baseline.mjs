/**
 * Unit gate: Operator Explorer quality baseline freeze (Arbor + Hotel Equities).
 * No Airtable writes.
 */
import assert from "node:assert/strict";
import {
  OPERATOR_QUALITY_BASELINE_EXPECTED_COUNT,
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  OPERATOR_QUALITY_BASELINE_RECORD_IDS,
  OPERATOR_QUALITY_BASELINE_SLUGS,
  OPERATOR_QUALITY_BASELINE_VERSION,
  getOperatorQualityBaselineEntry,
  getOperatorQualityBaselineFreezeSnapshot,
  isProtectedOperatorQualityBaseline,
} from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";
import {
  OPERATOR_TAB_CONTRACTS,
  OPERATOR_TAB_FACTORY_VERSION,
  getOperatorTabFactoryContractSummary,
  listOperatorPublishableTabs,
} from "../lib/partner-intelligence/operator-explorer-tab-contracts.js";

function main() {
  assert.equal(
    OPERATOR_QUALITY_BASELINE_VERSION,
    "frozen_2_operator_quality_baseline",
    "baseline version must match freeze docs"
  );
  assert.equal(
    OPERATOR_QUALITY_BASELINE_OPERATORS.length,
    OPERATOR_QUALITY_BASELINE_EXPECTED_COUNT,
    "expected exactly 2 golden operators"
  );
  assert.equal(OPERATOR_QUALITY_BASELINE_SLUGS.size, 2);
  assert.equal(OPERATOR_QUALITY_BASELINE_RECORD_IDS.size, 2);

  const arbor = getOperatorQualityBaselineEntry("arbor-lodging-cala");
  const he = getOperatorQualityBaselineEntry("hotel-equities-cala");
  assert.ok(arbor, "Arbor baseline missing");
  assert.ok(he, "Hotel Equities baseline missing");
  assert.equal(arbor.recordId, "recF5Z87OAqFgndoq");
  assert.equal(he.recordId, "recWPKu5laVZxsvpn");
  assert.equal(isProtectedOperatorQualityBaseline("recF5Z87OAqFgndoq"), true);
  assert.equal(isProtectedOperatorQualityBaseline("recWPKu5laVZxsvpn"), true);
  assert.equal(isProtectedOperatorQualityBaseline("recDOESNOTEXIST"), false);

  const snap = getOperatorQualityBaselineFreezeSnapshot();
  assert.equal(snap.expectedCount, 2);
  assert.deepEqual(
    snap.operators.map((o) => o.slug).sort(),
    ["arbor-lodging-cala", "hotel-equities-cala"]
  );

  const tabs = listOperatorPublishableTabs();
  assert.equal(tabs.length, 10, "expect 10 publishable Operator Explorer tabs");
  assert.ok(OPERATOR_TAB_CONTRACTS.length >= 10, "tab contracts must cover publishable tabs");

  const summary = getOperatorTabFactoryContractSummary();
  assert.equal(summary.version, OPERATOR_TAB_FACTORY_VERSION);
  assert.ok(summary.fieldCount > 0, "registry-backed field contracts must be non-empty");
  assert.ok(summary.benchmarkSlugs.includes("arbor-lodging-cala"));
  assert.ok(summary.benchmarkSlugs.includes("hotel-equities-cala"));

  console.log(
    JSON.stringify(
      {
        ok: true,
        baseline: snap,
        tabFactory: summary,
      },
      null,
      2
    )
  );
}

try {
  main();
} catch (err) {
  console.error("[test:operator-explorer-quality-baseline]", err?.message || err);
  process.exitCode = 1;
}
