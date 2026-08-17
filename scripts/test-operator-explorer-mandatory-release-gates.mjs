/**
 * Unit gate: Operator Explorer mandatory release gate contract surface.
 * No Airtable writes. Ensures gate checklist + baseline wiring stay aligned.
 */
import assert from "node:assert/strict";
import {
  OPERATOR_QUALITY_BASELINE_EXPECTED_COUNT,
  OPERATOR_QUALITY_BASELINE_VERSION,
} from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";
import {
  OPERATOR_FIELD_RESOLUTION_STATES,
  OPERATOR_TAB_CONTRACT_FAIL_RULES,
  getOperatorTabFactoryContractSummary,
} from "../lib/partner-intelligence/operator-explorer-tab-contracts.js";

/** Canonical gate ids — keep in sync with docs/data-intelligence/operator-explorer-mandatory-release-gates.md */
export const OPERATOR_MANDATORY_RELEASE_GATES = Object.freeze([
  "source_provenance_by_tab",
  "tab_factory_audit",
  "rendered_field_completeness",
  "no_empty_rendered_components",
  "section_pattern_parity",
  "golden_content_quality",
  "operator_specific_source_validation",
]);

function main() {
  assert.equal(OPERATOR_QUALITY_BASELINE_VERSION, "frozen_2_operator_quality_baseline");
  assert.equal(OPERATOR_QUALITY_BASELINE_EXPECTED_COUNT, 2);
  assert.equal(OPERATOR_MANDATORY_RELEASE_GATES.length, 7);
  assert.ok(OPERATOR_FIELD_RESOLUTION_STATES.includes("complete"));
  assert.ok(OPERATOR_FIELD_RESOLUTION_STATES.includes("blocked_empty_render"));
  assert.ok(OPERATOR_TAB_CONTRACT_FAIL_RULES.visible_empty_field);
  assert.ok(OPERATOR_TAB_CONTRACT_FAIL_RULES.below_benchmark_depth);

  const summary = getOperatorTabFactoryContractSummary();
  assert.equal(summary.publishableTabCount, 10);
  assert.ok(summary.fieldCount > 20, "expect substantive registry field coverage");

  console.log(
    JSON.stringify(
      {
        ok: true,
        gates: OPERATOR_MANDATORY_RELEASE_GATES,
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
  console.error("[test:operator-explorer-mandatory-release-gates]", err?.message || err);
  process.exitCode = 1;
}
