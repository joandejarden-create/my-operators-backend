#!/usr/bin/env node
/**
 * Phase 1B PR-1 regression tests — verified context bridge / eligibility / min coverage.
 */

import assert from "assert/strict";
import {
  DEFAULT_MINIMUM_ELIGIBLE_ATTRIBUTES,
  assessMinimumAttributeCoverage,
  classifySelectedAttributeEligibility,
  ELIGIBILITY_STATUS,
  hashSelectionInventory,
  explainDenominatorDifference,
  buildVerifiedContextBridgeDiagnostic,
  buildPilotSuiteDiagnostics,
  suggestAttributeCandidates,
  STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL,
} from "../lib/ai-demand-positioning/verified-context/index.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { ATTRIBUTE_DEFINITIONS, computeRealityGap } from "../lib/ai-demand-positioning/intelligence/reality-gap.js";

let failures = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failures += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("minimum threshold is configurable default 12", () => {
  const a = assessMinimumAttributeCoverage(12);
  assert.equal(a.meetsMinimum, true);
  assert.equal(a.minimumEligible, DEFAULT_MINIMUM_ELIGIBLE_ATTRIBUTES);
  const b = assessMinimumAttributeCoverage(10);
  assert.equal(b.meetsMinimum, false);
  assert.equal(b.flag, "BELOW_MINIMUM_ATTRIBUTE_COVERAGE");
  assert.equal(b.shortfall, 2);
  const c = assessMinimumAttributeCoverage(5, { minimumEligible: 5 });
  assert.equal(c.meetsMinimum, true);
  assert.equal(c.minimumEligible, 5);
});

check("Bethesda preserves selection and maps eligibility", () => {
  const profile = loadPropertyProfile("adp_bethesda_marriott");
  assert.ok(profile);
  assert.equal(profile.attributes.length, 15);
  const el = classifySelectedAttributeEligibility(profile.attributes, profile);
  assert.equal(el.selectedCount, 15);
  assert.equal(el.undefinedKeys.length, 0);
  assert.ok(el.eligibleCount >= 1);
  assert.ok(el.rows.every((r) => r.inSelectionInventory === true));
  // Customer fixture denom still includes all defined selected keys
  const fixtureDenom = profile.attributes.filter((a) => ATTRIBUTE_DEFINITIONS[a]).length;
  assert.equal(fixtureDenom, 15);
});

check("Spice Island selection preserved; semantic overlaps reported", () => {
  const profile = loadPropertyProfile("adp_spice_island_beach_resort");
  assert.equal(profile.attributes.length, 24);
  const el = classifySelectedAttributeEligibility(profile.attributes, profile);
  assert.equal(el.undefinedKeys.length, 0);
  assert.ok(el.semanticOverlaps.length >= 1);
});

check("YOTEL undefined keys are SELECTED_UNDEFINED not silent", () => {
  const profile = loadPropertyProfile("adp_yotel_geneva_lake");
  assert.ok(profile.attributes.includes("airport_shuttle"));
  assert.ok(profile.attributes.includes("park_and_fly"));
  const el = classifySelectedAttributeEligibility(profile.attributes, profile);
  assert.ok(el.undefinedKeys.includes("airport_shuttle"));
  assert.ok(el.undefinedKeys.includes("park_and_fly"));
  const undefRows = el.rows.filter((r) =>
    ["airport_shuttle", "park_and_fly"].includes(r.attribute)
  );
  assert.ok(
    undefRows.every((r) => r.eligibilityStatus === ELIGIBILITY_STATUS.SELECTED_UNDEFINED)
  );
  assert.ok(undefRows.every((r) => r.inEligibleDenominator === false));
  assert.ok(undefRows.every((r) => r.inSelectionInventory === true));
});

check("duplicate handling marks SELECTED_DUPLICATE", () => {
  const profile = loadPropertyProfile("adp_bethesda_marriott");
  const keys = ["meeting_space", "meeting_space", "ballroom"];
  const el = classifySelectedAttributeEligibility(keys, profile);
  assert.ok(el.duplicateKeys.includes("meeting_space"));
});

check("missing provenance → SELECTED_NEEDS_VERIFICATION excluded from bridged denom", () => {
  const profile = {
    propertyId: "adp_test_isolation",
    name: "Test Hotel",
    attributes: ["pet_friendly"],
    // no website / official URL → missing provenance path
  };
  const el = classifySelectedAttributeEligibility(profile.attributes, profile);
  assert.equal(el.rows[0].eligibilityStatus, ELIGIBILITY_STATUS.SELECTED_NEEDS_VERIFICATION);
  assert.equal(el.rows[0].inEligibleDenominator, false);
  assert.equal(el.eligibleCount, 0);
});

check("denominator exclusion artifact warning", () => {
  const fixtureRg = { gapScore: 80, totalAttributes: 10 };
  const bridgedRg = { gapScore: 50, totalAttributes: 6 };
  const eligibility = {
    rows: [
      {
        attribute: "a",
        inSelectionInventory: true,
        inEligibleDenominator: false,
        eligibilityStatus: "SELECTED_NEEDS_VERIFICATION",
        note: "weak",
      },
    ],
  };
  const diff = explainDenominatorDifference(fixtureRg, bridgedRg, eligibility);
  assert.ok(String(diff.artifactWarning || "").includes("DENOMINATOR_ARTIFACT"));
  assert.equal(diff.customerFacingUnchanged, true);
});

check("dual-run does not mutate profile selection", () => {
  const profile = loadPropertyProfile("adp_bethesda_marriott");
  const before = JSON.stringify(profile.attributes);
  const diag = buildVerifiedContextBridgeDiagnostic("adp_bethesda_marriott", {
    observations: [],
  });
  assert.equal(JSON.stringify(profile.attributes), before);
  assert.equal(diag.ok, true);
  assert.deepEqual(diag.selectedAttributes, profile.attributes);
  assert.equal(diag.historicalCompatibility.customerFacingRealityGapUnchanged, true);
  assert.equal(diag.dualRun.fixtureBaseline.mode, "RG_FIXTURE_BASELINE");
  assert.equal(diag.dualRun.bridgedEligible.mode, "RG_BRIDGED_ELIGIBLE_DIAGNOSTIC_V1");
});

check("identity isolation across pilots", () => {
  const suite = buildPilotSuiteDiagnostics({ observations: [] });
  assert.equal(suite.pilots.length, 2);
  assert.equal(suite.regression.length, 1);
  assert.ok(suite.isolation.every((row) => row.ok === true));
  const ids = new Set(suite.pilots.map((p) => p.identity.isolationKey));
  assert.equal(ids.size, 2);
  // YOTEL regression surfaces undefined attrs
  const yotel = suite.regression[0];
  assert.ok(yotel.eligibility.undefinedKeys.includes("airport_shuttle"));
});

check("suggestions never auto-approve and do not rewrite selection", () => {
  const profile = loadPropertyProfile("adp_bethesda_marriott");
  const sug = suggestAttributeCandidates(profile);
  assert.ok(sug.suggestionCount >= 0);
  assert.ok((sug.suggestions || []).every((s) => s.autoApproved === false));
  assert.ok(!(sug.suggestions || []).some((s) => profile.attributes.includes(s.attribute)));
});

check("selection inventory hash stable", () => {
  const a = hashSelectionInventory(["b", "a", "a"]);
  const b = hashSelectionInventory(["a", "b"]);
  assert.equal(a, b);
});

check("fixture RG path unchanged for empty observations", () => {
  const profile = loadPropertyProfile("adp_spice_island_beach_resort");
  const rg = computeRealityGap([], profile);
  assert.equal(rg.totalAttributes, 24);
});

check("Airtable selection proposal is design-only", () => {
  assert.equal(STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.productionWritesAuthorized, false);
  assert.equal(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.status,
    "PROPOSAL_AWAITING_FOUNDER_APPROVAL"
  );
});

if (failures) {
  console.error(`\n${failures} failure(s)`);
  process.exit(1);
}
console.log("\nAll Phase 1B verified-context bridge tests passed.");
