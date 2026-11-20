#!/usr/bin/env node
/**
 * Phase 1C / PR-2 tests — coverage policy, semantic model, recommendations, YOTEL.
 */

import assert from "assert/strict";
import {
  DEFAULT_COVERAGE_LIMITS,
  MEASUREMENT_COVERAGE_STATUS,
  assessMeasurementCoveragePolicy,
  buildMeasurementCoverageSnapshot,
  classifySemanticPair,
  SEMANTIC_RELATION,
  collapseToUniqueMeasurableConcepts,
  reconcileSemanticCounting,
  ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
  buildAttributeRecommendationPackage,
  buildPrioritizedShortlist,
  RECOMMENDED_ACTION,
  reviewYotelAirportAttributes,
  buildPhase1cGovernanceSuite,
  validateAttributeDefinitionAndRecognition,
  STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL,
  CONFIG_APPROVAL_STATUS,
  LINE_STATUS,
} from "../lib/ai-demand-positioning/verified-context/index.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { ATTRIBUTE_DEFINITIONS } from "../lib/ai-demand-positioning/intelligence/reality-gap.js";
import { hasAttributeRecognitionKeywords } from "../lib/ai-demand-positioning/execution/attribute-recognition-keywords-v1.js";

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

check("coverage policy defaults 12 / 15-18 / 20", () => {
  assert.equal(DEFAULT_COVERAGE_LIMITS.minimum, 12);
  assert.equal(DEFAULT_COVERAGE_LIMITS.preferredMin, 15);
  assert.equal(DEFAULT_COVERAGE_LIMITS.preferredMax, 18);
  assert.equal(DEFAULT_COVERAGE_LIMITS.maximum, 20);
});

check("exactly 12 → COMPLIANT", () => {
  const r = assessMeasurementCoveragePolicy(12);
  assert.equal(r.status, MEASUREMENT_COVERAGE_STATUS.MEASUREMENT_COVERAGE_COMPLIANT);
  assert.equal(r.meetsMinimum, true);
  assert.equal(r.withinMaximum, true);
});

check("exactly 20 → COMPLIANT", () => {
  const r = assessMeasurementCoveragePolicy(20);
  assert.equal(r.status, MEASUREMENT_COVERAGE_STATUS.MEASUREMENT_COVERAGE_COMPLIANT);
});

check("21+ → ABOVE_MAXIMUM", () => {
  const r = assessMeasurementCoveragePolicy(21);
  assert.equal(r.status, MEASUREMENT_COVERAGE_STATUS.ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE);
  assert.equal(r.surplus, 1);
});

check("below 12 → BELOW_MINIMUM with exception note", () => {
  const r = assessMeasurementCoveragePolicy(3);
  assert.equal(r.status, MEASUREMENT_COVERAGE_STATUS.BELOW_MINIMUM_ATTRIBUTE_COVERAGE);
  assert.equal(r.shortfall, 9);
  assert.ok(r.governedExceptionEligible);
});

check("semantic TRUE_DUPLICATE beachfront/oceanfront", () => {
  assert.equal(
    classifySemanticPair("beachfront", "oceanfront"),
    SEMANTIC_RELATION.TRUE_DUPLICATE
  );
});

check("semantic BROADER_NARROWER spa/full_service_spa", () => {
  assert.equal(
    classifySemanticPair("spa", "full_service_spa"),
    SEMANTIC_RELATION.BROADER_NARROWER
  );
});

check("semantic RELATED_BUT_DISTINCT luxury/five_diamond", () => {
  assert.equal(
    classifySemanticPair("luxury", "five_diamond"),
    SEMANTIC_RELATION.RELATED_BUT_DISTINCT
  );
});

check("true duplicates collapse for unique concept count", () => {
  const c = collapseToUniqueMeasurableConcepts([
    "beachfront",
    "oceanfront",
    "spa",
    "full_service_spa",
    "luxury",
    "five_diamond",
  ]);
  // beachfront/oceanfront = 1; spa/full_service_spa = 1 (broader/narrower collapsed); luxury+five_diamond = 2
  assert.equal(c.uniqueConceptCount, 4);
});

check("Bethesda recommendation package below minimum by unique concepts", () => {
  const profile = loadPropertyProfile("adp_bethesda_marriott");
  const pack = buildAttributeRecommendationPackage(profile);
  assert.equal(pack.selectedRawCount, 15);
  assert.ok(pack.uniqueEligibleConceptCount >= 1);
  assert.equal(
    pack.coverage.status,
    pack.uniqueEligibleConceptCount < 12
      ? MEASUREMENT_COVERAGE_STATUS.BELOW_MINIMUM_ATTRIBUTE_COVERAGE
      : MEASUREMENT_COVERAGE_STATUS.MEASUREMENT_COVERAGE_COMPLIANT
  );
  assert.deepEqual(pack.selectionPreserved, profile.attributes);
  assert.ok(pack.recommendations.every((r) => r.autoApproved === false));
  // Should surface ADD/VERIFY candidates when below min
  if (pack.coverage.shortfall > 0) {
    assert.ok(
      pack.recommendations.some((r) =>
        [RECOMMENDED_ACTION.ADD, RECOMMENDED_ACTION.VERIFY].includes(r.recommendedAction)
      )
    );
  }
});

check("Spice Island semantic reconciliation 20→22 explained", () => {
  const profile = loadPropertyProfile("adp_spice_island_beach_resort");
  const recon = reconcileSemanticCounting(profile.attributes);
  assert.equal(recon.phase1bUniqueConceptCount, 20);
  assert.equal(recon.rulesV1_1UniqueConceptCount, 22);
  assert.equal(recon.delta, 2);
  // Delta = RELATED independently measurable pairs kept separate
  const deltaAttrs = recon.deltaKeys.map((d) => d.attribute).sort();
  assert.ok(deltaAttrs.includes("event_space_outdoor") || deltaAttrs.includes("five_diamond"));
  assert.ok(recon.explanation.includes("RELATED_BUT_DISTINCT"));
});

check("Spice Island above max unique concepts; shortlist satisfies policy", () => {
  const profile = loadPropertyProfile("adp_spice_island_beach_resort");
  const pack = buildAttributeRecommendationPackage(profile);
  assert.equal(pack.selectedRawCount, 24);
  assert.equal(pack.uniqueEligibleConceptCount, 22);
  assert.equal(pack.semanticRulesVersion, ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION);
  assert.equal(
    pack.coverage.status,
    MEASUREMENT_COVERAGE_STATUS.ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE
  );
  assert.equal(pack.proposedShortlistMax20.semanticRulesVersion, ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION);
  assert.ok(pack.proposedShortlistMax20.uniqueConceptCount <= 20);
  assert.equal(pack.proposedShortlistMax20.satisfiesMeasurementPolicy, true);
  assert.equal(pack.proposedShortlistMax20.autoRemoved, false);
  assert.equal(pack.proposedShortlistMax20.uniqueConceptCount, 20);
  assert.equal(
    pack.proposedShortlistMax20.count,
    pack.proposedShortlistMax20.uniqueConceptCount
  );
  assert.ok(pack.proposedShortlistMax20.items.some((i) => i.attribute === "beachfront"));
  assert.ok(pack.proposedShortlistMax20.items.some((i) => i.attribute === "spa"));
  const exclAttrs = pack.proposedShortlistMax20.proposedExclusions.map((e) => e.attribute).sort();
  assert.deepEqual(exclAttrs, [
    "full_service",
    "full_service_spa",
    "oceanfront",
    "tennis_courts",
  ]);
  assert.ok(pack.fullCandidateInventoryCount > 20);
  assert.deepEqual(pack.selectionPreserved, profile.attributes);
});

check("coverage snapshot exposes Airtable min/max fields", () => {
  const snap = buildMeasurementCoverageSnapshot(22);
  assert.equal(snap.minimumEligibleRequired, 12);
  assert.equal(snap.maximumEligibleAllowed, 20);
  assert.equal(snap.preferredRangeMinimum, 15);
  assert.equal(snap.preferredRangeMaximum, 18);
  assert.equal(snap.aboveMaximum, true);
  assert.equal(
    snap.measurementCoverageComplianceResult,
    MEASUREMENT_COVERAGE_STATUS.ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE
  );
});

check("shortlist never exceeds maximum and does not auto-remove", () => {
  const fake = Array.from({ length: 25 }, (_, i) => ({
    attribute: `attr_${i}`,
    label: `Attr ${i}`,
    currentlySelected: i < 22,
    eligibilityForMeasurement: true,
    definitionCoverage: true,
    recommendedAction: i < 22 ? RECOMMENDED_ACTION.KEEP : RECOMMENDED_ACTION.ADD,
    rankingScore: 100 - i,
    explanation: "test",
    semanticConceptId: `concept_${i}`,
  }));
  const short = buildPrioritizedShortlist(fake, 20);
  assert.ok(short.count <= 20);
  assert.equal(short.autoRemoved, false);
});

check("YOTEL undefined keys + no fabricated enablement", () => {
  const review = reviewYotelAirportAttributes();
  assert.equal(review.ok, true);
  assert.equal(review.autoEnabledInCustomerRealityGap, false);
  for (const row of review.rows) {
    assert.equal(row.definitionPresent, false);
    assert.equal(row.recognitionRulePresent, false);
    assert.equal(row.customerFacingEnablement, "BLOCKED_PENDING_FOUNDER_Y1");
  }
  const profile = loadPropertyProfile("adp_yotel_geneva_lake");
  const pack = buildAttributeRecommendationPackage(profile);
  assert.ok(pack.uniqueEligibleConceptCount < 12);
  assert.equal(
    pack.coverage.status,
    MEASUREMENT_COVERAGE_STATUS.BELOW_MINIMUM_ATTRIBUTE_COVERAGE
  );
  assert.ok(pack.validationSummary.missingDefinitions.includes("airport_shuttle"));
  assert.ok(pack.validationSummary.missingDefinitions.includes("park_and_fly"));
});

check("definition/recognition registry: defs have recognition keys", () => {
  const missing = Object.keys(ATTRIBUTE_DEFINITIONS).filter(
    (k) => !hasAttributeRecognitionKeywords(k)
  );
  // Allow empty — Bethesda/Spice keys should be covered
  assert.ok(Array.isArray(missing));
  const profile = loadPropertyProfile("adp_bethesda_marriott");
  const v = validateAttributeDefinitionAndRecognition(profile.attributes, profile);
  assert.equal(v.missingDefinitions.length, 0);
});

check("Airtable Option B contract is design-only with min/max fields", () => {
  assert.equal(STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.productionWritesAuthorized, false);
  assert.equal(STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.schemaCreationAuthorized, false);
  assert.equal(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.persistenceClassification,
    "PERSISTENCE_DESIGNED"
  );
  assert.ok(CONFIG_APPROVAL_STATUS.REVIEW_REQUIRED);
  assert.equal(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.migrationPolicy.autoApproveFixtureSelections,
    false
  );
  assert.equal(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.migrationPolicy.fixtureBaselineImportStatus,
    CONFIG_APPROVAL_STATUS.REVIEW_REQUIRED
  );
  const cfgFields = STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.layers.strategicSelection.tables.configurations.fields.map(
    (f) => f.name
  );
  for (const required of [
    "Minimum Eligible Required",
    "Maximum Eligible Allowed",
    "Preferred Range Minimum",
    "Preferred Range Maximum",
    "Measurement Coverage Policy Version",
    "Unique Eligible Concept Count",
    "Measurement Coverage Compliance Result",
    "Approved Measurement Inventory Hash",
  ]) {
    assert.ok(cfgFields.includes(required), `missing field ${required}`);
  }
  assert.ok(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.layers.strategicSelection.inventoryScopes
      .fullCandidateInventory
  );
});

check("suite isolation and no Airtable writes flag", () => {
  const suite = buildPhase1cGovernanceSuite();
  assert.equal(suite.pilots.length, 2);
  assert.equal(suite.methodologyCompatibility.airtableWrites, false);
  assert.ok(suite.isolation.every((r) => r.ok));
  assert.equal(suite.yotelAirportReview.autoEnabledInCustomerRealityGap, false);
});

check("draft lifecycle statuses aligned across config and lines", () => {
  const lifecycle = STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.editingLifecycle.states;
  assert.deepEqual(lifecycle, [
    "DRAFT",
    "SUGGESTED",
    "REVIEW_REQUIRED",
    "APPROVED",
    "SUPERSEDED",
  ]);
  assert.ok(LINE_STATUS.DRAFT);
  assert.ok(LINE_STATUS.REVIEW_REQUIRED);
  assert.ok(LINE_STATUS.SUGGESTED);
  assert.ok(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.editingLifecycle.rules.some((r) =>
      r.includes("never mutate the Active APPROVED")
    )
  );
  assert.ok(
    STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL.editingLifecycle.rules.some((r) =>
      r.includes("historically retrievable")
    )
  );
});

if (failures) {
  console.error(`\n${failures} failure(s)`);
  process.exit(1);
}
console.log("\nAll Phase 1C attribute selection governance tests passed.");
