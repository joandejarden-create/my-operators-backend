#!/usr/bin/env node
/**
 * Permanent BE content standards — contract integrity + Gate 2 block.
 * Does NOT regenerate brand content.
 */
import assert from "assert";
import {
  loadPermanentContentStandards,
  loadValidationMatrix,
  validateStandardsContractIntegrity,
  assertGate2Allowed,
  textViolatesEditorialStandards,
  getFixedCollection,
  BE_PERMANENT_CONTENT_STANDARDS_V1,
} from "../lib/brand-explorer/be-permanent-content-standards-v1.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (e) {
    failed += 1;
    console.error("FAIL", name, e.message || e);
  }
}

check("contract_loads", () => {
  const c = loadPermanentContentStandards({ reload: true });
  assert.strictEqual(c.version, BE_PERMANENT_CONTENT_STANDARDS_V1);
  assert.strictEqual(c.forbidBrandSpecificExceptions, true);
  assert.strictEqual(c.forbidCurioOrAscendHardcoding, true);
});

check("matrix_covers_founder_requirements", () => {
  const m = loadValidationMatrix({ reload: true });
  assert.ok((m.rows || []).length >= 12);
  assert.strictEqual(m.gate2ContentReconstruction, "BLOCKED_UNTIL_GATE1_APPROVED");
});

check("contract_cardinality_integrity", () => {
  const r = validateStandardsContractIntegrity();
  assert.strictEqual(r.ok, true, JSON.stringify(r.failures));
  assert.strictEqual(r.gate1FounderApproved, false);
});

check("lifecycle_and_opening_titles", () => {
  const life = getFixedCollection("supportAcrossTheLifecycle");
  assert.deepStrictEqual(life.orderedCanonicalTitles, [
    "Evaluation",
    "Conversion Design",
    "Pre-Opening",
    "Opening",
    "Ramp-Up",
    "Ongoing",
  ]);
  const open = getFixedCollection("openingAndConversionPath");
  assert.deepStrictEqual(open.orderedCanonicalTitles, [
    "Application & Feasibility",
    "Design & Standards",
    "Pre-Opening Planning",
    "Opening Support",
    "Stabilization",
  ]);
  assert.strictEqual(getFixedCollection("loyaltyKeyBenefits").count, 6);
  assert.strictEqual(getFixedCollection("imageGallery").count, 6);
});

check("editorial_detects_em_dash_and_placeholders", () => {
  assert.ok(textViolatesEditorialStandards("Owner value — confirm with brand development").includes("em_dash"));
  assert.ok(textViolatesEditorialStandards("Benefit 1 for guests").some((h) => /Benefit/i.test(h)));
  assert.strictEqual(textViolatesEditorialStandards("Clear commercial guidance for owners.").length, 0);
});

check("gate2_blocked_until_gate1_approved", () => {
  let blocked = false;
  try {
    assertGate2Allowed();
  } catch (e) {
    blocked = e.code === "GATE2_BLOCKED_UNTIL_GATE1";
  }
  assert.strictEqual(blocked, true);
});

if (failed) {
  console.error(`\n${failed} permanent standards check(s) failed`);
  process.exit(1);
}
console.log("\nPermanent content standards contract integrity OK — Gate 2 remains blocked");
