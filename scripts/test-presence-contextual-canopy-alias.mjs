#!/usr/bin/env node
/**
 * Contextual Canopy alias regression — synthetic NON-HOLDOUT fixtures only.
 * GLOBAL bare Canopy alias must NOT be added.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  findEntitySpans,
  CONTEXTUAL_ALIAS_RULES,
  RESOLVER_VERSION,
} from "../lib/ai-visibility/normalize-entities.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(ROOT, "fixtures/ai-visibility/contextual-canopy-regression.json");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

const doc = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const index = buildGoldenSetScoringEntityIndex({});

console.log("Contextual Canopy Alias Regression\n");
console.log(`resolver=${RESOLVER_VERSION}`);

test("no global bare Canopy alias in CONTEXTUAL rules as unrestricted alias", () => {
  assert.ok(CONTEXTUAL_ALIAS_RULES.some((r) => r.id === "canopy_by_hilton_contextual_v1"));
  const rule = CONTEXTUAL_ALIAS_RULES.find((r) => r.id === "canopy_by_hilton_contextual_v1");
  assert.equal(rule.surface, "Canopy");
  assert.equal(rule.requiredParentContext, "Hilton");
  // Ensure entity aliases do not gain unrestricted Canopy via this module contract
  const canopyEntity = (index.entities || []).find((e) => e.name === "Canopy by Hilton");
  assert.ok(canopyEntity, "Canopy by Hilton must exist in scoring universe");
  const bare = (canopyEntity.aliases || []).some(
    (a) => String(a).trim().toLowerCase() === "canopy"
  );
  assert.equal(bare, false, "GLOBAL_BARE_ALIAS_ADDED must be NO");
});

for (const row of doc.positive || []) {
  test(`POSITIVE ${row.id}`, () => {
    const spans = findEntitySpans(row.text, index.aliasIndex);
    const hit = spans.some((s) => s.entity?.name === row.expectEntity);
    assert.equal(hit, true, `expected ${row.expectEntity} in: ${row.text.slice(0, 80)}`);
    const contextual = spans.find(
      (s) => s.entity?.name === row.expectEntity && s.matchKind === "CONTEXTUAL_ALIAS"
    );
    // Full-name matches also OK; at least one of the positives should be contextual
    assert.ok(hit);
    if (contextual) {
      assert.equal(contextual.matchedAlias, "Canopy");
    }
  });
}

for (const row of doc.negative || []) {
  test(`NEGATIVE ${row.id}`, () => {
    const spans = findEntitySpans(row.text, index.aliasIndex);
    const hit = spans.some((s) => s.entity?.name === row.expectEntityAbsent);
    assert.equal(hit, false, `must not resolve ${row.expectEntityAbsent}`);
  });
}

console.log(`\nPOSITIVE_TEST_N=${(doc.positive || []).length}`);
console.log(`NEGATIVE_TEST_N=${(doc.negative || []).length}`);
console.log(`${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
