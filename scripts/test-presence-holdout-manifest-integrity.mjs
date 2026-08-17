#!/usr/bin/env node
/**
 * Future holdout seal integrity — duplicate manifests must DO_NOT_SEAL.
 * Also proves the Holdout v2 double-bucket selection bug class is fixed.
 */
import assert from "node:assert/strict";
import {
  validateHoldoutManifestIntegrity,
  resolvePresenceSelectionLabel,
  dedupeHoldoutSelectionByCaseId,
} from "../lib/ai-visibility/validation/holdout-manifest-integrity.js";
import { selectHoldoutV2WithResponseGovernance } from "../lib/ai-visibility/validation/presence-validation-pool-governance.js";

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

console.log("Presence Holdout Manifest Integrity\n");

test("duplicate caseId => DO_NOT_SEAL", () => {
  const v = validateHoldoutManifestIntegrity([
    {
      caseId: "presval_dup",
      canonicalEntityId: "recA",
      sourceResponseId: "resp1",
    },
    {
      caseId: "presval_dup",
      canonicalEntityId: "recA",
      sourceResponseId: "resp1",
    },
  ]);
  assert.equal(v.ok, false);
  assert.equal(v.DO_NOT_SEAL, true);
  assert.deepEqual(v.duplicateCaseIds, ["presval_dup"]);
  assert.equal(v.UNIQUE_CASE_ID_COUNT, 1);
  assert.equal(v.PAIR_N, 2);
});

test("duplicate entity-response pair => DO_NOT_SEAL", () => {
  const v = validateHoldoutManifestIntegrity([
    {
      caseId: "a",
      canonicalEntityId: "recA",
      sourceResponseId: "resp1",
    },
    {
      caseId: "b",
      canonicalEntityId: "recA",
      sourceResponseId: "resp1",
    },
  ]);
  assert.equal(v.ok, false);
  assert.equal(v.DO_NOT_SEAL, true);
  assert.ok(v.duplicateEntityResponsePairs.includes("recA::resp1"));
});

test("unique rows seal-eligible", () => {
  const v = validateHoldoutManifestIntegrity([
    {
      caseId: "a",
      canonicalEntityId: "recA",
      sourceResponseId: "resp1",
    },
    {
      caseId: "b",
      canonicalEntityId: "recB",
      sourceResponseId: "resp2",
    },
  ]);
  assert.equal(v.ok, true);
  assert.equal(v.DO_NOT_SEAL, false);
  assert.equal(v.UNIQUE_CASE_ID_COUNT, 2);
  assert.equal(v.UNIQUE_ENTITY_RESPONSE_PAIR_COUNT, 2);
  assert.equal(v.NO_DUPLICATE_MANIFEST_ROWS, true);
});

test("human label wins over candidateType (v2 bug class)", () => {
  assert.equal(
    resolvePresenceSelectionLabel({
      humanLabel: "PRESENT",
      candidateType: "PRESENCE_FALSE",
    }),
    "PRESENT"
  );
  assert.equal(
    resolvePresenceSelectionLabel({
      humanLabel: "NOT_PRESENT",
      candidateType: "PRESENCE_TRUE",
    }),
    "NOT_PRESENT"
  );
});

test("selection does not double-count CHANGED PRESENCE_FALSE→PRESENT row", () => {
  const eligible = [
    {
      caseId: "presval_changed",
      sourceResponseId: "resp_x",
      humanLabel: "PRESENT",
      candidateType: "PRESENCE_FALSE",
      canonicalEntityId: "recCanopy",
      provider: "claude",
      language: "en",
      geography: "North America",
    },
    {
      caseId: "presval_other",
      sourceResponseId: "resp_y",
      humanLabel: "NOT_PRESENT",
      candidateType: "PRESENCE_FALSE",
      canonicalEntityId: "recOther",
      provider: "claude",
      language: "en",
      geography: "North America",
    },
  ];
  // Pad with unique pairs so selection can fill small design
  for (let i = 0; i < 10; i++) {
    eligible.push({
      caseId: `presval_p_${i}`,
      sourceResponseId: `resp_p_${i}`,
      humanLabel: "PRESENT",
      candidateType: "PRESENCE_TRUE",
      canonicalEntityId: `recP${i}`,
      provider: "openai",
      language: "en",
      geography: "Global",
    });
    eligible.push({
      caseId: `presval_n_${i}`,
      sourceResponseId: `resp_n_${i}`,
      humanLabel: "NOT_PRESENT",
      candidateType: "PRESENCE_FALSE",
      canonicalEntityId: `recN${i}`,
      provider: "openai",
      language: "en",
      geography: "Global",
    });
  }
  const sel = selectHoldoutV2WithResponseGovernance(eligible, {
    TOTAL_N: 8,
    PRESENCE_TRUE_N: 5,
    PRESENCE_FALSE_N: 3,
    CANDIDATE_CAP_PER_RESPONSE: 2,
  });
  const ids = sel.selected.map((c) => c.caseId);
  assert.equal(ids.filter((id) => id === "presval_changed").length, 1);
  assert.equal(new Set(ids).size, ids.length);
  assert.equal(sel.SELECTION_INTEGRITY_OK, true);
  assert.equal(sel.manifestIntegrity.ok, true);
});

test("dedupeHoldoutSelectionByCaseId keeps first", () => {
  const out = dedupeHoldoutSelectionByCaseId([
    { caseId: "a", n: 1 },
    { caseId: "a", n: 2 },
    { caseId: "b", n: 3 },
  ]);
  assert.equal(out.length, 2);
  assert.equal(out[0].n, 1);
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
