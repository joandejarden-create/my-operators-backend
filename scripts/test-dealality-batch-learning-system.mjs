/**
 * Regression tests for Dealality batch learning system contracts.
 *
 *   npm run test:dealality-batch-learning-system
 */

import assert from "node:assert/strict";
import {
  CLASSIFICATION_TYPES,
  LEARNING_SYSTEM_VERSION,
  STATUS,
  BATCH_LEARNING_CHECKLIST,
  WEBHOUND_HARD_CASE_RULES,
  buildSeedLearningEntries,
  buildLedgerDocument,
  validateLearningEntry,
  validateLedger,
  runBatchLearningAudit,
} from "../lib/data-intelligence/dealality-batch-learning-system.js";
import {
  matchesRejectedPin,
  isValidCoordPair,
} from "../lib/research-engine-v2/production-census-coordinate-extractor.js";
import {
  resolveGeocodingProvider,
  isStreetLevelAddress,
  buildOfficialGeocodeQuery,
  GEOCODING_PROVIDERS,
} from "../lib/research-engine-v2/production-census-geocoding-providers.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`[PASS] ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`[FAIL] ${name}:`, err?.message || err);
  }
}

check("seed entries validate", () => {
  const entries = buildSeedLearningEntries();
  assert.ok(entries.length >= 15, "expected seeded learnings");
  for (const [i, e] of entries.entries()) {
    const v = validateLearningEntry(e, i);
    assert.equal(v.ok, true, v.errors.join("; "));
  }
});

check("ledger validates", () => {
  const ledger = buildLedgerDocument();
  const v = validateLedger(ledger);
  assert.equal(v.ok, true, v.errors.join("; "));
  assert.equal(ledger.version, LEARNING_SYSTEM_VERSION);
  assert.equal(ledger.airtable_writes, false);
  assert.equal(ledger.brand_explorer_patches, false);
});

check("classification vocabulary complete", () => {
  for (const t of [
    "learned_code_rule",
    "learned_validation_rule",
    "learned_source_pattern",
    "learned_block_reason",
    "steward_review_case",
    "Webhound_candidate",
    "do_not_learn_noise",
  ]) {
    assert.ok(CLASSIFICATION_TYPES.includes(t), t);
  }
  assert.equal(BATCH_LEARNING_CHECKLIST.length, 10);
  assert.equal(WEBHOUND_HARD_CASE_RULES.may_write_airtable, false);
  assert.equal(WEBHOUND_HARD_CASE_RULES.sample_size_normal.max, 25);
});

check("census + be learnings both present", () => {
  const entries = buildSeedLearningEntries();
  assert.ok(entries.some((e) => e.process === "census"));
  assert.ok(entries.some((e) => e.process === "brand_explorer"));
  assert.ok(entries.some((e) => e.id.includes("mapbox") || e.id.includes("provider")));
  assert.ok(entries.some((e) => e.id.includes("forbidden-language")));
  assert.ok(entries.some((e) => e.issue_type === "Webhound_candidate"));
});

check("reject-pin locality cue learning", () => {
  assert.equal(isValidCoordPair(19.43, -99.1345), true);
  // Airport / Centro hotels must not false-flag as tourism centroid
  assert.equal(
    matchesRejectedPin(19.4366, -99.0759, { propertyName: "Hilton México City Airport" }),
    null
  );
  assert.equal(
    matchesRejectedPin(19.431213, -99.135644, {
      propertyName: "Umbral, Curio Collection by Hilton",
    }),
    null
  );
  assert.ok(matchesRejectedPin(0, 0));
});

check("geocode query rejects city-only", () => {
  assert.equal(isStreetLevelAddress("Cancun"), false);
  assert.equal(
    buildOfficialGeocodeQuery({
      name: "Marriott Cancun",
      address: "Cancun",
      city: "Cancun",
      country: "Mexico",
    }),
    null
  );
  assert.ok(
    buildOfficialGeocodeQuery({
      name: "Hotel Indigo Tijuana Downtown",
      address: "Calle Salvador Diaz Miron 4ta-8177",
      city: "Tijuana",
      country: "Mexico",
    })
  );
  assert.deepEqual([...GEOCODING_PROVIDERS].sort(), ["google", "mapbox", "none"].sort());
  const none = resolveGeocodingProvider("none");
  assert.equal(none.provider, "none");
});

check("audit runs without writes", () => {
  const audit = runBatchLearningAudit(buildLedgerDocument());
  assert.equal(audit.airtable_writes, false);
  assert.equal(audit.brand_explorer_patches, false);
  assert.ok(Object.values(STATUS).includes(audit.status));
  assert.ok(audit.last_census_batch || audit.summary);
});

if (failed) {
  console.error(`\n${failed} test(s) failed`);
  process.exit(1);
}
console.log("\nAll dealality-batch-learning-system tests passed.");
