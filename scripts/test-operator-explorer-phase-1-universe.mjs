import test from "node:test";
import assert from "node:assert/strict";
import {
  TEST_FIXTURE_MASTER_IDS,
  recordPurposeForMasterId,
  filterProductionUniverse,
  assertNoTestFixturesInProductionList,
  RECORD_PURPOSE,
} from "../lib/operator-explorer/phase-1-universe.js";

test("nine test fixtures mapped", () => {
  assert.equal(TEST_FIXTURE_MASTER_IDS.length, 9);
  for (const id of TEST_FIXTURE_MASTER_IDS) {
    assert.equal(recordPurposeForMasterId(id), RECORD_PURPOSE.TEST_FIXTURE);
  }
});

test("production universe excludes fixtures", () => {
  const masters = [
    { id: "recF5Z87OAqFgndoq", fields: { "Record Purpose": "Production", company_name: "Arbor" } },
    { id: "recTUjuDxL96yWcQA", fields: { "Record Purpose": "Test Fixture", company_name: "Antillano" } },
  ];
  const prod = filterProductionUniverse(masters);
  assert.equal(prod.length, 1);
  assert.equal(prod[0].id, "recF5Z87OAqFgndoq");
  assert.doesNotThrow(() => assertNoTestFixturesInProductionList(prod));
  assert.throws(() => assertNoTestFixturesInProductionList(masters));
});
