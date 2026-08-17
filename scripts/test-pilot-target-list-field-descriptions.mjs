/**
 * Unit tests for Pilot Target List field description map and dry-run planner.
 */
import assert from "node:assert/strict";
import {
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  PILOT_TARGET_LIST_DESCRIPTIONS_FOR_TABLE,
  PILOT_TARGET_LIST_FIELD_DESCRIPTIONS,
  hasMeaningfulDescription,
  planPilotTargetListDescriptionUpdates,
} from "../lib/gtm-owner-target/pilot-target-list-field-descriptions.js";

function testNoDuplicateKeys() {
  const keys = Object.keys(PILOT_TARGET_LIST_FIELD_DESCRIPTIONS);
  assert.equal(keys.length, new Set(keys).size);
}

function testDescriptionsNonEmpty() {
  for (const [fieldName, description] of Object.entries(PILOT_TARGET_LIST_FIELD_DESCRIPTIONS)) {
    assert.ok(String(description).trim(), `empty description for ${fieldName}`);
  }
}

function testRequiredOutreachFieldsCovered() {
  for (const fieldName of PILOT_TARGET_LIST_DESCRIPTIONS_FOR_TABLE) {
    assert.ok(
      PILOT_TARGET_LIST_FIELD_DESCRIPTIONS[fieldName],
      `missing description map for ${fieldName}`
    );
  }
}

function testPlannerSkipsExistingUnlessOverwrite() {
  const tableFields = [
    { id: "fld1", name: MAP_PILOT_TARGET_LIST.name, description: "Existing name help" },
    { id: "fld2", name: MAP_PILOT_TARGET_LIST.email, description: null },
  ];
  const plan = planPilotTargetListDescriptionUpdates(tableFields, { overwrite: false });
  assert.ok(plan.descriptionsAlreadyPresent.includes(MAP_PILOT_TARGET_LIST.name));
  assert.ok(plan.descriptionsToAdd.some((x) => x.fieldName === MAP_PILOT_TARGET_LIST.email));
  assert.equal(plan.descriptionsToOverwrite.length, 0);
}

function testPlannerOverwrite() {
  const tableFields = [
    { id: "fld1", name: MAP_PILOT_TARGET_LIST.name, description: "Old text" },
  ];
  const plan = planPilotTargetListDescriptionUpdates(tableFields, { overwrite: true });
  assert.ok(plan.descriptionsToOverwrite.some((x) => x.fieldName === MAP_PILOT_TARGET_LIST.name));
}

function testPlannerDryRunDoesNotMutateFields() {
  const tableFields = [
    { id: "fld1", name: MAP_PILOT_TARGET_LIST.email, description: null },
  ];
  const before = tableFields[0].description;
  planPilotTargetListDescriptionUpdates(tableFields, { overwrite: false });
  assert.equal(tableFields[0].description, before);
}

function testHasMeaningfulDescription() {
  assert.equal(hasMeaningfulDescription(null), false);
  assert.equal(hasMeaningfulDescription("  "), false);
  assert.equal(hasMeaningfulDescription("Help text"), true);
}

function run() {
  testNoDuplicateKeys();
  testDescriptionsNonEmpty();
  testRequiredOutreachFieldsCovered();
  testPlannerSkipsExistingUnlessOverwrite();
  testPlannerOverwrite();
  testPlannerDryRunDoesNotMutateFields();
  testHasMeaningfulDescription();
  console.log("test-pilot-target-list-field-descriptions: all passed");
}

run();
