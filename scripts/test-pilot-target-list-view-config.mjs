/**
 * Unit tests for Pilot Target List view configuration.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { MAP_PILOT_TARGET_LIST } from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  PILOT_TARGET_LIST_VIEW_CONFIGS,
  PILOT_TARGET_LIST_VIEW_NAMES,
  buildPilotTargetListViewsManualMarkdown,
  hydratePilotViewConfigs,
  planPilotTargetListViewSetup,
  resolveViewVisibleFields,
} from "../lib/gtm-owner-target/pilot-target-list-view-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function allTableFieldNames() {
  return Object.values(MAP_PILOT_TARGET_LIST).filter(
    (name, idx, arr) =>
      arr.indexOf(name) === idx &&
      name !== MAP_PILOT_TARGET_LIST.messageAngle &&
      name !== MAP_PILOT_TARGET_LIST.lastContactedDate
  );
}

function testFourViewsDefined() {
  assert.equal(PILOT_TARGET_LIST_VIEW_CONFIGS.length, 4);
  assert.equal(PILOT_TARGET_LIST_VIEW_NAMES.length, 4);
}

function testNoDuplicateViewNames() {
  assert.equal(PILOT_TARGET_LIST_VIEW_NAMES.length, new Set(PILOT_TARGET_LIST_VIEW_NAMES).size);
}

function testEachViewHasRequiredProperties() {
  for (const view of PILOT_TARGET_LIST_VIEW_CONFIGS) {
    assert.ok(view.name.trim(), "view name required");
    assert.ok(view.purpose.trim(), "view purpose required");
    assert.ok(view.visibleFieldKeys.length, "visibleFieldKeys required");
    assert.ok(view.filterFormula.trim(), "filterFormula required");
    assert.ok(view.sort.length, "sort required");
  }
}

function testVisibleFieldsResolveAgainstFieldMap() {
  const tableFields = allTableFieldNames();
  for (const view of hydratePilotViewConfigs(tableFields)) {
    assert.ok(view.visibleFields.length, `${view.name} should resolve visible fields`);
    assert.equal(view.missingRequiredFields.length, 0, `${view.name} missing required fields`);
    if (view.name === "Pilot Outreach Pipeline") {
      assert.ok(!view.visibleFields.includes(MAP_PILOT_TARGET_LIST.messageAngle));
    }
  }
}

function testOptionalMessageAngleOmittedWhenMissing() {
  const tableFields = allTableFieldNames();
  const pipeline = hydratePilotViewConfigs(tableFields).find(
    (v) => v.name === "Pilot Outreach Pipeline"
  );
  assert.ok(pipeline.omittedOptionalFields.includes(MAP_PILOT_TARGET_LIST.messageAngle));
}

function testPlanDetectsExistingViews() {
  const plan = planPilotTargetListViewSetup(
    [{ id: "viw1", name: "Pilot Outreach Pipeline", type: "grid" }],
    allTableFieldNames()
  );
  assert.equal(plan.viewsAlreadyPresent.length, 1);
  assert.equal(plan.viewsToCreate.length, 3);
}

function testManualReportGeneration() {
  const plan = planPilotTargetListViewSetup([], allTableFieldNames());
  const md = buildPilotTargetListViewsManualMarkdown(plan, {
    baseId: "appTEST",
    tableName: "Pilot Target List",
    tableId: "tblTEST",
  });
  assert.match(md, /Pilot Outreach Pipeline/);
  assert.match(md, /Drafting Queue/);
  assert.match(md, /Approved for Send \/ Mail Merge/);
  assert.match(md, /Follow-Up Needed/);
  assert.match(md, /Do Not Contact/);
}

function testDryRunPlannerDoesNotMutateViews() {
  const existing = [{ id: "viw1", name: "Grid view", type: "grid" }];
  const before = JSON.stringify(existing);
  planPilotTargetListViewSetup(existing, allTableFieldNames());
  assert.equal(JSON.stringify(existing), before);
}

function run() {
  testFourViewsDefined();
  testNoDuplicateViewNames();
  testEachViewHasRequiredProperties();
  testVisibleFieldsResolveAgainstFieldMap();
  testOptionalMessageAngleOmittedWhenMissing();
  testPlanDetectsExistingViews();
  testManualReportGeneration();
  testDryRunPlannerDoesNotMutateViews();
  console.log("test-pilot-target-list-view-config: all passed");
}

run();
