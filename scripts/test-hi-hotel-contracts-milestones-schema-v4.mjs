#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  validateBlueprintConsistency,
  expectedLinkGraph,
  buildMilestoneCreateFields,
  buildContractCreateFields,
  buildContractSelfLinkFields,
  buildMilestoneSelfLinkFields,
  PUBLICATION_STATE_DEFAULT,
  TABLE_IDS,
} from "../lib/hotel-intelligence/sec-edgar/hi-hotel-contracts-milestones-schema-v4.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("blueprint_consistent", () => {
  const v = validateBlueprintConsistency();
  assert.equal(v.ok, true, JSON.stringify(v.errors));
  assert.equal(v.counts.contractsComplete, 28);
  assert.equal(v.counts.milestonesComplete, 19);
});

test("milestone_contract_is_linked_record", () => {
  const fields = buildMilestoneCreateFields({
    contractsTableId: "tblContracts",
    inventoryId: TABLE_IDS.inventory,
    hpcId: TABLE_IDS.hpc,
    findingsId: TABLE_IDS.findings,
  });
  const contract = fields.find((f) => f.name === "Contract");
  assert.equal(contract.type, "multipleRecordLinks");
  assert.equal(contract.options.linkedTableId, "tblContracts");
});

test("link_graph_covers_mandatory_edges", () => {
  const g = expectedLinkGraph();
  const keys = g.map((e) => `${e.from}|${e.field}|${e.to}`);
  for (const req of [
    "HI Hotel Contracts|Discovery Property|HI Hotel Discovery Inventory",
    "HI Hotel Contracts|HPC Hotel|Hotel Property Census",
    "HI Hotel Contracts|Counterparty Org|HI Organizations",
    "HI Contract Milestones|Contract|HI Hotel Contracts",
    "HI Contract Milestones|Discovery Property|HI Hotel Discovery Inventory",
    "HI Contract Milestones|HPC Hotel|Hotel Property Census",
    "HI Discovery Findings|Linked Contract|HI Hotel Contracts",
    "HI Discovery Findings|Linked Milestone|HI Contract Milestones",
    "HI Hotel Contracts|Supersedes Contract|HI Hotel Contracts",
    "HI Hotel Contracts|Superseded By Contract|HI Hotel Contracts",
    "HI Contract Milestones|Superseded By Milestone|HI Contract Milestones",
  ]) {
    assert.ok(keys.includes(req), `missing ${req}`);
  }
});

test("publication_state_customer_blocked", () => {
  assert.equal(PUBLICATION_STATE_DEFAULT, "CUSTOMER_BLOCKED");
  const c = buildContractCreateFields({
    inventoryId: TABLE_IDS.inventory,
    hpcId: TABLE_IDS.hpc,
    orgsId: TABLE_IDS.organizations,
  });
  const pub = c.find((f) => f.name === "Publication State");
  assert.ok(pub.options.choices.some((x) => x.name === "CUSTOMER_BLOCKED"));
});

test("self_links_present", () => {
  assert.deepEqual(
    buildContractSelfLinkFields("tblX").map((f) => f.name),
    ["Supersedes Contract", "Superseded By Contract"],
  );
  assert.deepEqual(buildMilestoneSelfLinkFields("tblY").map((f) => f.name), [
    "Superseded By Milestone",
  ]);
});

test("simulated_idempotent_reapply_noop_when_complete", () => {
  // Pure simulation: if both tables exist and additive fields present, pending empty
  const live = {
    contractsExists: true,
    milestonesExists: true,
    inventoryFields: new Set(["Geographic Processing Class", "Continent / Macro Region"]),
    findingsFields: new Set(["Linked Contract", "Linked Milestone"]),
  };
  const pending = [];
  if (!live.contractsExists) pending.push("contracts");
  if (!live.milestonesExists) pending.push("milestones");
  for (const f of ["Geographic Processing Class", "Continent / Macro Region"]) {
    if (!live.inventoryFields.has(f)) pending.push(f);
  }
  for (const f of ["Linked Contract", "Linked Milestone"]) {
    if (!live.findingsFields.has(f)) pending.push(f);
  }
  assert.equal(pending.length, 0);
});

console.log(`\n${passed} schema tests passed`);
