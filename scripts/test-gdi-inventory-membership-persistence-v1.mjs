#!/usr/bin/env node
/**
 * $0 fixture tests — inventory membership persistence contract.
 * No Airtable writes. No schema changes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
  MEMBERSHIP_STATUS,
  CHANGE_CLASS,
  buildInventoryMembershipRecord,
  deriveMembershipFromPublications,
  writeDurableMembershipLedger,
  loadDurableMembershipLedger,
  planInventoryMembershipAirtableWrites,
  filterRowsByStagedMembership,
  evaluateMembershipPersistenceReadiness,
  PROPOSED_GDO_MEMBERSHIP_FIELDS,
} from "../lib/group-demand-intelligence/persistence/gdi-inventory-membership-persistence-v1.js";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`PASS ${name}`);
}

check("contract_ids", () => {
  assert.equal(
    GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
    "gdi-inventory-membership-persistence-v1",
  );
  assert.ok(PROPOSED_GDO_MEMBERSHIP_FIELDS.length >= 3);
});

check("build_record_defaults", () => {
  const r = buildInventoryMembershipRecord({
    hotelId: "recHotel",
    opportunityId: "opp1",
    membershipStatus: MEMBERSHIP_STATUS.STAGED,
    stagedPublicationVersion: "gdi_staged_v1",
  });
  assert.equal(r.hotelId, "recHotel");
  assert.equal(r.opportunityId, "opp1");
  assert.equal(r.membershipStatus, MEMBERSHIP_STATUS.STAGED);
  assert.equal(r.changeClass, CHANGE_CLASS.RETAINED);
});

check("derive_membership_separates_research_staged_current", () => {
  const ledger = deriveMembershipFromPublications({
    hotelId: "recHotel",
    stagedPackage: {
      publicationVersion: "staged_v1",
      inventoryIds: ["a", "b"],
      opportunities: [
        { id: "a", inventoryPopulationPrimary: "SALES_OPPORTUNITIES" },
        { id: "b", inventoryPopulationPrimary: "WATCHLIST" },
      ],
    },
    currentPackage: {
      publicationVersion: "current_v1",
      inventoryIds: ["a"],
      opportunities: [{ id: "a" }],
    },
    researchOpportunities: [
      { id: "a" },
      { id: "b" },
      { id: "c" },
    ],
  });
  assert.equal(ledger.counts.total, 3);
  assert.equal(ledger.counts.current, 1);
  assert.equal(ledger.counts.researchOnly, 1);
  const c = ledger.rows.find((r) => r.opportunityId === "c");
  assert.equal(c.membershipStatus, MEMBERSHIP_STATUS.RESEARCH_ONLY);
  const a = ledger.rows.find((r) => r.opportunityId === "a");
  assert.equal(a.membershipStatus, MEMBERSHIP_STATUS.CURRENT);
  assert.equal(a.currentPublicationVersion, "current_v1");
  assert.equal(a.stagedPublicationVersion, "staged_v1");
});

check("durable_ledger_dry_run_and_apply_to_temp", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-mem-"));
  const ledger = deriveMembershipFromPublications({
    hotelId: "recTmp",
    stagedPackage: {
      publicationVersion: "s1",
      inventoryIds: ["x"],
      opportunities: [{ id: "x" }],
    },
    researchOpportunities: [{ id: "x" }, { id: "y" }],
  });
  const dry = writeDurableMembershipLedger(ledger, { root: tmp, dryRun: true });
  assert.equal(dry.dryRun, true);
  assert.equal(fs.existsSync(dry.path), false);
  const applied = writeDurableMembershipLedger(ledger, {
    root: tmp,
    dryRun: false,
  });
  assert.equal(applied.dryRun, false);
  const loaded = loadDurableMembershipLedger("recTmp", tmp);
  assert.equal(loaded.rows.length, 2);
});

check("airtable_plan_fails_closed_without_schema_auth", () => {
  const ledger = deriveMembershipFromPublications({
    hotelId: "recHotel",
    stagedPackage: {
      publicationVersion: "s1",
      inventoryIds: ["a"],
      opportunities: [{ id: "a" }],
    },
  });
  const plan = planInventoryMembershipAirtableWrites(ledger);
  assert.equal(plan.dryRun, true);
  assert.equal(plan.airtableWrites, false);
  assert.equal(plan.schemaAuthorized, false);
  assert.match(plan.note, /SCHEMA_AUTHORIZATION_REQUIRED/);
  assert.equal(plan.patches[0].mode, "PAYLOAD_EMBED_UNTIL_SCHEMA_AUTH");
});

check("filter_rows_by_staged_membership", () => {
  const ledger = deriveMembershipFromPublications({
    hotelId: "recHotel",
    stagedPackage: {
      publicationVersion: "s1",
      inventoryIds: ["a", "b"],
      opportunities: [{ id: "a" }, { id: "b" }],
    },
    researchOpportunities: [{ id: "a" }, { id: "b" }, { id: "c" }],
  });
  const filtered = filterRowsByStagedMembership(
    [{ id: "a" }, { id: "b" }, { id: "c" }],
    ledger,
    "s1",
  );
  assert.equal(filtered.length, 2);
  assert.deepEqual(
    filtered.map((r) => r.id).sort(),
    ["a", "b"],
  );
});

check("readiness_reports_airtable_gap", () => {
  const ledger = deriveMembershipFromPublications({
    hotelId: "recHotel",
    stagedPackage: {
      publicationVersion: "s1",
      inventoryIds: ["a"],
      opportunities: [{ id: "a" }],
    },
  });
  const ev = evaluateMembershipPersistenceReadiness({
    membershipLedger: ledger,
    airtableRows: [{ id: "a" }, { id: "extra" }],
    stagedPackage: {
      inventoryIds: ["a"],
      opportunities: [{ id: "a" }],
    },
  });
  assert.equal(ev.status, "PARTIAL");
  assert.ok(
    ev.corrections.some((c) => c.code === "INVENTORY_MEMBERSHIP_NOT_PERSISTED"),
  );
});

console.log(`\nOK ${passed} checks — gdi-inventory-membership-persistence-v1`);
