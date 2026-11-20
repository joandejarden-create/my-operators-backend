#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GRAPH_EVIDENCE_REUSE_DECISION,
  TABLE_HI_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  LIFECYCLE_STATUS_OPTIONS,
  assertCanonicalDiscoveryBase,
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_BASE_ID_FORBIDDEN,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

test("graph_evidence_not_reused_for_unmatched", () => {
  assert.equal(GRAPH_EVIDENCE_REUSE_DECISION.decision, "DO_NOT_REUSE_FOR_UNMATCHED");
  assert.equal(GRAPH_EVIDENCE_REUSE_DECISION.createInstead, TABLE_HI_DISCOVERY_FINDINGS);
  assert.ok(GRAPH_EVIDENCE_REUSE_DECISION.reasons.length >= 3);
});

test("canonical_base_gate", () => {
  assert.equal(assertCanonicalDiscoveryBase(GRAPH_BASE_ID_CANONICAL).ok, true);
  assert.equal(assertCanonicalDiscoveryBase(GRAPH_BASE_ID_FORBIDDEN).ok, false);
});

test("lifecycle_separates_discovery_from_verified_intelligence", () => {
  assert.ok(LIFECYCLE_STATUS_OPTIONS.includes("DISCOVERY_PERSISTED"));
  assert.ok(LIFECYCLE_STATUS_OPTIONS.includes("IDENTITY_VERIFIED"));
  assert.ok(LIFECYCLE_STATUS_OPTIONS.includes("PERSISTED_VERIFIED"));
  assert.notEqual(
    MAP_DISCOVERY_INVENTORY.lifecycleStatus,
    MAP_DISCOVERY_INVENTORY.identityClass,
  );
});

test("required_inventory_and_findings_field_maps", () => {
  for (const k of [
    "discoveryPropertyId",
    "hotelNameNormalized",
    "historicalAliases",
    "identityClass",
    "secRegistrant",
    "hpcHotel",
    "lifecycleStatus",
    "sourceDocumentLedger",
  ]) {
    assert.ok(MAP_DISCOVERY_INVENTORY[k], k);
  }
  for (const k of [
    "findingId",
    "discoveryProperty",
    "findingType",
    "fieldName",
    "valueObserved",
    "historicalApplicability",
    "hpcPromotionStatus",
  ]) {
    assert.ok(MAP_DISCOVERY_FINDINGS[k], k);
  }
});

test("generated_table_ids_present_after_apply", () => {
  const p = path.join(
    ROOT,
    "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json",
  );
  assert.ok(fs.existsSync(p), "generated ids file missing — run ensure --apply");
  const ids = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.equal(ids.baseId, GRAPH_BASE_ID_CANONICAL);
  assert.ok(ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY]?.startsWith("tbl"));
  assert.ok(ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS]?.startsWith("tbl"));
});

test("migration_apply_report_counts", () => {
  const p = path.join(
    ROOT,
    "reports/sec-edgar-v23-discovery-inventory/MIGRATION_APPLY.json",
  );
  assert.ok(fs.existsSync(p));
  const r = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.equal(r.summary.verifiedImported, 18);
  assert.equal(r.summary.probableImported, 71);
  assert.equal(r.summary.readBacksFailed, 0);
  assert.ok(r.summary.findingsPersisted >= 1);
  assert.equal(r.summary.hpcFieldsEnriched, 0);
});

test("city_bleed_quarantine_applied", () => {
  const p = path.join(
    ROOT,
    "reports/sec-edgar-v23-discovery-inventory/QUALITY_QUARANTINE_APPLY.json",
  );
  assert.ok(fs.existsSync(p));
  const r = JSON.parse(fs.readFileSync(p, "utf8"));
  assert.ok(r.cityBleedCount >= 1);
  assert.equal(r.mode, "APPLY");
});

console.log(`${passed} tests passed`);
