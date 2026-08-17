/**
 * Unit tests — Full CALA shell orchestrator guards + allowlist regressions.
 * No Airtable writes.
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  SHELL_PREFLIGHT_CLASS,
  MATCH,
  classifyShellPreflightQuality,
  COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT,
} from "../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js";
import {
  assertLockedCensusTable,
  assertNoProtectedShellFields,
  buildSafeAllowlistForCountry,
  ORCHESTRATOR_STATUS,
} from "../lib/research-engine-v2/full-cala-15k-shell-orchestrator-v1.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

function safeHbx(over = {}) {
  return {
    candidate_id: over.candidate_id || "hbx_900001",
    property_name: over.property_name || "Hotel Test Plaza",
    normalized_property_name: "hotel test plaza",
    country: over.country || "Costa Rica",
    city: over.city || "San José",
    address: over.address || "1 Main St",
    website: over.website || "https://example.com",
    phone: over.phone || "+50622221111",
    source_type: over.source_type || "hbx_content_api",
    merged_sources: over.merged_sources || ["hbx_content_api"],
    match_class: over.match_class || MATCH.NEW_HIGH,
    external_ids: { hbx_code: over.hbx_code ?? 900001 },
    brand_text: over.brand_text || null,
    chain_text: over.chain_text || null,
    ...over,
  };
}

function weakCvent(over = {}) {
  const name = over.property_name || "Casa Blanca Suites Norte";
  return {
    candidate_id: over.candidate_id || "cvent_weak_1",
    property_name: name,
    normalized_property_name:
      over.normalized_property_name || name.toLowerCase(),
    country: over.country || "Mexico",
    city: "",
    address: "",
    website: "",
    phone: "",
    source_type: "cvent_candidate",
    merged_sources: ["cvent_candidate"],
    match_class: MATCH.NEW_MEDIUM,
    external_ids: {},
    ...over,
  };
}

test("locked table ID assertion accepts production Census only", () => {
  assert.doesNotThrow(() =>
    assertLockedCensusTable(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)
  );
  assert.throws(
    () => assertLockedCensusTable("tblWRONG"),
    /target_table_id_mismatch/
  );
});

test("protected-field assertion fails closed", () => {
  assert.doesNotThrow(() =>
    assertNoProtectedShellFields({
      "Property Name": "Hotel X",
      Country: "Panama",
      "HBX Hotel Code": "123",
    })
  );
  assert.throws(
    () => assertNoProtectedShellFields({ "Rooms / Keys": 100 }),
    /protected_field_proposed/
  );
  assert.throws(
    () => assertNoProtectedShellFields({ "Current Brand": "Hilton" }),
    /protected_field_proposed/
  );
  assert.throws(
    () => assertNoProtectedShellFields({ "Brand Family": "Hilton" }),
    /protected_field_proposed/
  );
});

test("SAFE HBX allowlist includes; weak/REVIEW excluded; no fill-with-weak", () => {
  const pool = [
    safeHbx({ candidate_id: "hbx_1", hbx_code: 1, property_name: "Hotel Alpha" }),
    safeHbx({
      candidate_id: "hbx_2",
      hbx_code: 2,
      property_name: "Hotel Beta",
      city: "",
    }), // HBX missing city → REVIEW
    weakCvent({ candidate_id: "cvent_weak_mx" }),
    safeHbx({
      candidate_id: "cvent_safe_no_hbx",
      hbx_code: null,
      external_ids: {},
      source_type: "cvent_candidate",
      merged_sources: ["cvent_candidate"],
      property_name: "Hotel Cvent Strong",
      city: "Cancún",
      address: "Blvd Kukulcan",
      website: "https://hotelcvent.example",
    }),
  ];
  // Force REVIEW on missing city HBX
  const reviewClass = classifyShellPreflightQuality(pool[1], {
    cventOnlyQualityGate: true,
  });
  assert.equal(reviewClass.class, SHELL_PREFLIGHT_CLASS.REVIEW);

  const weakClass = classifyShellPreflightQuality(pool[2], {
    cventOnlyQualityGate: true,
  });
  assert.equal(weakClass.class, SHELL_PREFLIGHT_CLASS.WEAK);
  assert.match(weakClass.reason, /cvent_only_missing_city|weak/);

  const built = buildSafeAllowlistForCountry(pool, { maxInserts: 500 });
  assert.equal(built.planned.length, 1);
  assert.equal(built.planned[0].candidate_id, "hbx_1");
  assert.equal(built.heldWeak >= 1, true);
  assert.equal(built.heldReview >= 1, true);
  assert.equal(
    built.planned.every((p) => p.hbx_hotel_code != null),
    true
  );
  assert.equal(
    built.planned.every((p) => p.preflight_class === SHELL_PREFLIGHT_CLASS.SAFE),
    true
  );
});

test("within-plan HBX dedupe prevents duplicate codes", () => {
  const pool = [
    safeHbx({ candidate_id: "a", hbx_code: 55, property_name: "Hotel One" }),
    safeHbx({ candidate_id: "b", hbx_code: 55, property_name: "Hotel One Dup" }),
  ];
  const built = buildSafeAllowlistForCountry(pool, { maxInserts: 500 });
  assert.equal(built.planned.length, 1);
  assert.equal(built.skippedHbx, 1);
});

test("Colombia Batch 2 allowlist artifact fingerprint still matches approved baseline", () => {
  const fpPath = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-15k-colombia-batch-2-allowlist.json"
  );
  assert.equal(fs.existsSync(fpPath), true);
  const artifact = JSON.parse(fs.readFileSync(fpPath, "utf8"));
  const expected = COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT;
  assert.equal(artifact.fingerprint.count, expected.expected_insert_count);
  assert.equal(artifact.fingerprint.hbx_only, expected.hbx_only);
  assert.equal(artifact.fingerprint.cvent_plus_hbx, expected.cvent_plus_hbx);
  assert.equal(artifact.fingerprint.cvent_only, 0);
  assert.equal(
    artifact.fingerprint.first_candidate_id,
    expected.first_candidate_id
  );
  assert.equal(artifact.fingerprint.all_have_hbx, true);
  assert.equal(artifact.fingerprint.all_safe, true);
  assert.equal(artifact.material_drift, false);
  assert.equal(artifact.records.length, 293);
});

test("Mexico/Colombia weak identity remain AUTO_HOLD (not SAFE allowlist)", () => {
  const mexicoWeak = Array.from({ length: 20 }, (_, i) =>
    weakCvent({
      candidate_id: `cvent_mx_weak_${i}`,
      country: "Mexico",
      property_name: `Casa Azul Residences ${i}`,
      normalized_property_name: `casa azul residences ${i}`,
    })
  );
  const colombiaWeak = Array.from({ length: 10 }, (_, i) =>
    weakCvent({
      candidate_id: `cvent_co_weak_${i}`,
      country: "Colombia",
      property_name: `Torre Verde Apartments ${i}`,
      normalized_property_name: `torre verde apartments ${i}`,
    })
  );
  const mx = buildSafeAllowlistForCountry(mexicoWeak, { maxInserts: 500 });
  const co = buildSafeAllowlistForCountry(colombiaWeak, { maxInserts: 500 });
  assert.equal(mx.planned.length, 0);
  assert.equal(co.planned.length, 0);
  assert.equal(mx.heldWeak + mx.heldReview + mx.heldOther, mexicoWeak.length);
  assert.equal(co.heldWeak + co.heldReview + co.heldOther, colombiaWeak.length);
  assert.ok(mx.heldWeak + mx.heldOther >= 15);
  assert.ok(co.heldWeak + co.heldOther >= 8);
});

test("batch size is a ceiling — smaller pools are not padded", () => {
  const pool = [
    safeHbx({
      candidate_id: "h1",
      hbx_code: 11,
      property_name: "Hotel Alpha CR",
      normalized_property_name: "hotel alpha cr",
    }),
    safeHbx({
      candidate_id: "h2",
      hbx_code: 12,
      property_name: "Hotel Beta CR",
      normalized_property_name: "hotel beta cr",
    }),
  ];
  const built = buildSafeAllowlistForCountry(pool, { maxInserts: 500 });
  assert.equal(built.planned.length, 2);
});

test("orchestrator status constants include exhausted + founder stop", () => {
  assert.match(
    ORCHESTRATOR_STATUS.EXHAUSTED,
    /universe_exhausted_pending_enrichment/
  );
  assert.match(
    ORCHESTRATOR_STATUS.FOUNDER_STOP,
    /stop_for_founder_review/
  );
});

test("resume does not recreate applied allowlist identity (applied filter contract)", () => {
  const appliedCandidates = new Set(["hbx_1"]);
  const appliedHbx = new Set([1]);
  const pool = [
    safeHbx({ candidate_id: "hbx_1", hbx_code: 1 }),
    safeHbx({ candidate_id: "hbx_2", hbx_code: 2, property_name: "Hotel Next" }),
  ];
  const remaining = pool.filter((c) => {
    if (appliedCandidates.has(c.candidate_id)) return false;
    const code = Number(c.external_ids?.hbx_code);
    if (appliedHbx.has(code)) return false;
    return true;
  });
  const built = buildSafeAllowlistForCountry(remaining, { maxInserts: 500 });
  assert.equal(built.planned.length, 1);
  assert.equal(built.planned[0].candidate_id, "hbx_2");
});
