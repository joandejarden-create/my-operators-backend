#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  classifyContractCurrentStatus,
  isCommerciallyFutureWindow,
  CONTRACT_CURRENT_STATUS,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v3-status-v1.js";
import {
  listV3SecApiQueryPlan,
  adaptWeightsFromBatch,
  isPriorWaveAccession,
  V3_SEC_API_QUERY_FAMILIES,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v3-query-plan-v1.js";
import {
  loadV3Checkpoint,
  saveV3Checkpoint,
  markAccessionProcessed,
  isAccessionProcessed,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v3-checkpoint-v1.js";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

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

test("future_window_2027_2035", () => {
  assert.equal(isCommerciallyFutureWindow("2033-01-31"), true);
  assert.equal(isCommerciallyFutureWindow("2026-10-31"), false);
  assert.equal(isCommerciallyFutureWindow(2029), true);
});

test("status_expired_unconfirmed_without_later_evidence", () => {
  const s = classifyContractCurrentStatus(
    { primaryDate: "2020-05-31", dateMeaning: "FRANCHISE_EXPIRATION" },
    { now: "2026-11-20", laterEvidence: [] },
  );
  assert.equal(s.status, CONTRACT_CURRENT_STATUS.EXPIRED_STATUS_UNCONFIRMED);
});

test("status_superseded_on_renewal_evidence", () => {
  const s = classifyContractCurrentStatus(
    { primaryDate: "2029-06-22", dateMeaning: "FRANCHISE_EXPIRATION" },
    {
      now: "2026-11-20",
      laterEvidence: [{ kind: "AMENDMENT_OR_RENEWAL", primaryDate: "2034-01-01" }],
    },
  );
  assert.equal(s.status, CONTRACT_CURRENT_STATUS.SUPERSEDED);
});

test("status_silence_is_not_corroboration_for_future", () => {
  const s = classifyContractCurrentStatus(
    { primaryDate: "2033-01-31", dateMeaning: "FRANCHISE_EXPIRATION" },
    { now: "2026-11-20", laterEvidence: [] },
  );
  assert.equal(s.status, CONTRACT_CURRENT_STATUS.HISTORICALLY_VERIFIED);
  assert.notEqual(s.status, CONTRACT_CURRENT_STATUS.CURRENTLY_CORROBORATED);
});

test("query_plan_covers_franchise_and_management", () => {
  const blob = V3_SEC_API_QUERY_FAMILIES.map((q) => q.query).join(" ");
  assert.match(blob, /Franchise Expiration Date/);
  assert.match(blob, /Management Agreement Expiration/);
  assert.match(blob, /Hotel Management Agreement/);
  assert.match(blob, /Hotel Collateral|Mortgaged Property/);
  const plan = listV3SecApiQueryPlan();
  assert.ok(plan.length > V3_SEC_API_QUERY_FAMILIES.length);
  const franchise = V3_SEC_API_QUERY_FAMILIES.find((q) => q.id === "franchise_expiration_date");
  assert.ok(franchise.formTypesPreferred.every((f) => /^424B/.test(f)));
});

test("adaptive_weights_boost_and_floor_exploration", () => {
  const next = adaptWeightsFromBatch({}, { yieldByQueryFamily: { franchise_expiration_date: 5 } });
  assert.ok(next.franchise_expiration_date > 1);
  assert.ok(next.hotel_management_agreement_ex10 >= 0.35);
});

test("prior_wave_accession_exclusion", () => {
  assert.equal(isPriorWaveAccession("0001539497-20-000579"), true);
  assert.equal(isPriorWaveAccession("0001539497-18-000207"), true);
  assert.equal(isPriorWaveAccession("0001539497-99-000001"), false);
});

test("checkpoint_persists_accessions", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ctim-v3-"));
  const p = path.join(dir, "cp.json");
  let cp = loadV3Checkpoint(p);
  markAccessionProcessed(cp, "0001539497-18-000999", { ok: true });
  saveV3Checkpoint(cp, p);
  cp = loadV3Checkpoint(p);
  assert.equal(isAccessionProcessed(cp, "0001539497-18-000999"), true);
});

console.log(`\n${passed} tests passed`);
