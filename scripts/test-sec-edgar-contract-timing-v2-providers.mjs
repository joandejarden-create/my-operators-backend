#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  createContractTimingV2Budget,
  CONTRACT_TIMING_V2_PAID_CEILING_USD,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import {
  listSecApiQueryPlan,
  isBaselineAccession,
  BASELINE_JPMCC_ACCESSION,
  SEC_API_QUERY_FAMILIES,
  EXA_QUERY_FAMILIES,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-query-families-v1.js";
import { loadSecApiIoConfig } from "../lib/hotel-intelligence/sec-edgar/sec-api-io-client-v1.js";
import { loadExaContractTimingConfig } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
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

test("paid_ceiling_25", () => {
  assert.equal(CONTRACT_TIMING_V2_PAID_CEILING_USD, 25);
});

test("budget_stops_at_ceiling", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ctim-v2-"));
  const b = createContractTimingV2Budget({
    ceilingUsd: 0.01,
    ledgerPath: path.join(dir, "ledger.json"),
  });
  const ok = b.record({ provider: "EXA", operation: "search", usd: 0.005 });
  assert.equal(ok.ok, true);
  const blocked = b.record({ provider: "EXA", operation: "search", usd: 0.01 });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.error, "CONTRACT_TIMING_V2_PAID_CEILING");
});

test("baseline_accession_excluded_helper", () => {
  assert.equal(isBaselineAccession(BASELINE_JPMCC_ACCESSION), true);
  assert.equal(isBaselineAccession("0001539497-20-000579"), true);
  assert.equal(isBaselineAccession("0001539497-21-000001"), false);
});

test("query_families_cover_required_phrases", () => {
  const blob = SEC_API_QUERY_FAMILIES.map((q) => q.query).join(" ");
  assert.match(blob, /Franchise Expiration Date/);
  assert.match(blob, /Management Agreement Expiration/);
  assert.match(blob, /Hotel Management Agreement/);
  assert.match(blob, /License Agreement Expiration/);
  assert.ok(EXA_QUERY_FAMILIES.length >= 4);
  const plan = listSecApiQueryPlan();
  assert.ok(plan.length > SEC_API_QUERY_FAMILIES.length);
});

test("configs_do_not_embed_secrets", () => {
  const sec = loadSecApiIoConfig({ SEC_API_IO_KEY: "secret-should-not-leak-in-export" });
  const exa = loadExaContractTimingConfig({ EXA_API_KEY: "exa-secret-should-not-leak" });
  assert.equal(sec.apiKeyPresent, true);
  assert.equal(exa.apiKeyPresent, true);
  assert.equal(sec.apiKeyLen > 0, true);
  // ensure raw key not a property of returned config
  assert.equal("apiKey" in sec, false);
});

console.log(`\n${passed} tests passed`);
