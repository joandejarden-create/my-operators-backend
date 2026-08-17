#!/usr/bin/env node
/**
 * Unit tests for Apify usage/cost tracking (no live Apify calls, no Airtable writes).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  APIFY_USE_CASES,
  APIFY_AUTH_METHODS,
  APIFY_COST_SOURCE,
  buildApifyUsageRecord,
  fromApifyRunPayload,
  extractApifyRunCost,
  summarizeApifyUsage,
  createApifyUsageStore,
  scrubSecrets,
  recordTripadvisorRoomCountRun,
} from "../lib/hotel-intelligence/apify-usage/index.js";

const sampleRun = {
  data: {
    id: "run_test_abc",
    actId: "dbEyMBriog95Fv8CW",
    actorName: "maxcopell/tripadvisor",
    startedAt: "2026-08-11T23:48:11.683Z",
    finishedAt: "2026-08-11T23:50:14.368Z",
    status: "SUCCEEDED",
    defaultDatasetId: "ds_test",
    stats: { computeUnits: 0.06807 },
    pricingInfo: { pricingModel: "PAY_PER_EVENT" },
    chargedEventCounts: { "result-scraped": 33, "apify-actor-start": 2 },
    usageTotalUsd: 0.1651,
    consoleUrl: "https://console.apify.com/view/runs/run_test_abc",
  },
};

const cost = extractApifyRunCost(sampleRun);
assert.equal(cost.apify_run_cost_usd, 0.1651);
assert.equal(cost.cost_source, APIFY_COST_SOURCE.APIFY_USAGE_TOTAL_USD);

const partial = fromApifyRunPayload(sampleRun);
assert.equal(partial.run_id, "run_test_abc");
assert.equal(partial.records_returned, 33);
assert.equal(partial.actor_name, "maxcopell/tripadvisor");

const row = buildApifyUsageRecord({
  use_case: APIFY_USE_CASES.ROOM_COUNT,
  apify_run: sampleRun,
  records_requested: 40,
  successful_matches: 20,
  successful_enrichments: 10,
  verified_enrichments: 2,
  auth_method: APIFY_AUTH_METHODS.MCP,
  label: "unit_test",
});

assert.equal(row.dealality_use_case, "ROOM_COUNT");
assert.equal(row.apify_run_cost_usd, 0.1651);
assert.equal(row.cost_per_returned_record, Number((0.1651 / 33).toFixed(8)));
assert.equal(row.cost_per_successful_enrichment, Number((0.1651 / 10).toFixed(8)));
assert.equal(row.cost_per_verified_enrichment, Number((0.1651 / 2).toFixed(8)));
assert.equal(row.production_writes, false);
assert.equal(row.authoritative_hotel_data, false);

assert.throws(
  () => buildApifyUsageRecord({ use_case: "NOT_A_REAL_CASE" }),
  /Invalid Apify use case/
);

const scrubbed = scrubSecrets({
  run_id: "x",
  APIFY_TOKEN: "secret",
  token: "nope",
  notes: "ok",
});
assert.equal(scrubbed.APIFY_TOKEN, undefined);
assert.equal(scrubbed.token, undefined);
assert.equal(scrubbed.notes, "ok");

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "apify-usage-"));
const store = createApifyUsageStore({ root: tmp });
const a = recordTripadvisorRoomCountRun(store, {
  apify_run: sampleRun,
  records_requested: 40,
  successful_matches: 20,
  successful_enrichments: 10,
  verified_enrichments: 2,
  label: "unit_test",
});
assert.equal(a.created, true);
const b = recordTripadvisorRoomCountRun(store, {
  apify_run: sampleRun,
  verified_enrichments: 3,
  label: "unit_test_update",
});
assert.equal(b.created, false);
assert.equal(b.row.verified_enrichments, 3);

const sum = summarizeApifyUsage(store.list());
assert.equal(sum.runs, 1);
assert.equal(sum.total_apify_run_cost_usd, 0.1651);

for (const uc of Object.keys(APIFY_USE_CASES)) {
  assert.equal(APIFY_USE_CASES[uc], uc);
}

console.log("ok - apify usage tracking normalize + ledger");
