#!/usr/bin/env node
/**
 * Market Demand Intelligence MVP tests.
 *   node scripts/test-market-demand.mjs
 */
import "../load-env.js";
import {
  normalizeDemandCenterRecord,
  normalizeMarketDemandSnapshotRecord,
  normalizeNearbyHotelSupplyRecord,
} from "../lib/market-demand/normalize-market-demand.js";
import {
  calculateCategoryScores,
  deriveOverallDemandStrength,
  buildSnapshotAirtableFields,
  summarizeDemandCenters,
} from "../lib/market-demand/scoring.js";
import { SAMPLE_IMPORT_PAYLOAD } from "../lib/market-demand/sample-import-payload.js";
import { MARKET_DEMAND_DEAL_RECORD_ID_FIELD } from "../lib/market-demand/airtable-market-demand-fields.js";
import {
  getDealDemandCenters,
  getDealMarketDemandSnapshot,
  postGenerateMarketDemandSnapshot,
} from "../api/market-demand.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    status(c) {
      out.statusCode = c;
      return this;
    },
    json(b) {
      out.body = b;
      return this;
    },
    out,
  };
}

function testNormalizeMissingFields() {
  const dc = normalizeDemandCenterRecord({ id: "recDC1", fields: {} });
  assert(dc.id === "recDC1", "demand center id preserved");
  assert(dc.name === "", "missing name → empty string");
  assert(Array.isArray(dc.linkedDealIds) && dc.linkedDealIds.length === 0, "missing links → []");
  assert(dc.relevanceScore === null, "missing relevance → null");

  const snap = normalizeMarketDemandSnapshotRecord(null);
  assert(snap.id === "", "null snapshot → empty id");
  assert(snap.scores.leisure === null, "null snapshot scores null");

  const hotel = normalizeNearbyHotelSupplyRecord({ id: "recH1", fields: { "Unknown Column": 200 } });
  assert(hotel.rooms === null, "unknown field does not populate rooms");
}

function testCategorySummary() {
  const centers = [
    normalizeDemandCenterRecord({
      id: "rec1",
      fields: {
        "Demand Category": "Transportation",
        "Relevance Score": 90,
        "Data Confidence": "High",
      },
    }),
    normalizeDemandCenterRecord({
      id: "rec2",
      fields: {
        "Demand Category": "Leisure",
        "Relevance Score": 70,
        "Data Confidence": "Medium",
      },
    }),
  ];
  const summary = summarizeDemandCenters(centers);
  assert(summary.totalDemandCenters === 2, "summary count");
  assert(summary.categories.Transportation === 1, "category bucket");
  assert(summary.topCategories.length <= 3, "top categories capped at 3");
}

function testScoring() {
  const centers = SAMPLE_IMPORT_PAYLOAD.demandCenters.map((item, i) =>
    normalizeDemandCenterRecord({
      id: "recS" + i,
      fields: {
        "Demand Center Name": item.name,
        "Demand Category": item.category,
        "Demand Strength": item.demandStrength,
        "Data Confidence": item.dataConfidence,
        "Relevance Score": item.relevanceScore,
      },
    })
  );
  const { scores } = calculateCategoryScores(centers);
  assert(scores.transportation > 0, "transportation score > 0");
  assert(scores.leisure > 0, "leisure score > 0");
  const overall = deriveOverallDemandStrength(centers.length, scores);
  assert(["Strong", "Moderate", "Limited", "Unclear"].includes(overall), "overall strength enum");
  const built = buildSnapshotAirtableFields({
    dealId: "recTESTdeal",
    demandCenters: centers,
    nearbyHotels: [],
  });
  assert(built.normalized.demandSummary.length > 20, "deterministic summary generated");
  assert(built.fields[MARKET_DEMAND_DEAL_RECORD_ID_FIELD] === "recTESTdeal", "snapshot stores deal record id");
}

function testNormalizeDealRecordId() {
  const dc = normalizeDemandCenterRecord({
    id: "recDC",
    fields: { "Deal Record ID": "recDEAL1", "Demand Center Name": "Test" },
  });
  assert(dc.linkedDealIds[0] === "recDEAL1", "normalize reads Deal Record ID text field");
}

function testApiMissingConfig() {
  const prevBase = process.env.AIRTABLE_BASE_ID;
  const prevAlt = process.env.AIRTABLE_BASE_ID_ALT;
  const prevKey = process.env.AIRTABLE_API_KEY;
  delete process.env.AIRTABLE_BASE_ID;
  delete process.env.AIRTABLE_BASE_ID_ALT;
  delete process.env.AIRTABLE_API_KEY;

  const req = { params: { dealId: "recTESTmissing" } };
  const res = mockRes();
  return getDealDemandCenters(req, res).then(() => {
    assert(res.out.statusCode === 503, "missing config → 503");
    assert(res.out.body?.error === "airtable_config_missing", "airtable_config_missing error code");
    if (prevBase) process.env.AIRTABLE_BASE_ID = prevBase;
    if (prevAlt) process.env.AIRTABLE_BASE_ID_ALT = prevAlt;
    if (prevKey) process.env.AIRTABLE_API_KEY = prevKey;
  });
}

function testApiMissingDealId() {
  const req = { params: { dealId: "" } };
  const res = mockRes();
  return getDealDemandCenters(req, res).then(() => {
    assert(res.out.statusCode === 400, "missing deal id → 400");
    assert(res.out.body?.error === "missing_deal_id", "missing_deal_id error");
  });
}

function testNoSnapshotShape() {
  const snap = normalizeMarketDemandSnapshotRecord(null);
  const payload = { ok: true, dealId: "recX", snapshot: snap.id ? snap : null, hasSnapshot: false };
  assert(payload.hasSnapshot === false, "hasSnapshot false when no record");
  assert(payload.snapshot === null, "snapshot null when empty");
}

async function main() {
  testNormalizeMissingFields();
  testCategorySummary();
  testScoring();
  testNormalizeDealRecordId();
  testNoSnapshotShape();
  await testApiMissingDealId();
  await testApiMissingConfig();

  // Handler exports exist
  assert(typeof getDealMarketDemandSnapshot === "function", "getDealMarketDemandSnapshot exported");
  assert(typeof postGenerateMarketDemandSnapshot === "function", "postGenerateMarketDemandSnapshot exported");

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll market demand tests passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
