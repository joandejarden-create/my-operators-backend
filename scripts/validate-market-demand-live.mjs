#!/usr/bin/env node
/**
 * Live validation for Market Demand Intelligence MVP.
 *   node scripts/validate-market-demand-live.mjs [dealId]
 */
import "dotenv/config";
import Airtable from "airtable";
import {
  MARKETS_TABLE,
  DEMAND_CENTERS_TABLE,
  DEMAND_CATEGORIES_TABLE,
  NEARBY_HOTEL_SUPPLY_TABLE,
  MARKET_DEMAND_SNAPSHOTS_TABLE,
  MARKET_FIELDS,
  DEMAND_CENTER_FIELDS,
  DEMAND_CATEGORY_FIELDS,
  NEARBY_HOTEL_SUPPLY_FIELDS,
  MARKET_DEMAND_SNAPSHOT_FIELDS,
  DEALS_MARKET_DEMAND_FIELDS,
  MARKET_DEMAND_DEAL_RECORD_ID_FIELD,
  DEALS_TABLE,
} from "../lib/market-demand/airtable-market-demand-fields.js";
import {
  getMarketDemandBaseId,
  getDealsBaseId,
} from "../lib/market-demand/market-demand-base.js";
import {
  getDealDemandCenters,
  getDealNearbyHotelSupply,
  getDealMarketDemandSnapshot,
  postGenerateMarketDemandSnapshot,
} from "../api/market-demand.js";

const PLATFORM_EXPECTED = {
  [MARKETS_TABLE]: Object.values(MARKET_FIELDS),
  [DEMAND_CENTERS_TABLE]: Object.values(DEMAND_CENTER_FIELDS).filter(
    (f) => f !== DEMAND_CENTER_FIELDS.linkedDeals
  ),
  [DEMAND_CATEGORIES_TABLE]: Object.values(DEMAND_CATEGORY_FIELDS),
  [NEARBY_HOTEL_SUPPLY_TABLE]: Object.values(NEARBY_HOTEL_SUPPLY_FIELDS).filter(
    (f) => f !== NEARBY_HOTEL_SUPPLY_FIELDS.linkedDeal
  ),
  [MARKET_DEMAND_SNAPSHOTS_TABLE]: Object.values(MARKET_DEMAND_SNAPSHOT_FIELDS).filter(
    (f) => f !== MARKET_DEMAND_SNAPSHOT_FIELDS.linkedDeal
  ),
};

const DEALS_OPTIONAL_EXPECTED = Object.values(DEALS_MARKET_DEMAND_FIELDS).filter(
  (f) => f !== DEALS_MARKET_DEMAND_FIELDS.linkedMarket
);

const FIELD_TYPE_EXPECTATIONS = {
  [MARKET_FIELDS.latitude]: ["number"],
  [MARKET_FIELDS.longitude]: ["number"],
  [DEMAND_CENTER_FIELDS.latitude]: ["number"],
  [DEMAND_CENTER_FIELDS.longitude]: ["number"],
  [DEMAND_CENTER_FIELDS.distanceFromDeal]: ["number"],
  [DEMAND_CENTER_FIELDS.estimatedDriveTime]: ["number"],
  [DEMAND_CENTER_FIELDS.relevanceScore]: ["number"],
  [NEARBY_HOTEL_SUPPLY_FIELDS.rooms]: ["number"],
  [NEARBY_HOTEL_SUPPLY_FIELDS.distanceFromDeal]: ["number"],
  [NEARBY_HOTEL_SUPPLY_FIELDS.estimatedDriveTime]: ["number"],
  [MARKET_DEMAND_SNAPSHOT_FIELDS.leisureDemandScore]: ["number"],
  [DEALS_MARKET_DEMAND_FIELDS.demandCenterCount]: ["number"],
  [DEMAND_CENTER_FIELDS.linkedMarket]: ["multipleRecordLinks", "singleRecordLink"],
  [MARKET_DEMAND_DEAL_RECORD_ID_FIELD]: ["singleLineText"],
  [NEARBY_HOTEL_SUPPLY_FIELDS.linkedMarket]: ["multipleRecordLinks", "singleRecordLink"],
  [MARKET_DEMAND_SNAPSHOT_FIELDS.linkedMarket]: ["multipleRecordLinks", "singleRecordLink"],
  [DEALS_MARKET_DEMAND_FIELDS.linkedMarketRecordId]: ["singleLineText"],
  [DEMAND_CENTER_FIELDS.demandPattern]: ["multipleSelects", "singleSelect"],
  [DEMAND_CENTER_FIELDS.relevantHotelTypes]: ["multipleSelects", "singleSelect"],
  [DEMAND_CENTER_FIELDS.source]: ["multipleSelects", "singleSelect", "singleLineText"],
};

const EXPECTED_SELECT_HINTS = {
  [DEMAND_CENTER_FIELDS.demandStrength]: ["High", "Medium", "Low"],
  [DEMAND_CENTER_FIELDS.dataConfidence]: ["High", "Medium", "Low"],
  [MARKET_DEMAND_SNAPSHOT_FIELDS.overallDemandStrength]: ["Strong", "Moderate", "Limited", "Unclear"],
};

async function fetchMetaTables(baseId, apiKey) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Meta API ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

function analyzeTable(tableMeta, expectedFields) {
  const fieldMap = new Map((tableMeta?.fields || []).map((f) => [f.name, f]));
  const present = [];
  const missing = [];
  const typeMismatches = [];
  const linkIssues = [];
  const selectIssues = [];

  for (const name of expectedFields) {
    const f = fieldMap.get(name);
    if (!f) {
      missing.push(name);
      continue;
    }
    present.push(name);

    const expectedTypes = FIELD_TYPE_EXPECTATIONS[name];
    if (expectedTypes && !expectedTypes.includes(f.type)) {
      typeMismatches.push({ field: name, expected: expectedTypes.join("|"), actual: f.type });
    }

    if (f.type === "multipleRecordLinks" || f.type === "singleRecordLink") {
      const opts = f.options || {};
      if (!opts.linkedTableId) {
        linkIssues.push({ field: name, issue: "no linkedTableId in field options" });
      }
    }

    if (f.type === "singleSelect" || f.type === "multipleSelects") {
      const hints = EXPECTED_SELECT_HINTS[name];
      if (hints) {
        const choices = (f.options?.choices || []).map((c) => c.name);
        const missingOpts = hints.filter((h) => !choices.some((c) => c.toLowerCase() === h.toLowerCase()));
        if (missingOpts.length) {
          selectIssues.push({ field: name, missingOptions: missingOpts, actualChoices: choices });
        }
      }
    }
  }

  return { present, missing, typeMismatches, linkIssues, selectIssues, recordCount: null };
}

async function testTableRead(base, tableName) {
  try {
    const rows = await base(tableName).select({ maxRecords: 3 }).firstPage();
    return { ok: true, sampleCount: rows.length, error: null };
  } catch (err) {
    return { ok: false, sampleCount: 0, error: err.message || String(err) };
  }
}

async function findSeededDeal(platformBase, dealsBase) {
  const argDeal = process.argv[2];
  if (argDeal && argDeal.startsWith("rec")) return argDeal;

  try {
    const dcRows = await platformBase(DEMAND_CENTERS_TABLE).select({ maxRecords: 50 }).all();
    for (const r of dcRows) {
      const textId = r.fields?.[MARKET_DEMAND_DEAL_RECORD_ID_FIELD];
      if (typeof textId === "string" && textId.startsWith("rec")) return textId;
      const links = r.fields?.[DEMAND_CENTER_FIELDS.linkedDeals];
      if (Array.isArray(links) && links[0]?.startsWith?.("rec")) return links[0];
    }
  } catch {
    /* table may not exist */
  }

  try {
    const snapRows = await platformBase(MARKET_DEMAND_SNAPSHOTS_TABLE).select({ maxRecords: 20 }).all();
    for (const r of snapRows) {
      const textId = r.fields?.[MARKET_DEMAND_DEAL_RECORD_ID_FIELD];
      if (typeof textId === "string" && textId.startsWith("rec")) return textId;
      const links = r.fields?.[MARKET_DEMAND_SNAPSHOT_FIELDS.linkedDeal];
      const id = Array.isArray(links) ? links[0] : links;
      if (typeof id === "string" && id.startsWith("rec")) return id;
    }
  } catch {
    /* */
  }

  const deals = await dealsBase(DEALS_TABLE).select({ maxRecords: 1 }).firstPage();
  return deals[0]?.id || null;
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

async function callHandler(handler, dealId) {
  const req = { params: { dealId }, body: {} };
  const res = mockRes();
  await handler(req, res);
  return res.out;
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const platformBaseId = getMarketDemandBaseId();
  const dealsBaseId = getDealsBaseId();
  const report = {
    bases: { platform: platformBaseId, deals: dealsBaseId },
    connectivity: {},
    fieldAudit: {},
    dealsOptionalFields: {},
    apiTests: {},
    authPattern: {
      marketDemandRoutes: [
        "GET /api/deals/:dealId/demand-centers",
        "GET /api/deals/:dealId/nearby-hotel-supply",
        "GET /api/deals/:dealId/market-demand-snapshot",
        "POST /api/deals/:dealId/generate-market-demand-snapshot",
        "POST /api/deals/:dealId/import-demand-centers",
      ],
      middleware: "marketDemandDealAuth → myDealsAuth (memberstackAuth, requireDealalityUser, requireMyDealsAccess) → requireDealRecordAccess",
      sameAsOperatorCapabilitySnapshot: true,
    },
    dealId: null,
    blockers: [],
  };

  if (!apiKey || !platformBaseId || !dealsBaseId) {
    console.log(
      JSON.stringify(
        {
          error: "AIRTABLE_API_KEY, AIRTABLE_BASE_ID_ALT (or AIRTABLE_MARKET_DEMAND_BASE_ID), and AIRTABLE_BASE_ID required",
          report,
        },
        null,
        2
      )
    );
    process.exit(1);
  }

  const platformBase = new Airtable({ apiKey }).base(platformBaseId);
  const dealsBase = new Airtable({ apiKey }).base(dealsBaseId);

  let platformMeta;
  let dealsMeta;
  try {
    platformMeta = await fetchMetaTables(platformBaseId, apiKey);
    dealsMeta = await fetchMetaTables(dealsBaseId, apiKey);
  } catch (err) {
    console.log(JSON.stringify({ error: err.message, report }, null, 2));
    process.exit(1);
  }

  const platformTableByName = new Map((platformMeta.tables || []).map((t) => [t.name, t]));
  const dealsTableByName = new Map((dealsMeta.tables || []).map((t) => [t.name, t]));

  for (const tableName of Object.keys(PLATFORM_EXPECTED)) {
    const metaTable = platformTableByName.get(tableName);
    const read = await testTableRead(platformBase, tableName);
    report.connectivity[tableName] = {
      base: "platform",
      inSchema: Boolean(metaTable),
      tableId: metaTable?.id || null,
      dataReadOk: read.ok,
      dataReadError: read.error,
      sampleRecords: read.sampleCount,
    };
    if (!metaTable) {
      report.blockers.push(`Table missing on Platform base: ${tableName}`);
    } else if (!read.ok) {
      report.blockers.push(`Table not readable on Platform: ${tableName} — ${read.error}`);
    }

    if (metaTable) {
      const audit = analyzeTable(metaTable, PLATFORM_EXPECTED[tableName]);
      report.fieldAudit[tableName] = audit;
      if (audit.missing.length) {
        report.blockers.push(`${tableName}: missing fields — ${audit.missing.join(", ")}`);
      }
    }
  }

  const dealsMetaTable = dealsTableByName.get(DEALS_TABLE);
  const dealsRead = await testTableRead(dealsBase, DEALS_TABLE);
  report.connectivity[DEALS_TABLE] = {
    base: "mvp",
    inSchema: Boolean(dealsMetaTable),
    tableId: dealsMetaTable?.id || null,
    dataReadOk: dealsRead.ok,
    dataReadError: dealsRead.error,
    sampleRecords: dealsRead.sampleCount,
  };
  if (dealsMetaTable) {
    report.dealsOptionalFields = analyzeTable(dealsMetaTable, DEALS_OPTIONAL_EXPECTED);
  }

  const dealId = await findSeededDeal(platformBase, dealsBase);
  report.dealId = dealId;

  if (!dealId) {
    report.blockers.push("No deal ID available for API tests");
  } else {
    const handlers = [
      ["GET demand-centers", getDealDemandCenters],
      ["GET nearby-hotel-supply", getDealNearbyHotelSupply],
      ["GET market-demand-snapshot", getDealMarketDemandSnapshot],
      ["POST generate-market-demand-snapshot", postGenerateMarketDemandSnapshot],
    ];
    for (const [name, handler] of handlers) {
      const out = await callHandler(handler, dealId);
      report.apiTests[name] = {
        statusCode: out.statusCode,
        ok: out.body?.ok,
        error: out.body?.error,
        message: out.body?.message,
        summary: summarizeApiBody(name, out.body),
      };
      if (!out.body?.ok && out.body?.error === "market_demand_tables_missing") {
        report.blockers.push(`API ${name}: tables not configured`);
      }
    }
  }

  console.log(JSON.stringify(report, null, 2));
}

function summarizeApiBody(name, body) {
  if (!body) return null;
  if (name.includes("demand-centers")) {
    return {
      count: body.demandCenters?.length,
      total: body.summary?.totalDemandCenters,
      categories: body.summary?.categories,
    };
  }
  if (name.includes("nearby-hotel-supply")) {
    return { count: body.nearbyHotelSupply?.length, total: body.summary?.totalHotels };
  }
  if (name.includes("snapshot") && !name.includes("generate")) {
    return { hasSnapshot: body.hasSnapshot, overall: body.snapshot?.overallDemandStrength };
  }
  if (name.includes("generate")) {
    return {
      hasSnapshot: body.hasSnapshot,
      overall: body.snapshot?.overallDemandStrength,
      profile: body.snapshot?.primaryDemandProfile,
    };
  }
  return null;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
