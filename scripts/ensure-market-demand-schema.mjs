#!/usr/bin/env node
/**
 * Ensure Market Demand Intelligence Airtable schema (non-destructive).
 *
 *   node scripts/ensure-market-demand-schema.mjs --dry-run
 *   node scripts/ensure-market-demand-schema.mjs --apply
 *
 * Requires AIRTABLE_API_KEY with schema.bases:read + schema.bases:write
 */
import "../load-env.js";
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
  DEALS_LINKED_MARKET_RECORD_ID_FIELD,
  DEALS_TABLE,
} from "../lib/market-demand/airtable-market-demand-fields.js";
import {
  getMarketDemandBaseId,
  getDealsBaseId,
} from "../lib/market-demand/market-demand-base.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

const CONFIDENCE_OPTIONS = ["High", "Medium", "Low"];
const DEMAND_STRENGTH_OPTIONS = ["High", "Medium", "Low"];
const OVERALL_STRENGTH_OPTIONS = ["Strong", "Moderate", "Limited", "Unclear"];
const RELEVANCE_OPTIONS = ["High", "Medium", "Low"];
const COMPETITIVE_RELEVANCE_OPTIONS = ["Primary", "Secondary", "Tertiary", "Low", "Unknown"];

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name) || null;
}

function hasField(table, name) {
  return (table?.fields || []).some((f) => f.name === name);
}

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

function singleSelect(name, optionNames) {
  return { name, type: "singleSelect", options: { choices: choices(optionNames) } };
}

function multiSelect(name, optionNames) {
  return { name, type: "multipleSelects", options: { choices: choices(optionNames) } };
}

function dateField(name) {
  return { name, type: "date", options: { dateFormat: { name: "iso" } } };
}

function numberField(name, precision = 0) {
  return { name, type: "number", options: { precision } };
}

function linkField(name, linkedTableId, preferSingle = false) {
  return {
    name,
    type: preferSingle ? "multipleRecordLinks" : "multipleRecordLinks",
    options: { linkedTableId },
  };
}

async function createField(baseId, token, tableId, spec) {
  if (DRY) {
    console.log(`[dry-run] would create field ${spec.name}`);
    return { ok: true, dry: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify({
      name: spec.name,
      type: spec.type,
      ...(spec.options ? { options: spec.options } : {}),
    }),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

async function createTable(baseId, token, body) {
  if (DRY) {
    console.log(`[dry-run] would create table ${body.name} (${body.fields?.length || 0} fields)`);
    return { ok: true, dry: true, json: { id: "dry_run", name: body.name } };
  }
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) return { ok: false, status: res.status, json };
  return { ok: true, json };
}

async function ensureField(baseId, token, table, spec) {
  if (!table) return { skipped: true, reason: "no table" };
  if (hasField(table, spec.name)) {
    console.log(`  skip (exists): ${spec.name}`);
    return { skipped: true };
  }
  const r = await createField(baseId, token, table.id, spec);
  if (!r.ok) {
    console.error(`  FAIL ${spec.name}: ${r.status}`, JSON.stringify(r.json));
    return { failed: true };
  }
  console.log(`  created: ${spec.name}`);
  if (!DRY && table.fields) table.fields.push({ name: spec.name, type: spec.type });
  await sleep(220);
  return { created: true };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function marketsInitialFields() {
  return [
    { name: MARKET_FIELDS.name, type: "singleLineText" },
    { name: MARKET_FIELDS.country, type: "singleLineText" },
    { name: MARKET_FIELDS.region, type: "singleLineText" },
    { name: MARKET_FIELDS.subregion, type: "singleLineText" },
    singleSelect(MARKET_FIELDS.marketType, ["Urban", "Suburban", "Resort", "Airport", "Mixed"]),
    numberField(MARKET_FIELDS.latitude, 6),
    numberField(MARKET_FIELDS.longitude, 6),
    { name: MARKET_FIELDS.primaryDemandProfile, type: "singleLineText" },
    { name: MARKET_FIELDS.marketNotes, type: "multilineText" },
    singleSelect(MARKET_FIELDS.dataConfidence, CONFIDENCE_OPTIONS),
    dateField(MARKET_FIELDS.lastReviewed),
  ];
}

function demandCategoriesInitialFields() {
  return [
    { name: DEMAND_CATEGORY_FIELDS.category, type: "singleLineText" },
    { name: DEMAND_CATEGORY_FIELDS.description, type: "multilineText" },
    multiSelect(DEMAND_CATEGORY_FIELDS.typicalDemandPattern, ["Transient", "Weekday", "Weekend", "Seasonal", "Year-Round"]),
    multiSelect(DEMAND_CATEGORY_FIELDS.mostRelevantHotelTypes, ["Select-Service", "Full-Service", "Extended-Stay", "Resort", "Lifestyle"]),
    { name: DEMAND_CATEGORY_FIELDS.brandFitImplications, type: "multilineText" },
    { name: DEMAND_CATEGORY_FIELDS.operatorFitImplications, type: "multilineText" },
    numberField(DEMAND_CATEGORY_FIELDS.scoringWeight, 0),
  ];
}

function demandCentersInitialFields(marketsId) {
  return [
    { name: DEMAND_CENTER_FIELDS.name, type: "singleLineText" },
    linkField(DEMAND_CENTER_FIELDS.linkedMarket, marketsId),
    { name: MARKET_DEMAND_DEAL_RECORD_ID_FIELD, type: "singleLineText" },
    singleSelect(DEMAND_CENTER_FIELDS.demandCategory, [
      "Leisure",
      "Corporate",
      "Group / Event",
      "Medical",
      "Education",
      "Transportation",
      "Industrial",
      "Retail / Mixed-Use",
      "Government",
    ]),
    { name: DEMAND_CENTER_FIELDS.demandSubcategory, type: "singleLineText" },
    { name: DEMAND_CENTER_FIELDS.address, type: "singleLineText" },
    numberField(DEMAND_CENTER_FIELDS.latitude, 6),
    numberField(DEMAND_CENTER_FIELDS.longitude, 6),
    numberField(DEMAND_CENTER_FIELDS.distanceFromDeal, 1),
    numberField(DEMAND_CENTER_FIELDS.estimatedDriveTime, 0),
    singleSelect(DEMAND_CENTER_FIELDS.demandStrength, DEMAND_STRENGTH_OPTIONS),
    singleSelect(DEMAND_CENTER_FIELDS.relevanceToHotelDemand, RELEVANCE_OPTIONS),
    multiSelect(DEMAND_CENTER_FIELDS.demandPattern, ["Transient", "Weekday", "Weekend", "Seasonal", "Year-Round", "Group", "Leisure"]),
    multiSelect(DEMAND_CENTER_FIELDS.relevantHotelTypes, ["Select-Service", "Full-Service", "Airport", "Midscale", "Extended-Stay", "Lifestyle", "Boutique", "Upper Upscale"]),
    multiSelect(DEMAND_CENTER_FIELDS.source, ["Manual Research", "Broker Insight", "Owner Input", "Public Source"]),
    { name: DEMAND_CENTER_FIELDS.sourceReference, type: "url" },
    { name: DEMAND_CENTER_FIELDS.sourcePlaceId, type: "singleLineText" },
    singleSelect(DEMAND_CENTER_FIELDS.dataConfidence, CONFIDENCE_OPTIONS),
    dateField(DEMAND_CENTER_FIELDS.lastVerified),
    { name: DEMAND_CENTER_FIELDS.notes, type: "multilineText" },
    { name: DEMAND_CENTER_FIELDS.aiInterpretation, type: "multilineText" },
    numberField(DEMAND_CENTER_FIELDS.relevanceScore, 0),
  ];
}

function nearbyHotelInitialFields(marketsId) {
  return [
    { name: NEARBY_HOTEL_SUPPLY_FIELDS.hotelName, type: "singleLineText" },
    linkField(NEARBY_HOTEL_SUPPLY_FIELDS.linkedMarket, marketsId),
    { name: MARKET_DEMAND_DEAL_RECORD_ID_FIELD, type: "singleLineText" },
    { name: NEARBY_HOTEL_SUPPLY_FIELDS.brand, type: "singleLineText" },
    { name: NEARBY_HOTEL_SUPPLY_FIELDS.parentCompany, type: "singleLineText" },
    singleSelect(NEARBY_HOTEL_SUPPLY_FIELDS.chainScale, ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale", "Economy", "Independent"]),
    singleSelect(NEARBY_HOTEL_SUPPLY_FIELDS.hotelType, ["Full-Service", "Select-Service", "Extended-Stay", "Resort", "Boutique"]),
    numberField(NEARBY_HOTEL_SUPPLY_FIELDS.rooms, 0),
    numberField(NEARBY_HOTEL_SUPPLY_FIELDS.distanceFromDeal, 1),
    numberField(NEARBY_HOTEL_SUPPLY_FIELDS.estimatedDriveTime, 0),
    singleSelect(NEARBY_HOTEL_SUPPLY_FIELDS.competitiveRelevance, COMPETITIVE_RELEVANCE_OPTIONS),
    { name: NEARBY_HOTEL_SUPPLY_FIELDS.source, type: "singleLineText" },
    singleSelect(NEARBY_HOTEL_SUPPLY_FIELDS.dataConfidence, CONFIDENCE_OPTIONS),
    { name: NEARBY_HOTEL_SUPPLY_FIELDS.notes, type: "multilineText" },
  ];
}

function snapshotsInitialFields(marketsId) {
  const scoreFields = [
    MARKET_DEMAND_SNAPSHOT_FIELDS.leisureDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.corporateDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.groupDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.medicalDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.educationDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.transportationDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.industrialDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.retailMixedUseDemandScore,
    MARKET_DEMAND_SNAPSHOT_FIELDS.governmentDemandScore,
  ];
  return [
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.snapshotName, type: "singleLineText" },
    { name: MARKET_DEMAND_DEAL_RECORD_ID_FIELD, type: "singleLineText" },
    linkField(MARKET_DEMAND_SNAPSHOT_FIELDS.linkedMarket, marketsId),
    ...scoreFields.map((n) => numberField(n, 0)),
    singleSelect(MARKET_DEMAND_SNAPSHOT_FIELDS.overallDemandStrength, OVERALL_STRENGTH_OPTIONS),
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.primaryDemandProfile, type: "singleLineText" },
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.demandSummary, type: "multilineText" },
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.demandGaps, type: "multilineText" },
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.brandImplications, type: "multilineText" },
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.operatorImplications, type: "multilineText" },
    { name: MARKET_DEMAND_SNAPSHOT_FIELDS.recommendedFollowUp, type: "multilineText" },
    singleSelect(MARKET_DEMAND_SNAPSHOT_FIELDS.dataConfidence, CONFIDENCE_OPTIONS),
    dateField(MARKET_DEMAND_SNAPSHOT_FIELDS.lastGenerated),
  ];
}

function dealsOptionalFieldSpecs() {
  return [
    { name: DEALS_LINKED_MARKET_RECORD_ID_FIELD, type: "singleLineText" },
    numberField(DEALS_MARKET_DEMAND_FIELDS.demandCenterCount, 0),
    { name: DEALS_MARKET_DEMAND_FIELDS.primaryDemandDrivers, type: "singleLineText" },
    singleSelect(DEALS_MARKET_DEMAND_FIELDS.demandStrengthScore, OVERALL_STRENGTH_OPTIONS),
    singleSelect(DEALS_MARKET_DEMAND_FIELDS.demandConfidence, CONFIDENCE_OPTIONS),
    { name: DEALS_MARKET_DEMAND_FIELDS.demandSummary, type: "multilineText" },
    { name: DEALS_MARKET_DEMAND_FIELDS.demandGapsQuestions, type: "multilineText" },
  ];
}

async function ensureTable(baseId, token, tables, tableName, buildFields) {
  let table = findTable(tables, tableName);
  if (!table) {
    console.log(`\nCreate table: ${tableName}`);
    const fields = buildFields();
    const cr = await createTable(baseId, token, {
      name: tableName,
      description: "Market Demand Intelligence (Dealality MVP).",
      fields,
    });
    if (!cr.ok) throw new Error(`Create ${tableName} failed ${cr.status}: ${JSON.stringify(cr.json)}`);
    console.log(`  created table ${tableName}`);
    if (!DRY) {
      const refresh = await metaFetch(baseId, token, "/tables");
      tables = refresh.json.tables || [];
      table = findTable(tables, tableName);
    } else {
      table = { id: "dry_run", name: tableName, fields: fields.map((f) => ({ name: f.name })) };
    }
    return { tables, table, created: true };
  }

  console.log(`\n${tableName} — ensure missing fields`);
  for (const spec of buildFields()) {
    await ensureField(baseId, token, table, spec);
  }
  return { tables, table, created: false };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const platformBaseId = getMarketDemandBaseId();
  const dealsBaseId = getDealsBaseId();
  if (!token) throw new Error("Set AIRTABLE_API_KEY");
  if (!platformBaseId) {
    throw new Error(
      "Set AIRTABLE_BASE_ID_ALT or AIRTABLE_MARKET_DEMAND_BASE_ID for Platform Market Demand tables"
    );
  }
  if (!dealsBaseId) throw new Error("Set AIRTABLE_BASE_ID for Deals optional mirror fields");

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Platform base (Market Demand tables):", platformBaseId);
  console.log("MVP base (Deals optional fields):", dealsBaseId);

  const { res: listRes, json: listJson } = await metaFetch(platformBaseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List Platform tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  let tables = listJson.tables || [];

  let marketsResult = await ensureTable(platformBaseId, token, tables, MARKETS_TABLE, marketsInitialFields);
  tables = marketsResult.tables;
  const marketsTable = marketsResult.table;

  let catResult = await ensureTable(
    platformBaseId,
    token,
    tables,
    DEMAND_CATEGORIES_TABLE,
    demandCategoriesInitialFields
  );
  tables = catResult.tables;

  let dcResult = await ensureTable(platformBaseId, token, tables, DEMAND_CENTERS_TABLE, () =>
    demandCentersInitialFields(marketsTable.id)
  );
  tables = dcResult.tables;

  let hotelResult = await ensureTable(platformBaseId, token, tables, NEARBY_HOTEL_SUPPLY_TABLE, () =>
    nearbyHotelInitialFields(marketsTable.id)
  );
  tables = hotelResult.tables;

  let snapResult = await ensureTable(
    platformBaseId,
    token,
    tables,
    MARKET_DEMAND_SNAPSHOTS_TABLE,
    () => snapshotsInitialFields(marketsTable.id)
  );
  tables = snapResult.tables;

  const dealsList = await metaFetch(dealsBaseId, token, "/tables");
  if (!dealsList.res.ok) {
    throw new Error(`List MVP tables failed ${dealsList.res.status}: ${JSON.stringify(dealsList.json)}`);
  }
  const dealsTable = findTable(dealsList.json.tables || [], DEALS_TABLE);
  if (!dealsTable) throw new Error(`Deals table not found on MVP base: ${DEALS_TABLE}`);

  console.log(`\n${DEALS_TABLE} (MVP) — optional Market Demand fields`);
  for (const spec of dealsOptionalFieldSpecs()) {
    await ensureField(dealsBaseId, token, dealsTable, spec);
  }

  console.log("\nDone.", DRY ? "Re-run with --apply to create schema." : "Schema apply complete.");
  console.log(
    "\nNote: Remove orphan Market Demand tables from Deal Capture MVP if they were created there by mistake."
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
