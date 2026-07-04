/**
 * Step 1 — Read-only Hotel Census inventory before STR Excel import.
 *
 * Does NOT create, update, or delete Airtable records.
 *
 * Usage:
 *   node scripts/inventory-hotel-census-for-str-import.mjs
 *
 * Output:
 *   reports/hotel-census-str-field-inventory.csv
 *   reports/hotel-census-str-data-quality.csv
 *   reports/hotel-census-str-duplicates.csv
 *   reports/hotel-census-str-inventory-summary.json
 */
import "../load-env.js";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { fetchTableSchema } from "../lib/str-census-import/airtable-meta.mjs";
import {
  inventoryAirtableFields,
  recommendCensusFieldMapping,
  fieldValue,
} from "../lib/str-census-import/field-mapping.mjs";
import {
  normalizeStrId,
  normalizeKey,
  nameCityCountryKey,
} from "../lib/str-census-import/normalize.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

const FIELD_CSV = join(REPORTS, "hotel-census-str-field-inventory.csv");
const QUALITY_CSV = join(REPORTS, "hotel-census-str-data-quality.csv");
const DUPES_CSV = join(REPORTS, "hotel-census-str-duplicates.csv");
const SUMMARY_JSON = join(REPORTS, "hotel-census-str-inventory-summary.json");

const SAMPLE_LIMIT = 5;

function addToSet(map, key, recordId) {
  if (!key) return;
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(recordId);
}

function pickSamples(arr, limit = SAMPLE_LIMIT) {
  return arr.slice(0, limit);
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  console.log("=== Hotel Census STR import inventory (read-only) ===\n");

  const schema = await fetchTableSchema(HOTEL_CENSUS_TABLE);
  if (!schema.metadataAvailable) {
    console.warn("Metadata API unavailable:", schema.metadataError);
    console.warn("Continuing with record scan only (field list from first record batch).\n");
  }

  let fieldInventory = inventoryAirtableFields(schema.fields);

  const base = new Airtable({ apiKey }).base(baseId);
  console.log(`Loading all "${HOTEL_CENSUS_TABLE}" records (read-only)...`);
  const records = await base(HOTEL_CENSUS_TABLE).select({ pageSize: 100 }).all();
  console.log(`Loaded ${records.length} records.\n`);

  if (!fieldInventory.length && records.length) {
    const names = new Set();
    for (const r of records) {
      Object.keys(r.fields || {}).forEach((n) => names.add(n));
    }
    fieldInventory = inventoryAirtableFields(
      [...names].map((name) => ({ name, id: "", type: "unknown" }))
    );
  }

  let { mapping, strIdPick, strMarketExists, strSubmarketExists, recommendations } =
    recommendCensusFieldMapping(fieldInventory, records);

  const strIdField = mapping.strId;
  const cityField = mapping.city;
  const nameField = mapping.hotelName;
  const countryField = mapping.country;
  const marketField = mapping.strMarket;
  const submarketField = mapping.strSubmarket;

  let withStrId = 0;
  let missingStrId = 0;
  let missingCity = 0;
  let missingCountry = 0;
  let missingName = 0;
  let withMarket = 0;
  let withSubmarket = 0;

  const countries = new Set();
  const markets = new Set();
  const submarkets = new Set();

  const strIdIndex = new Map();
  const nccIndex = new Map();

  const samples = {
    strId: [],
    missingStrId: [],
    strMarket: [],
    strSubmarket: [],
    nameCityCountry: [],
  };

  for (const rec of records) {
    const f = rec.fields || {};
    const strId = normalizeStrId(strIdField ? fieldValue(f, strIdField) : "");
    const city = fieldValue(f, cityField);
    const name = fieldValue(f, nameField);
    const country = fieldValue(f, countryField);
    const market = fieldValue(f, marketField);
    const submarket = fieldValue(f, submarketField);

    if (strId) {
      withStrId++;
      addToSet(strIdIndex, strId, rec.id);
      if (samples.strId.length < SAMPLE_LIMIT) {
        samples.strId.push({ recordId: rec.id, strId, name, city, country });
      }
    } else {
      missingStrId++;
      if (samples.missingStrId.length < SAMPLE_LIMIT) {
        samples.missingStrId.push({ recordId: rec.id, name, city, country });
      }
    }

    if (!normalizeKey(city)) missingCity++;
    if (!normalizeKey(country)) missingCountry++;
    if (!normalizeKey(name)) missingName++;

    if (normalizeKey(market)) {
      withMarket++;
      markets.add(market.trim());
      if (samples.strMarket.length < SAMPLE_LIMIT) {
        samples.strMarket.push({ recordId: rec.id, strMarket: market, strId, name });
      }
    }
    if (normalizeKey(submarket)) {
      withSubmarket++;
      submarkets.add(submarket.trim());
      if (samples.strSubmarket.length < SAMPLE_LIMIT) {
        samples.strSubmarket.push({ recordId: rec.id, strSubmarket: submarket, strId, name });
      }
    }

    if (normalizeKey(country)) countries.add(country.trim());

    const ncc = nameCityCountryKey(name, city, country);
    if (ncc !== "||") addToSet(nccIndex, ncc, rec.id);
    if (samples.nameCityCountry.length < SAMPLE_LIMIT && name && city && country) {
      samples.nameCityCountry.push({ recordId: rec.id, name, city, country, strId });
    }
  }

  const duplicateRows = [];

  for (const [strId, ids] of strIdIndex) {
    if (ids.length <= 1) continue;
    const names = new Set();
    for (const id of ids) {
      const rec = records.find((r) => r.id === id);
      names.add(fieldValue(rec?.fields, nameField));
    }
    duplicateRows.push({
      issueType: ids.length > 1 && names.size > 1 ? "Same STR ID, different hotel names" : "Duplicate STR ID",
      key: strId,
      recordIds: ids.join("; "),
      recordCount: ids.length,
      hotelNames: [...names].join(" | "),
      notes: "Resolve before STR import",
    });
  }

  for (const [ncc, ids] of nccIndex) {
    if (ids.length <= 1) continue;
    const strIds = new Set();
    for (const id of ids) {
      const rec = records.find((r) => r.id === id);
      const sid = normalizeStrId(fieldValue(rec?.fields, strIdField));
      if (sid) strIds.add(sid);
    }
    const [name, city, country] = ncc.split("|");
    duplicateRows.push({
      issueType:
        strIds.size > 1
          ? "Same Name+City+Country, different STR IDs"
          : "Duplicate Name+City+Country",
      key: `${name} / ${city} / ${country}`,
      recordIds: ids.join("; "),
      recordCount: ids.length,
      hotelNames: "",
      notes: strIds.size > 1 ? `STR IDs: ${[...strIds].join(", ")}` : "",
    });
  }

  const duplicateStrIdCount = [...strIdIndex.values()].filter((a) => a.length > 1).length;

  writeCsv(
    FIELD_CSV,
    fieldInventory.map((r) => ({
      fieldName: r.name,
      fieldId: r.id,
      fieldType: r.type,
      excelMapping: r.excelRole || "",
      matchScore: r.matchScore,
      matchReason: r.matchReason,
    })),
    ["fieldName", "fieldId", "fieldType", "excelMapping", "matchScore", "matchReason"]
  );

  const qualityRows = [
    { metric: "totalRecords", value: records.length, notes: "" },
    { metric: "recordsWithStrId", value: withStrId, notes: strIdField || "field TBD" },
    { metric: "recordsMissingStrId", value: missingStrId, notes: "" },
    { metric: "duplicateStrIdGroups", value: duplicateStrIdCount, notes: "Same STR ID on multiple records" },
    { metric: "recordsMissingCity", value: missingCity, notes: cityField || "field TBD" },
    { metric: "recordsMissingCountry", value: missingCountry, notes: countryField || "field TBD" },
    { metric: "recordsMissingHotelName", value: missingName, notes: nameField || "field TBD" },
    { metric: "recordsWithStrMarket", value: withMarket, notes: marketField || "not found" },
    { metric: "recordsWithStrSubmarket", value: withSubmarket, notes: submarketField || "not found" },
    { metric: "distinctCountries", value: countries.size, notes: pickSamples([...countries], 10).join("; ") },
    { metric: "distinctStrMarkets", value: markets.size, notes: pickSamples([...markets], 10).join("; ") },
    { metric: "distinctStrSubmarkets", value: submarkets.size, notes: pickSamples([...submarkets], 10).join("; ") },
  ];
  writeCsv(QUALITY_CSV, qualityRows, ["metric", "value", "notes"]);

  writeCsv(DUPES_CSV, duplicateRows, [
    "issueType",
    "key",
    "recordIds",
    "recordCount",
    "hotelNames",
    "notes",
  ]);

  const summary = {
    generatedAt: new Date().toISOString(),
    baseId: process.env.AIRTABLE_BASE_ID_ALT,
    table: HOTEL_CENSUS_TABLE,
    tableId: schema.table?.id || null,
    metadataAvailable: schema.metadataAvailable,
    metadataError: schema.metadataError,
    recommendedFieldMapping: mapping,
    strIdFieldCandidates: strIdPick?.candidates || [],
    strIdPopulatedCount: strIdPick?.populatedCount ?? 0,
    strMarketFieldExists: strMarketExists,
    strSubmarketFieldExists: strSubmarketExists,
    dataQuality: Object.fromEntries(qualityRows.map((r) => [r.metric, r.value])),
    samples,
    duplicateIssueCount: duplicateRows.length,
    recommendations,
    reportFiles: {
      fieldInventory: FIELD_CSV,
      dataQuality: QUALITY_CSV,
      duplicates: DUPES_CSV,
    },
  };

  writeJson(SUMMARY_JSON, summary);

  console.log("Reports:");
  console.log(" ", FIELD_CSV);
  console.log(" ", QUALITY_CSV);
  console.log(" ", DUPES_CSV);
  console.log(" ", SUMMARY_JSON);
  console.log("\n--- Recommended field mapping ---");
  for (const [role, field] of Object.entries(mapping)) {
    console.log(`  ${role}: ${field || "(not detected)"}`);
  }
  console.log(`\nSTR Market field exists: ${strMarketExists}`);
  console.log(`STR Submarket field exists: ${strSubmarketExists}`);
  console.log("\n--- Recommendations ---");
  recommendations.forEach((r) => console.log(`  • ${r}`));
  console.log("\n--- Data quality (high level) ---");
  console.log(`  Total records: ${records.length}`);
  console.log(`  With STR ID: ${withStrId} | Missing STR ID: ${missingStrId}`);
  console.log(`  Duplicate STR ID groups: ${duplicateStrIdCount}`);
  console.log(`  Duplicate / conflict rows logged: ${duplicateRows.length}`);
  console.log("\nDone. No Airtable changes were made.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
