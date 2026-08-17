#!/usr/bin/env node
/**
 * Ensure Travel Infrastructure Data radar extension fields (non-destructive).
 *
 *   node scripts/ensure-travel-infrastructure-schema.mjs --dry-run
 *   node scripts/ensure-travel-infrastructure-schema.mjs --apply
 */
import "../load-env.js";
import {
  TRAVEL_INFRASTRUCTURE_TABLE,
  TRAVEL_INFRASTRUCTURE_FIELDS as F,
  TRAVEL_INFRA_DEAL_RECORD_ID_FIELD,
  RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE,
  POINT_TYPES,
  POINT_SUBTYPES,
  MAP_ICON_TYPES,
  DEMAND_PATTERN_OPTIONS,
  RELEVANT_HOTEL_TYPES_OPTIONS,
  DEMAND_RELEVANCE_OPTIONS,
  DATA_CONFIDENCE_OPTIONS,
  VISIBILITY_OPTIONS,
  SUBMARKET_OPTIONS,
} from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import { getTravelInfrastructureBaseId } from "../lib/travel-infrastructure/travel-infrastructure-base.js";

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

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
  return (
    (tables || []).find((t) => t.name === name) ||
    (tables || []).find((t) => /travel infrastructure/i.test(t.name)) ||
    null
  );
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

function checkboxField(name) {
  return {
    name,
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
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

async function ensureField(baseId, token, table, spec) {
  if (!table) return { skipped: true };
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
  await new Promise((r) => setTimeout(r, 220));
  return { created: true };
}

function extensionFieldSpecs(marketsTableId) {
  const specs = [
    singleSelect(F.radarCategory, [RADAR_CATEGORY_TRAVEL_INFRASTRUCTURE]),
    singleSelect(F.pointType, [...POINT_TYPES, "Convention Center"]),
    singleSelect(F.pointSubtype, [...POINT_SUBTYPES, "Convention Center", "Unknown"]),
    { name: TRAVEL_INFRA_DEAL_RECORD_ID_FIELD, type: "singleLineText" },
    { name: F.address, type: "singleLineText" },
    singleSelect(F.submarket, SUBMARKET_OPTIONS),
    numberField(F.distanceFromDeal, 1),
    numberField(F.estimatedDriveTime, 0),
    singleSelect(F.demandRelevance, DEMAND_RELEVANCE_OPTIONS),
    multiSelect(F.demandPattern, DEMAND_PATTERN_OPTIONS),
    multiSelect(F.relevantHotelTypes, RELEVANT_HOTEL_TYPES_OPTIONS),
    { name: F.hotelDemandRationale, type: "multilineText" },
    multiSelect(F.source, ["Manual Research", "Broker Insight", "Owner Input", "Public Source"]),
    { name: F.sourceReference, type: "url" },
    singleSelect(F.dataConfidence, DATA_CONFIDENCE_OPTIONS),
    checkboxField(F.includeOnRadarMap),
    { name: F.mapLayer, type: "singleLineText" },
    singleSelect(F.mapIconType, MAP_ICON_TYPES),
    singleSelect(F.visibility, VISIBILITY_OPTIONS),
    { name: F.notes, type: "multilineText" },
  ];
  if (marketsTableId) {
    specs.splice(3, 0, {
      name: F.linkedMarket,
      type: "multipleRecordLinks",
      options: { linkedTableId: marketsTableId },
    });
  }
  return specs;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = getTravelInfrastructureBaseId();
  if (!token || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Platform base:", baseId);

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed ${res.status}: ${JSON.stringify(json)}`);

  const table = findTable(json.tables, TRAVEL_INFRASTRUCTURE_TABLE);
  if (!table) {
    throw new Error(`Travel Infrastructure table not found (expected ${TRAVEL_INFRASTRUCTURE_TABLE})`);
  }

  console.log(`\n${table.name} — radar extension fields (legacy Name/Type/Lat/Lng preserved)`);
  const marketsTable = (json.tables || []).find((t) => t.name === "Markets");
  for (const spec of extensionFieldSpecs(marketsTable?.id)) {
    await ensureField(baseId, token, table, spec);
  }

  console.log("\nDone.", DRY ? "Re-run with --apply to create fields." : "Schema apply complete.");
  console.log("Existing records keep legacy Type; new Point Type / Point Subtype fields are additive.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
