#!/usr/bin/env node
/**
 * Ensure Demand Anchors Airtable table + fields (non-destructive).
 *
 *   node scripts/ensure-demand-anchors-schema.mjs --dry-run
 *   node scripts/ensure-demand-anchors-schema.mjs --apply
 */
import "../load-env.js";
import {
  DEMAND_ANCHORS_TABLE,
  DEMAND_ANCHORS_FIELDS as F,
  DEMAND_ANCHOR_DEAL_RECORD_ID_FIELD,
  RADAR_CATEGORY_DEMAND_ANCHORS,
  POINT_TYPES,
  DEMAND_SEGMENT_OPTIONS,
  DEMAND_PATTERN_OPTIONS,
  RELEVANT_HOTEL_TYPES_OPTIONS,
  MAP_ICON_TYPES,
  DEMAND_RELEVANCE_OPTIONS,
  DATA_CONFIDENCE_OPTIONS,
  VISIBILITY_OPTIONS,
  SOURCE_OPTIONS,
  SUBMARKET_OPTIONS,
} from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { getDemandAnchorsBaseId } from "../lib/demand-anchors/demand-anchors-base.js";

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
    (tables || []).find((t) => /demand anchors/i.test(t.name)) ||
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

function linkField(name, linkedTableId) {
  return {
    name,
    type: "multipleRecordLinks",
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

function demandAnchorsFieldSpecs(marketsTableId, dealsTableId) {
  const specs = [
    singleSelect(F.radarCategory, [RADAR_CATEGORY_DEMAND_ANCHORS]),
    singleSelect(F.pointType, POINT_TYPES),
    { name: F.pointSubtype, type: "singleLineText" },
    { name: DEMAND_ANCHOR_DEAL_RECORD_ID_FIELD, type: "singleLineText" },
    { name: F.city, type: "singleLineText" },
    { name: F.country, type: "singleLineText" },
    { name: F.region, type: "singleLineText" },
    singleSelect(F.submarket, SUBMARKET_OPTIONS),
    { name: F.address, type: "singleLineText" },
    numberField(F.lat, 6),
    numberField(F.lng, 6),
    numberField(F.distanceFromDeal, 1),
    numberField(F.estimatedDriveTime, 0),
    singleSelect(F.demandRelevance, DEMAND_RELEVANCE_OPTIONS),
    singleSelect(F.demandSegment, DEMAND_SEGMENT_OPTIONS),
    multiSelect(F.demandPattern, DEMAND_PATTERN_OPTIONS),
    multiSelect(F.relevantHotelTypes, RELEVANT_HOTEL_TYPES_OPTIONS),
    { name: F.hotelDemandRationale, type: "multilineText" },
    multiSelect(F.source, SOURCE_OPTIONS),
    { name: F.sourceReference, type: "url" },
    singleSelect(F.dataConfidence, DATA_CONFIDENCE_OPTIONS),
    checkboxField(F.includeOnRadarMap),
    { name: F.mapLayer, type: "singleLineText" },
    singleSelect(F.mapIconType, MAP_ICON_TYPES),
    singleSelect(F.visibility, VISIBILITY_OPTIONS),
    { name: F.notes, type: "multilineText" },
    dateField(F.lastVerified),
  ];

  if (marketsTableId) {
    specs.splice(3, 0, linkField(F.linkedMarket, marketsTableId));
  }
  if (dealsTableId) {
    specs.splice(4, 0, linkField(F.linkedDeals, dealsTableId));
  }

  return specs;
}

function demandAnchorsInitialFields(marketsTableId, dealsTableId) {
  return [{ name: F.name, type: "singleLineText" }, ...demandAnchorsFieldSpecs(marketsTableId, dealsTableId)];
}

async function ensureTable(baseId, token, tables, tableName, buildFields) {
  let table = findTable(tables, tableName);
  if (!table) {
    console.log(`\nCreate table: ${tableName}`);
    const fields = buildFields();
    const cr = await createTable(baseId, token, {
      name: tableName,
      description: "Hotel demand-generating locations for Dealality Radar (Demand Anchors layer).",
      fields,
    });
    if (!cr.ok) {
      throw new Error(`Create ${tableName} failed ${cr.status}: ${JSON.stringify(cr.json)}`);
    }
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
  const baseId = getDemandAnchorsBaseId();
  if (!token || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  console.log(DRY ? "=== DRY RUN ===" : "=== APPLY ===");
  console.log("Platform base:", baseId);

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed ${res.status}: ${JSON.stringify(json)}`);

  let tables = json.tables || [];
  const marketsTable = findTable(tables, "Markets");
  const dealsTable = findTable(tables, "Deals");

  const result = await ensureTable(baseId, token, tables, DEMAND_ANCHORS_TABLE, () =>
    demandAnchorsInitialFields(marketsTable?.id, dealsTable?.id)
  );

  console.log("\nDone.", DRY ? "Re-run with --apply to create schema." : "Schema apply complete.");
  if (!dealsTable) {
    console.log("Note: Linked Deals field skipped — no Deals table on Platform base (use Deal Record ID for MVP deals).");
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
