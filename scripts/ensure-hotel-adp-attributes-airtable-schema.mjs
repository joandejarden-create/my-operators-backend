/**
 * Ensure "Hotel ADP Attributes" table on ADP / intelligence base.
 *
 * Usage:
 *   node scripts/ensure-hotel-adp-attributes-airtable-schema.mjs
 *   node scripts/ensure-hotel-adp-attributes-airtable-schema.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CANONICAL_INTELLIGENCE_BASE_ID } from "../lib/decision-outcomes/airtable-base.js";
import {
  HOTEL_ADP_ATTRIBUTES_TABLE,
  MAP_HOTEL_ADP_ATTRIBUTE as F,
  VAL_ATTRIBUTE_CATEGORY,
  VAL_ADP_USE_TYPE,
  VAL_SOURCE_TYPE,
  VAL_CONFIDENCE,
  HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/adp-attributes/field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

const baseId = (
  process.env.ADP_AIRTABLE_BASE_ID ||
  process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID
).trim();
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}
function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}
function multiSelect(name, optionNames, description) {
  const field = { name, type: "multipleSelects", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}
function checkbox(name, description) {
  const field = {
    name,
    type: "checkbox",
    options: { color: "greenBright", icon: "check" },
  };
  if (description) field.description = description;
  return field;
}
function dateTimeField(name, description) {
  const field = {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
  if (description) field.description = description;
  return field;
}
function dateField(name, description) {
  const field = { name, type: "date", options: { dateFormat: { name: "iso" } } };
  if (description) field.description = description;
  return field;
}
function singleLine(name, description) {
  const field = { name, type: "singleLineText" };
  if (description) field.description = description;
  return field;
}
function multiline(name, description) {
  const field = { name, type: "multilineText" };
  if (description) field.description = description;
  return field;
}
function urlField(name, description) {
  const field = { name, type: "url" };
  if (description) field.description = description;
  return field;
}

async function metaFetch(metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
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

function attributeFields() {
  return [
    singleLine(F.attributeKey, "Primary — equals Dedupe Key"),
    singleLine(F.dedupeKey, "HPC Hotel ID + Attribute Name + Attribute Version"),
    singleLine(F.hpcHotelId, "Canonical Hotel Property Census rec…"),
    singleLine(F.dealalityHotelId),
    singleLine(F.adpPropertyId),
    singleLine(F.hotelName),
    singleSelect(F.attributeCategory, VAL_ATTRIBUTE_CATEGORY),
    singleLine(F.attributeName),
    multiline(F.attributeValue),
    singleLine(F.normalizedValue),
    checkbox(F.usedInAdp, "Whether ADP currently consumes this attribute"),
    multiSelect(F.adpUseType, VAL_ADP_USE_TYPE),
    singleSelect(F.sourceType, VAL_SOURCE_TYPE),
    singleLine(F.sourceRecordId),
    singleLine(F.sourceName),
    urlField(F.sourceUrl),
    singleSelect(F.confidence, VAL_CONFIDENCE),
    dateField(F.effectiveFrom),
    dateField(F.effectiveTo),
    singleLine(F.attributeVersion),
    dateTimeField(F.lastVerifiedAt),
    checkbox(F.active, "False = deactivated (history preserved)"),
    multiline(F.notes),
    singleLine(F.schemaVersion, HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION),
  ];
}

if (!token || !baseId) {
  console.error("Missing Airtable token or base id");
  process.exit(1);
}

const decision = {
  generatedAt: new Date().toISOString(),
  apply: APPLY,
  baseId,
  table: HOTEL_ADP_ATTRIBUTES_TABLE,
  schemaVersion: HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION,
  tables: {},
};

const { res: listRes, json: listJson } = await metaFetch("/tables");
if (!listRes.ok) {
  console.error(listJson);
  process.exit(1);
}
const existing = (listJson.tables || []).find((t) => t.name === HOTEL_ADP_ATTRIBUTES_TABLE);
const fields = attributeFields();

if (existing) {
  decision.tables[HOTEL_ADP_ATTRIBUTES_TABLE] = {
    action: "REUSED",
    tableId: existing.id,
  };
  const have = new Set((existing.fields || []).map((f) => f.name));
  const missing = fields.filter((f) => !have.has(f.name));
  decision.tables[HOTEL_ADP_ATTRIBUTES_TABLE].missingFields = missing.map((f) => f.name);
  if (APPLY && missing.length) {
    for (const field of missing) {
      const { res, json } = await metaFetch(`/tables/${existing.id}/fields`, {
        method: "POST",
        body: JSON.stringify(field),
      });
      if (!res.ok) {
        decision.fieldErrors = decision.fieldErrors || [];
        decision.fieldErrors.push({ field: field.name, error: json });
      }
    }
  }
} else {
  decision.tables[HOTEL_ADP_ATTRIBUTES_TABLE] = {
    action: APPLY ? "CREATE" : "WOULD_CREATE",
  };
  if (APPLY) {
    const body = {
      name: HOTEL_ADP_ATTRIBUTES_TABLE,
      description:
        "Derived audit trail of hotel attributes consumed by ADP. Not a hotel identity SoT — links to HPC Hotel ID.",
      fields,
    };
    const { res, json } = await metaFetch("/tables", {
      method: "POST",
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      decision.tables[HOTEL_ADP_ATTRIBUTES_TABLE].error = json;
      console.error(JSON.stringify(decision, null, 2));
      process.exit(1);
    }
    decision.tables[HOTEL_ADP_ATTRIBUTES_TABLE].tableId = json.id;
  }
}

const outDir = path.join(ROOT, "reports", "hotel-intelligence");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(
  outDir,
  APPLY
    ? "hotel-adp-attributes-schema-apply-2026-09-29.json"
    : "hotel-adp-attributes-schema-dry-run-2026-09-29.json"
);
fs.writeFileSync(outPath, JSON.stringify(decision, null, 2) + "\n");
console.log(JSON.stringify({ ...decision, outPath }, null, 2));
