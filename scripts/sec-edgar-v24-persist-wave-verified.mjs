#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ids = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"),
    "utf8",
  ),
);
const tableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const inv = JSON.parse(
  fs.readFileSync(path.join(ROOT, "data/sec-edgar/discovery-inventory/hotels.json"), "utf8"),
);
const verified = (inv.hotels || []).filter(
  (h) => h.identityClass === "VERIFIED_PROPERTY" && h.discoveryVersion === "sec-edgar-universal-wave-v24",
);
const by = new Map();
for (const h of verified) {
  const k = `${h.normalizedName || h.hotelName}|${h.city}|${h.stateRegion}`.toLowerCase();
  if (!by.has(k)) by.set(k, h);
}
const uniq = [...by.values()];
console.error(`wave verified unique=${uniq.length}`);

async function find(propertyId) {
  const formula = `{Discovery Property ID}="${propertyId}"`;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return json.records?.[0] || null;
}

async function create(fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}

const persisted = [];
const skipped = [];
const errors = [];
for (const h of uniq) {
  const propertyId = h.propertyId || h.discoveryId;
  if (!propertyId) continue;
  const existing = await find(propertyId);
  if (existing) {
    skipped.push(propertyId);
    continue;
  }
  const fields = {
    [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: propertyId,
    [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: h.normalizedName || h.hotelName,
    [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: h.hotelName,
    [MAP_DISCOVERY_INVENTORY.city]: h.city || null,
    [MAP_DISCOVERY_INVENTORY.stateRegion]: h.stateRegion || null,
    [MAP_DISCOVERY_INVENTORY.country]: h.country || "United States",
    [MAP_DISCOVERY_INVENTORY.identityClass]: "VERIFIED_PROPERTY",
    [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: "IDENTITY_VERIFIED",
    [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "DISCOVERY_PERSISTED",
    [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
    [MAP_DISCOVERY_INVENTORY.secRegistrant]: h.registrant || null,
    [MAP_DISCOVERY_INVENTORY.registrantCik]: h.cik || null,
    [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: h.sourceUrl || null,
    [MAP_DISCOVERY_INVENTORY.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
    [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
    [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
    [MAP_DISCOVERY_INVENTORY.discoveryMethod]: h.discoveryMethod || "HTML_PROPERTY_TABLE",
  };
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  const created = await create(fields);
  if (!created.ok) {
    errors.push(created.json);
    continue;
  }
  const rb = await find(propertyId);
  persisted.push({ propertyId, airtableId: created.json.id, readBackOk: Boolean(rb) });
  await new Promise((r) => setTimeout(r, 220));
}

const out = {
  persisted,
  skipped,
  errors,
  counts: {
    persisted: persisted.length,
    skipped: skipped.length,
    errors: errors.length,
    readBacksOk: persisted.filter((x) => x.readBackOk).length,
  },
};
fs.writeFileSync(
  path.join(ROOT, "reports/sec-edgar-universal-wave-v24/AIRTABLE_PERSIST.json"),
  JSON.stringify(out, null, 2),
);
console.log(JSON.stringify(out.counts, null, 2));
