/**
 * Create CoStar Contacts table in GTM Airtable base.
 *
 *   node scripts/ensure-gtm-costar-contacts-table.mjs
 *   node scripts/ensure-gtm-costar-contacts-table.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
  COSTAR_CONTACT_HEADERS,
  VAL_GTM_CONTACT_OUTREACH_STATUS,
  VAL_GTM_CONTACT_CALA_HOTEL,
} from "../lib/gtm-owner-target/contact-field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

const NUMBER_HEADERS = new Set([
  MAP_GTM_CONTACT.leaseTransactions3Y,
  MAP_GTM_CONTACT.leaseTransactionsSf3Y,
  MAP_GTM_CONTACT.leaseListings,
  MAP_GTM_CONTACT.leaseListingsPortfolioSf,
  MAP_GTM_CONTACT.leaseListingsAvailableSf,
  MAP_GTM_CONTACT.saleTransactions3Y,
  MAP_GTM_CONTACT.saleTransactionsSf3Y,
  MAP_GTM_CONTACT.saleTransactionsVolume3Y,
  MAP_GTM_CONTACT.saleListings,
  MAP_GTM_CONTACT.saleListingsSf,
]);

function airtableFieldForHeader(header) {
  if (header === MAP_GTM_CONTACT.email) return { name: header, type: "email" };
  if (header === MAP_GTM_CONTACT.phone) return { name: header, type: "phoneNumber" };
  if (header === MAP_GTM_CONTACT.linkedIn || header === MAP_GTM_CONTACT.website) {
    return { name: header, type: "url" };
  }
  if (NUMBER_HEADERS.has(header)) {
    return { name: header, type: "number", options: { precision: 0 } };
  }
  return { name: header, type: "singleLineText" };
}

function buildContactTableFields() {
  const fields = COSTAR_CONTACT_HEADERS.map(airtableFieldForHeader);
  fields.push({
    name: MAP_GTM_CONTACT.contactDedupeKey,
    type: "singleLineText",
    description: "Stable dedupe key (email or name+company+phone).",
  });
  fields.push({ name: MAP_GTM_CONTACT.sourceFile, type: "singleLineText" });
  fields.push({
    name: MAP_GTM_CONTACT.outreachStatus,
    type: "singleSelect",
    options: { choices: VAL_GTM_CONTACT_OUTREACH_STATUS.map((name) => ({ name })) },
  });
  fields.push({ name: MAP_GTM_CONTACT.internalNotes, type: "multilineText" });
  fields.push({
    name: MAP_GTM_CONTACT.calaHotelContact,
    type: "singleSelect",
    options: { choices: VAL_GTM_CONTACT_CALA_HOTEL.map((name) => ({ name })) },
    description: "Derived from Owner Target property footprint — CALA hotel presence.",
  });
  fields.push({
    name: MAP_GTM_CONTACT.calaPropertyCount,
    type: "number",
    options: { precision: 0 },
  });
  fields.push({ name: MAP_GTM_CONTACT.calaCountriesSummary, type: "singleLineText" });
  fields.push({ name: MAP_GTM_CONTACT.matchedOwnerName, type: "singleLineText" });
  fields.push({ name: MAP_GTM_CONTACT.calaMatchType, type: "singleLineText" });
  return fields;
}

async function metaFetch(baseId, token, metaPath, init = {}) {
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

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  let table = tables.find((t) => t.name === GTM_CONTACT_TABLE);
  const fieldSpecs = buildContactTableFields();
  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    tableName: GTM_CONTACT_TABLE,
    tableId: table?.id || null,
    createdTable: false,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  if (!table) {
    if (!APPLY) {
      report.wouldCreateTable = true;
      report.fieldsWouldCreate = fieldSpecs.map((f) => f.name);
      console.log("WOULD CREATE TABLE", GTM_CONTACT_TABLE, `(${fieldSpecs.length} fields)`);
    } else {
      const primary = fieldSpecs.find((f) => f.name === MAP_GTM_CONTACT.name) || fieldSpecs[0];
      const rest = fieldSpecs.filter((f) => f.name !== primary.name);
      const { res, json } = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: GTM_CONTACT_TABLE,
          description: "CoStar Contact Data Export — internal GTM only.",
          fields: [primary, ...rest],
        }),
      });
      if (!res.ok) throw new Error(`Create table failed: ${JSON.stringify(json)}`);
      table = json;
      report.createdTable = true;
      report.tableId = json.id;
      report.fieldsCreated = (json.fields || []).map((f) => f.name);
      console.log("CREATED TABLE", json.name, json.id);
    }
  } else {
    console.log("TABLE EXISTS", table.name, table.id);
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const fieldSpec of fieldSpecs) {
      if (existing.has(fieldSpec.name)) {
        report.fieldsSkipped.push(fieldSpec.name);
        continue;
      }
      if (!APPLY) {
        report.fieldsWouldCreate.push(fieldSpec.name);
        continue;
      }
      const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
        method: "POST",
        body: JSON.stringify(fieldSpec),
      });
      if (!res.ok) {
        report.fieldsFailed.push({ name: fieldSpec.name, error: json });
        console.error("FIELD FAILED", fieldSpec.name, JSON.stringify(json));
      } else {
        report.fieldsCreated.push(fieldSpec.name);
        console.log("CREATED FIELD", fieldSpec.name);
      }
    }
  }

  const outPath = path.join(ROOT, "reports", "ensure-gtm-costar-contacts-table.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
