/**
 * Create CoStar Companies table in GTM Airtable base.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_COMPANY_TABLE,
  MAP_GTM_COMPANY,
  COSTAR_COMPANY_HEADERS,
  VAL_GTM_COMPANY_OUTREACH_STATUS,
  VAL_GTM_COMPANY_CALA_HOTELS,
} from "../lib/gtm-owner-target/company-field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

const NUMBER_HEADERS = new Set([
  MAP_GTM_COMPANY.employees,
  MAP_GTM_COMPANY.locations,
  MAP_GTM_COMPANY.managedProperties,
  MAP_GTM_COMPANY.ownedProperties,
  MAP_GTM_COMPANY.operatedProperties,
  MAP_GTM_COMPANY.leaseTransactions3Y,
  MAP_GTM_COMPANY.leaseTransactionsSf3Y,
  MAP_GTM_COMPANY.leaseListings,
  MAP_GTM_COMPANY.leaseListingsPortfolioSf,
  MAP_GTM_COMPANY.leaseListingsAvailableSf,
  MAP_GTM_COMPANY.saleTransactions3Y,
  MAP_GTM_COMPANY.saleTransactionsSf3Y,
  MAP_GTM_COMPANY.saleTransactionsVolume3Y,
  MAP_GTM_COMPANY.saleListings,
  MAP_GTM_COMPANY.saleListingsSf,
]);

function fieldForHeader(header) {
  if (header === MAP_GTM_COMPANY.website) return { name: header, type: "url" };
  if (NUMBER_HEADERS.has(header)) return { name: header, type: "number", options: { precision: 0 } };
  return { name: header, type: "singleLineText" };
}

function buildFields() {
  const fields = COSTAR_COMPANY_HEADERS.map(fieldForHeader);
  fields.push({
    name: MAP_GTM_COMPANY.companyDedupeKey,
    type: "singleLineText",
    description: "Stable dedupe key: company + HQ city + HQ country.",
  });
  fields.push({ name: MAP_GTM_COMPANY.sourceFile, type: "singleLineText" });
  fields.push({ name: MAP_GTM_COMPANY.companyOverview, type: "multilineText" });
  fields.push({
    name: MAP_GTM_COMPANY.outreachStatus,
    type: "singleSelect",
    options: { choices: VAL_GTM_COMPANY_OUTREACH_STATUS.map((name) => ({ name })) },
  });
  fields.push({ name: MAP_GTM_COMPANY.internalNotes, type: "multilineText" });
  fields.push({
    name: MAP_GTM_COMPANY.calaHotels,
    type: "singleSelect",
    options: { choices: VAL_GTM_COMPANY_CALA_HOTELS.map((name) => ({ name })) },
    description: "Derived from Properties True Owner footprint — CALA hotel presence.",
  });
  fields.push({
    name: MAP_GTM_COMPANY.calaPropertyCount,
    type: "number",
    options: { precision: 0 },
  });
  fields.push({ name: MAP_GTM_COMPANY.calaCountriesSummary, type: "singleLineText" });
  fields.push({ name: MAP_GTM_COMPANY.matchedOwnerName, type: "singleLineText" });
  fields.push({ name: MAP_GTM_COMPANY.calaMatchType, type: "singleLineText" });
  return fields;
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", ...(init.headers || {}) },
  });
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  return { res, json };
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const { res, json } = await metaFetch(baseId, apiKey, "/tables");
  if (!res.ok) throw new Error(JSON.stringify(json));

  const tables = json.tables || [];
  let table = tables.find((t) => t.name === GTM_COMPANY_TABLE);
  const fieldSpecs = buildFields();
  const report = { mode: APPLY ? "apply" : "dry-run", baseId, tableName: GTM_COMPANY_TABLE, createdTable: false, fieldsCreated: [], fieldsWouldCreate: [], fieldsSkipped: [], fieldsFailed: [] };

  if (!table) {
    if (!APPLY) {
      report.wouldCreateTable = true;
      report.fieldsWouldCreate = fieldSpecs.map((f) => f.name);
      console.log("WOULD CREATE TABLE", GTM_COMPANY_TABLE);
    } else {
      const primary = fieldSpecs.find((f) => f.name === MAP_GTM_COMPANY.company) || fieldSpecs[0];
      const rest = fieldSpecs.filter((f) => f.name !== primary.name);
      const cr = await metaFetch(baseId, apiKey, "/tables", {
        method: "POST",
        body: JSON.stringify({ name: GTM_COMPANY_TABLE, description: "CoStar Companies Data Export — internal GTM only.", fields: [primary, ...rest] }),
      });
      if (!cr.res.ok) throw new Error(JSON.stringify(cr.json));
      table = cr.json;
      report.createdTable = true;
      report.tableId = cr.json.id;
      report.fieldsCreated = (cr.json.fields || []).map((f) => f.name);
      console.log("CREATED TABLE", cr.json.name, cr.json.id);
    }
  } else {
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const spec of fieldSpecs) {
      if (existing.has(spec.name)) { report.fieldsSkipped.push(spec.name); continue; }
      if (!APPLY) { report.fieldsWouldCreate.push(spec.name); continue; }
      const fr = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, { method: "POST", body: JSON.stringify(spec) });
      if (!fr.res.ok) report.fieldsFailed.push({ name: spec.name, error: fr.json });
      else report.fieldsCreated.push(spec.name);
    }
    console.log("TABLE EXISTS", table.name, table.id);
  }

  const out = path.join(ROOT, "reports", "ensure-gtm-costar-companies-table.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log("Wrote", out);
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
