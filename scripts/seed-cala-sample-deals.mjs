#!/usr/bin/env node
/**
 * Seed CALA sample opportunities into Airtable (Deals + linked tables + Target List).
 *
 * Prerequisites: AIRTABLE_API_KEY, AIRTABLE_BASE_ID in .env
 *
 * Usage:
 *   node scripts/dry-run-cala-sample-deal-import.mjs          # review JSON first
 *   node scripts/seed-cala-sample-deals.mjs                   # dry-run summary only
 *   node scripts/seed-cala-sample-deals.mjs --apply             # write to Airtable
 *   node scripts/seed-cala-sample-deals.mjs --apply --clean     # delete prior CALA samples then seed
 *   node scripts/seed-cala-sample-deals.mjs --apply --only merida-centro-select-service
 *
 * Optional env:
 *   CALA_SAMPLE_USER_RECORD_ID=recXXX  — link deals to Users table
 *   CALA_SAMPLE_COMPANY_RECORD_ID=recXXX — link Company Profile
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validateSampleDealRecord } from "../lib/sample-opportunity-deal-schema.js";
import { buildSampleDealImportBundle } from "../lib/sample-deal-airtable-import.js";
import {
  DEALS_TABLE,
  DEALS_STATUS_FIELD,
  LOCATION_PROPERTY_TABLE,
  MARKET_PERFORMANCE_TABLE,
  STRATEGIC_INTENT_TABLE,
  CONTACT_UPLOADS_TABLE,
  LEASE_STRUCTURE_TABLE,
  LOCATION_LINK_FIELD,
  MARKET_PERFORMANCE_LINK_FIELD,
  STRATEGIC_INTENT_LINK_FIELD,
  CONTACT_UPLOADS_LINK_FIELD,
} from "../api/schemas/deal-setup-fields.js";
import { INTAKE_DEALS_USER_LINK } from "../api/schemas/intake-deal-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const FIXTURES_DIR = path.join(ROOT, "fixtures", "sample-deals");
const DRY_RUN_DIR = path.join(ROOT, "data", "cala-sample-import-dry-run");
const RESULTS_PATH = path.join(ROOT, "data", "cala-sample-import-results.json");

const TARGET_LIST_TABLE = process.env.AIRTABLE_TABLE_TARGET_LIST || "Target List";
const SAMPLE_STATUS = process.env.CALA_SAMPLE_DEALS_STATUS || "In Review";
const LEGACY_SAMPLE_STATUS = "Sample — CALA demo";

const CALA_FIXTURES = [
  "proyecto-reforma-urban-conversion.example.json",
  "playa-dorada-resort-repositioning.example.json",
  "cartagena-walled-city-collection.example.json",
  "merida-centro-select-service.example.json",
  "san-juan-bay-turnaround.example.json",
  "panama-city-mixed-use-hotel-component.example.json",
  "aeropuerto-cancun-select-service.example.json",
  "cusco-heritage-palace-hotel.example.json",
  "colonial-city-lifestyle-conversion.example.json",
  "riviera-maya-wellness-resort-repositioning.example.json",
  "andean-business-hotel-reflag.example.json",
  "cascadas-lifestyle-hotel-component.example.json",
];

const DELAY_MS = Math.max(200, parseInt(process.env.CALA_SEED_AIRTABLE_DELAY_MS || "350", 10) || 350);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseArgs(argv) {
  const flags = { apply: false, clean: false, only: null, fromDryRun: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") flags.apply = true;
    else if (a === "--clean") flags.clean = true;
    else if (a === "--from-dry-run") flags.fromDryRun = true;
    else if (a === "--only" && argv[i + 1]) flags.only = argv[++i].replace(/\.example\.json$/, "");
    else if (a.startsWith("--only=")) flags.only = a.slice("--only=".length).replace(/\.example\.json$/, "");
  }
  return flags;
}

function escapeFormula(s) {
  return String(s ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

async function airtableFetch(baseId, apiKey, method, table, body, recordId) {
  const tableEnc = encodeURIComponent(table);
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${tableEnc}/${encodeURIComponent(recordId)}`
    : `https://api.airtable.com/v0/${baseId}/${tableEnc}`;
  const res = await fetch(url, {
    method,
    headers: { Authorization: "Bearer " + apiKey, "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json();
  if (!res.ok || data.error) {
    const msg = data.error?.message || data.error?.type || `HTTP ${res.status}`;
    throw new Error(`${table} ${method}: ${msg}`);
  }
  return data;
}

function loadCalaProjectNames() {
  const names = [];
  for (const file of CALA_FIXTURES) {
    const p = path.join(FIXTURES_DIR, file);
    if (!fs.existsSync(p)) continue;
    const record = JSON.parse(fs.readFileSync(p, "utf8"));
    const name =
      record.fictionalDeal?.projectName ||
      record.fictionalDeal?.fields?.["Project Name"] ||
      record.fictionalDeal?.fields?.["Property Name"];
    if (name) names.push(name);
  }
  return [...new Set(names)];
}

async function findSampleDeals(baseId, apiKey) {
  const projectNames = loadCalaProjectNames();
  const statusParts = [SAMPLE_STATUS, LEGACY_SAMPLE_STATUS]
    .filter((s, i, a) => s && a.indexOf(s) === i)
    .map((s) => `{${DEALS_STATUS_FIELD}} = '${escapeFormula(s)}'`);
  const nameParts = projectNames.map(
    (n) => `{Project Name} = '${escapeFormula(n)}'`
  );
  const formula = `OR(${[...statusParts, ...nameParts].join(", ")})`;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}?filterByFormula=${encodeURIComponent(formula)}&pageSize=100`;
  const ids = [];
  let offset = null;
  do {
    const u = offset ? `${url}&offset=${encodeURIComponent(offset)}` : url;
    const res = await fetch(u, { headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error) throw new Error(data.error.message);
    for (const rec of data.records || []) ids.push(rec.id);
    offset = data.offset;
  } while (offset);
  return ids;
}

async function deleteRecords(baseId, apiKey, table, ids) {
  for (let i = 0; i < ids.length; i += 10) {
    const chunk = ids.slice(i, i + 10);
    const params = chunk.map((id) => `records[]=${encodeURIComponent(id)}`).join("&");
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
    const res = await fetch(url, { method: "DELETE", headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error) throw new Error(`Delete ${table}: ${data.error.message}`);
    await sleep(DELAY_MS);
  }
}

async function deleteTargetsForDeals(baseId, apiKey, dealIds) {
  if (!dealIds.length) return 0;
  const all = [];
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TARGET_LIST_TABLE)}?pageSize=100`;
  let offset = null;
  do {
    const u = offset ? `${url}&offset=${encodeURIComponent(offset)}` : url;
    const res = await fetch(u, { headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error) throw new Error(data.error.message);
    for (const rec of data.records || []) {
      const linked = rec.fields?.Deal_ID;
      if (Array.isArray(linked) && linked.some((id) => dealIds.includes(id))) {
        all.push(rec.id);
      }
    }
    offset = data.offset;
  } while (offset);
  if (all.length) await deleteRecords(baseId, apiKey, TARGET_LIST_TABLE, all);
  return all.length;
}

/**
 * @param {object} bundle
 * @param {string} baseId
 * @param {string} apiKey
 */
async function applyBundle(bundle, baseId, apiKey) {
  const dealFields = { ...bundle.tables.deals.fields };
  dealFields[DEALS_STATUS_FIELD] = SAMPLE_STATUS;

  const userId = process.env.CALA_SAMPLE_USER_RECORD_ID;
  if (userId?.startsWith("rec")) dealFields[INTAKE_DEALS_USER_LINK] = [userId];
  const companyField = process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || "Company Profile";
  const companyId = process.env.CALA_SAMPLE_COMPANY_RECORD_ID;
  if (companyId?.startsWith("rec")) dealFields[companyField] = [companyId];

  const created = await airtableFetch(baseId, apiKey, "POST", DEALS_TABLE, {
    fields: dealFields,
    typecast: true,
  });
  const dealId = created.id;
  await sleep(DELAY_MS);

  const linkPatch = {};

  const locFields = bundle.tables.locationProperty.fields;
  if (Object.keys(locFields).length > 0) {
    const loc = await airtableFetch(baseId, apiKey, "POST", LOCATION_PROPERTY_TABLE, {
      fields: { ...locFields, Deal_ID: [dealId] },
      typecast: true,
    });
    linkPatch[LOCATION_LINK_FIELD] = [loc.id];
    await sleep(DELAY_MS);
  }

  const mpFields = bundle.tables.marketPerformance.fields;
  if (Object.keys(mpFields).length > 0) {
    const mpLink = bundle.tables.marketPerformance.dealLinkField || "Deal_ID";
    const mp = await airtableFetch(baseId, apiKey, "POST", MARKET_PERFORMANCE_TABLE, {
      fields: { ...mpFields, [mpLink]: [dealId] },
      typecast: true,
    });
    linkPatch[MARKET_PERFORMANCE_LINK_FIELD] = [mp.id];
    await sleep(DELAY_MS);
  }

  const siFields = bundle.tables.strategicIntent.fields;
  if (Object.keys(siFields).length > 0) {
    const si = await airtableFetch(baseId, apiKey, "POST", STRATEGIC_INTENT_TABLE, {
      fields: siFields,
      typecast: true,
    });
    linkPatch[STRATEGIC_INTENT_LINK_FIELD] = [si.id];
    await sleep(DELAY_MS);
  }

  const cuFields = bundle.tables.contactUploads.fields;
  if (Object.keys(cuFields).length > 0) {
    const cuLink = bundle.tables.contactUploads.dealLinkField || "Deal_ID";
    const cu = await airtableFetch(baseId, apiKey, "POST", CONTACT_UPLOADS_TABLE, {
      fields: { ...cuFields, [cuLink]: [dealId] },
      typecast: true,
    });
    linkPatch[CONTACT_UPLOADS_LINK_FIELD] = [cu.id];
    await sleep(DELAY_MS);
  }

  const lsFields = bundle.tables.leaseStructure.fields;
  if (Object.keys(lsFields).length > 0) {
    const lsLink = bundle.tables.leaseStructure.dealLinkField || "Deal_ID";
    await airtableFetch(baseId, apiKey, "POST", LEASE_STRUCTURE_TABLE, {
      fields: { ...lsFields, [lsLink]: [dealId] },
      typecast: true,
    });
    await sleep(DELAY_MS);
  }

  if (Object.keys(linkPatch).length > 0) {
    await airtableFetch(baseId, apiKey, "PATCH", DEALS_TABLE, { fields: linkPatch, typecast: true }, dealId);
    await sleep(DELAY_MS);
  }

  const targetIds = [];
  const now = new Date().toISOString();
  for (const row of bundle.targetList) {
    const fields = {
      ...row.fields,
      Deal_ID: [dealId],
      "Added Date": now,
      "Last Updated": now,
    };
    try {
      const tr = await airtableFetch(baseId, apiKey, "POST", TARGET_LIST_TABLE, {
        fields,
        typecast: true,
      });
      targetIds.push(tr.id);
    } catch (e) {
      console.warn(`  Target list skip (${row.fields["Brand Name"]}):`, e.message);
    }
    await sleep(DELAY_MS);
  }

  return { dealId, targetListIds: targetIds, projectName: bundle.projectName };
}

function loadBundle(slug, fromDryRun) {
  if (fromDryRun) {
    const p = path.join(DRY_RUN_DIR, `${slug}.import.json`);
    if (!fs.existsSync(p)) throw new Error(`Missing dry-run bundle: ${p}. Run dry-run script first.`);
    return JSON.parse(fs.readFileSync(p, "utf8"));
  }
  const file = `${slug}.example.json`;
  const record = JSON.parse(fs.readFileSync(path.join(FIXTURES_DIR, file), "utf8"));
  return buildSampleDealImportBundle(record, { fixtureFile: `fixtures/sample-deals/${file}` });
}

async function main() {
  const flags = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;

  let files = CALA_FIXTURES;
  if (flags.only) {
    files = files.filter((f) => f.includes(flags.only));
    if (!files.length) {
      console.error("No fixture for --only", flags.only);
      process.exit(1);
    }
  }

  if (!flags.apply) {
    console.log("Dry-run mode (no --apply). Generating import bundles via dry-run script path…\n");
    const { spawnSync } = await import("node:child_process");
    const args = ["scripts/dry-run-cala-sample-deal-import.mjs"];
    if (flags.only) args.push("--only", flags.only);
    const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit" });
    process.exit(r.status ?? 0);
  }

  if (!baseId || !apiKey) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env before --apply");
    process.exit(1);
  }

  if (flags.clean) {
    console.log("Cleaning prior CALA sample deals…");
    const dealIds = await findSampleDeals(baseId, apiKey);
    const targetN = await deleteTargetsForDeals(baseId, apiKey, dealIds);
    if (dealIds.length) await deleteRecords(baseId, apiKey, DEALS_TABLE, dealIds);
    console.log(`Deleted ${dealIds.length} deal(s), ${targetN} target list row(s).\n`);
  }

  const results = {
    appliedAt: new Date().toISOString(),
    sampleStatus: SAMPLE_STATUS,
    deals: [],
  };

  for (const file of files) {
    const slug = file.replace(/\.example\.json$/, "");
    console.log(`Seeding ${slug}…`);
    const record = JSON.parse(fs.readFileSync(path.join(FIXTURES_DIR, file), "utf8"));
    const v = validateSampleDealRecord(record);
    if (!v.ok) {
      console.error("Validation failed:", v.errors);
      process.exit(1);
    }
    const bundle = flags.fromDryRun ? loadBundle(slug, true) : buildSampleDealImportBundle(record, {
      fixtureFile: `fixtures/sample-deals/${file}`,
    });
    try {
      const out = await applyBundle(bundle, baseId, apiKey);
      results.deals.push({ slug, sampleId: bundle.sampleId, ...out });
      console.log(`  OK deal ${out.dealId} — ${out.projectName} (${out.targetListIds.length} targets)`);
    } catch (e) {
      console.error(`  FAIL ${slug}:`, e.message);
      results.deals.push({ slug, sampleId: bundle.sampleId, error: e.message });
    }
  }

  fs.mkdirSync(path.dirname(RESULTS_PATH), { recursive: true });
  fs.writeFileSync(RESULTS_PATH, JSON.stringify(results, null, 2) + "\n", "utf8");
  console.log("\nResults written:", RESULTS_PATH);

  const failed = results.deals.filter((d) => d.error);
  if (failed.length) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
