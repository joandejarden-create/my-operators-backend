#!/usr/bin/env node
/**
 * Import real pilot deals from fixtures/pilot-deals/*.json into Airtable.
 *
 *   node scripts/seed-pilot-deal.mjs --slug altiplano-mijares-32
 *   node scripts/seed-pilot-deal.mjs --apply --update --slug altiplano-mijares-32
 *
 * Env overrides:
 *   PILOT_DEAL_STATUS=Under Review
 *   PILOT_DEAL_USER_RECORD_ID=recXXX
 *   PILOT_DEAL_COMPANY_RECORD_ID=recXXX
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validateSampleDealRecord } from "../lib/sample-opportunity-deal-schema.js";
import {
  buildSampleDealImportBundle,
  bundleFieldStats,
} from "../lib/sample-deal-airtable-import.js";
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
  LEASE_STRUCTURE_LINK_FIELD,
} from "../api/schemas/deal-setup-fields.js";
import { INTAKE_DEALS_USER_LINK } from "../api/schemas/intake-deal-fields.js";
import { DEALS_COMPANY_LINK_FIELD } from "../lib/pilot-provisioning/pilot-field-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const MANIFEST_PATH = path.join(ROOT, "data", "pilot-deals-seed.json");
const DRAFT_DIR = path.join(ROOT, "data", "pilot-import-drafts");
const RESULTS_PATH = path.join(ROOT, "data", "pilot-deal-import-results.json");

const DELAY_MS = Math.max(200, parseInt(process.env.PILOT_SEED_AIRTABLE_DELAY_MS || "350", 10) || 350);
const PILOT_DEAL_STATUS =
  process.env.PILOT_DEAL_STATUS ||
  process.env.PILOT_DEALS_STATUS ||
  "Under Review";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function parseArgs(argv) {
  const flags = { apply: false, slug: null, update: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") flags.apply = true;
    else if (a === "--update") flags.update = true;
    else if (a === "--slug" && argv[i + 1]) flags.slug = argv[++i];
    else if (a.startsWith("--slug=")) flags.slug = a.slice("--slug=".length);
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

async function findDealByProjectName(baseId, apiKey, projectName) {
  const formula = `{Project Name} = '${escapeFormula(projectName)}'`;
  const url =
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}` +
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=3`;
  const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  const data = await res.json();
  if (data.error) throw new Error(data.error.message);
  return data.records || [];
}

async function applyBundle(bundle, baseId, apiKey, { userId, companyId, dealStatus }) {
  const dealFields = { ...bundle.tables.deals.fields };
  dealFields[DEALS_STATUS_FIELD] = dealStatus;

  if (userId?.startsWith("rec")) dealFields[INTAKE_DEALS_USER_LINK] = [userId];
  const companyField = process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || DEALS_COMPANY_LINK_FIELD;
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

  return { dealId, projectName: bundle.projectName };
}

const LINKED_TABLE_PATCHES = [
  { linkField: LOCATION_LINK_FIELD, table: LOCATION_PROPERTY_TABLE, bundleKey: "locationProperty" },
  { linkField: MARKET_PERFORMANCE_LINK_FIELD, table: MARKET_PERFORMANCE_TABLE, bundleKey: "marketPerformance" },
  { linkField: STRATEGIC_INTENT_LINK_FIELD, table: STRATEGIC_INTENT_TABLE, bundleKey: "strategicIntent" },
  { linkField: CONTACT_UPLOADS_LINK_FIELD, table: CONTACT_UPLOADS_TABLE, bundleKey: "contactUploads" },
  { linkField: LEASE_STRUCTURE_LINK_FIELD, table: LEASE_STRUCTURE_TABLE, bundleKey: "leaseStructure" },
];

function firstLinkedId(fields, linkField) {
  const val = fields?.[linkField];
  if (Array.isArray(val) && val.length) {
    const id = typeof val[0] === "string" ? val[0] : val[0]?.id;
    if (id?.startsWith("rec")) return id;
  }
  return null;
}

async function updateExistingDeal(dealId, bundle, baseId, apiKey) {
  const dealRec = await airtableFetch(baseId, apiKey, "GET", DEALS_TABLE, null, dealId);
  const dealFields = { ...bundle.tables.deals.fields };
  delete dealFields[DEALS_STATUS_FIELD];
  delete dealFields[INTAKE_DEALS_USER_LINK];
  const companyField = process.env.AIRTABLE_DEALS_COMPANY_LINK_FIELD || DEALS_COMPANY_LINK_FIELD;
  delete dealFields[companyField];
  for (const spec of LINKED_TABLE_PATCHES) {
    delete dealFields[spec.linkField];
  }
  if (Object.keys(dealFields).length) {
    await airtableFetch(baseId, apiKey, "PATCH", DEALS_TABLE, { fields: dealFields, typecast: true }, dealId);
    await sleep(DELAY_MS);
  }

  for (const spec of LINKED_TABLE_PATCHES) {
    const payload = bundle.tables[spec.bundleKey]?.fields || {};
    if (!Object.keys(payload).length) continue;
    const childId = firstLinkedId(dealRec.fields, spec.linkField);
    if (!childId) {
      throw new Error(`Missing linked ${spec.table} on deal ${dealId}`);
    }
    await airtableFetch(baseId, apiKey, "PATCH", spec.table, { fields: payload, typecast: true }, childId);
    await sleep(DELAY_MS);
  }

  return { dealId, projectName: bundle.projectName, mode: "update" };
}

function resolveIds(entry, defaults) {
  return {
    userId:
      process.env.PILOT_DEAL_USER_RECORD_ID ||
      process.env.CALA_SAMPLE_USER_RECORD_ID ||
      entry.userRecordId ||
      defaults.userRecordId ||
      "",
    companyId:
      process.env.PILOT_DEAL_COMPANY_RECORD_ID ||
      process.env.CALA_SAMPLE_COMPANY_RECORD_ID ||
      entry.companyProfileId ||
      defaults.companyProfileId ||
      "",
    dealStatus: process.env.PILOT_DEAL_STATUS || entry.dealStatus || defaults.dealStatus || PILOT_DEAL_STATUS,
  };
}

async function main() {
  const flags = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;

  if (!fs.existsSync(MANIFEST_PATH)) {
    console.error("Missing manifest:", MANIFEST_PATH);
    process.exit(1);
  }

  const manifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"));
  const defaults = manifest.defaults || {};
  let deals = manifest.deals || [];
  if (flags.slug) {
    deals = deals.filter((d) => d.slug === flags.slug || d.fixtureFile?.includes(flags.slug));
    if (!deals.length) {
      console.error("No manifest entry for --slug", flags.slug);
      process.exit(1);
    }
  }

  const results = {
    generatedAt: new Date().toISOString(),
    mode: flags.apply ? "apply" : "dry-run",
    dealStatus: PILOT_DEAL_STATUS,
    deals: [],
  };

  for (const entry of deals) {
    const fixturePath = path.join(ROOT, entry.fixtureFile);
    if (!fs.existsSync(fixturePath)) {
      console.error("Missing fixture:", fixturePath);
      process.exit(1);
    }

    const record = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    const validation = validateSampleDealRecord(record);
    if (!validation.ok) {
      console.error("Validation failed:", validation.errors);
      process.exit(1);
    }

    const bundle = buildSampleDealImportBundle(record, { fixtureFile: entry.fixtureFile });
    const stats = bundleFieldStats(bundle);
    const ids = resolveIds(entry, defaults);

    fs.mkdirSync(DRAFT_DIR, { recursive: true });
    const draftPath = path.join(DRAFT_DIR, `${entry.slug || "pilot"}.import.json`);
    fs.writeFileSync(draftPath, JSON.stringify(bundle, null, 2) + "\n");

    const result = {
      pilotId: entry.pilotId,
      slug: entry.slug,
      projectName: bundle.projectName || entry.projectName,
      fixtureFile: entry.fixtureFile,
      draftPath,
      stats,
      userRecordId: ids.userId,
      companyProfileId: ids.companyId,
      dealStatus: ids.dealStatus,
      validationWarnings: validation.warnings,
    };

    console.log(`\n${entry.slug}: ${result.projectName}`);
    console.log("  stats:", JSON.stringify(stats));
    console.log("  draft:", draftPath);
    console.log("  link User_ID:", ids.userId || "(none)");
    console.log("  link Company Profile:", ids.companyId || "(none)");
    console.log("  Deal Status:", ids.dealStatus);

    if (flags.apply) {
      if (!baseId || !apiKey) {
        console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID before --apply");
        process.exit(1);
      }
      if (!ids.companyId?.startsWith("rec")) {
        console.error("Company Profile record id required for pilot apply");
        process.exit(1);
      }

      const existing = await findDealByProjectName(baseId, apiKey, result.projectName);
      const updateDealId =
        process.env.PILOT_DEAL_RECORD_ID ||
        entry.dealAirtableId ||
        (existing.length ? existing[0].id : null);

      if (flags.update) {
        if (!updateDealId?.startsWith("rec")) {
          console.error("  FAIL: --update requires dealAirtableId in manifest or existing deal by name");
          result.status = "error";
          result.error = "missing deal id for update";
        } else {
          try {
            const out = await updateExistingDeal(updateDealId, bundle, baseId, apiKey);
            result.status = "updated";
            result.dealId = out.dealId;
            console.log("  UPDATED deal", out.dealId);
          } catch (err) {
            result.status = "error";
            result.error = err.message;
            console.error("  FAIL:", err.message);
          }
        }
      } else if (existing.length) {
        result.status = "skipped";
        result.existingDealIds = existing.map((r) => r.id);
        console.log("  SKIP: deal already exists:", result.existingDealIds.join(", "));
      } else {
        try {
          const out = await applyBundle(bundle, baseId, apiKey, ids);
          result.status = "ok";
          result.dealId = out.dealId;
          console.log("  OK deal", out.dealId);
        } catch (err) {
          result.status = "error";
          result.error = err.message;
          console.error("  FAIL:", err.message);
        }
      }
    }

    results.deals.push(result);
  }

  fs.mkdirSync(path.dirname(RESULTS_PATH), { recursive: true });
  fs.writeFileSync(RESULTS_PATH, JSON.stringify(results, null, 2) + "\n");
  console.log("\nResults:", RESULTS_PATH);
  console.log(flags.apply ? "Applied to Airtable." : "Dry run only — pass --apply to write.");

  const failed = results.deals.filter((d) => d.status === "error");
  if (failed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
