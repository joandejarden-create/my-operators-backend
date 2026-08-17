#!/usr/bin/env node
/**
 * Seed all 22 Operator Explorer DNA JSON fields on new-base split tables.
 *
 *   node scripts/seed-operator-dna-explorer-json-data.mjs
 *   node scripts/seed-operator-dna-explorer-json-data.mjs --apply
 *   node scripts/seed-operator-dna-explorer-json-data.mjs --apply --force
 *   node scripts/seed-operator-dna-explorer-json-data.mjs --apply --master recXXXXXXXX
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  DNA_EXPLORER_JSON_TABLE,
  DNA_EXPLORER_JSON_FIELD_SPECS,
} from "../lib/operator-dna-explorer-json-fields.js";
import {
  buildProfileDnaJsonSeedFields,
  buildPlatformDnaJsonSeedFields,
  buildCommercialDnaJsonSeedFields,
  hasDnaJsonSeed,
  dnaExplorerJsonKeysForTable,
  DNA_JSON_FIELD_TO_TAB,
} from "../lib/operator-dna-explorer-json-seed-data.js";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PROFILE_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_COMMERCIAL_TABLE,
  fetchAllRecordsRest,
  airtableFetchJson,
  buildPrefillObjectFromNewBaseRows,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const masterArg = process.argv.find((a) => a.startsWith("--master="));
const MASTER_FILTER = masterArg ? masterArg.split("=")[1] : null;

function enc(s) {
  return encodeURIComponent(s);
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function masterIdFromRow(fields) {
  const op = fields && fields.Operator;
  return Array.isArray(op) && op[0] ? String(op[0]) : "";
}

async function patchTable(tableName, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(tableName)}/${enc(recordId)}`;
  return airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
}

function seedFieldsForTable(tableName, ctx) {
  if (tableName === DNA_EXPLORER_JSON_TABLE.PROFILE) {
    return buildProfileDnaJsonSeedFields(ctx);
  }
  if (tableName === DNA_EXPLORER_JSON_TABLE.PLATFORM) {
    return buildPlatformDnaJsonSeedFields(ctx);
  }
  if (tableName === DNA_EXPLORER_JSON_TABLE.COMMERCIAL) {
    return buildCommercialDnaJsonSeedFields();
  }
  return {};
}

function filterEmptyOnly(seedFields, existing, force) {
  if (force) return seedFields;
  const out = {};
  for (const [k, v] of Object.entries(seedFields)) {
    if (nz(existing[k])) continue;
    out[k] = v;
  }
  return out;
}

async function verifyMaster(master, profile, platform, commercial) {
  const prefill = buildPrefillObjectFromNewBaseRows(
    master,
    profile,
    platform,
    commercial,
    null
  );
  const missing = [];
  for (const spec of DNA_EXPLORER_JSON_FIELD_SPECS) {
    if (!nz(prefill[spec.formKey])) {
      missing.push({ formKey: spec.formKey, tab: DNA_JSON_FIELD_TO_TAB[spec.formKey] });
    }
  }
  return missing;
}

async function main() {
  const [masters, profileRows, platformRows, commercialRows] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_PROFILE_TABLE),
    fetchAllRecordsRest(NEW_BASE_PLATFORM_TABLE),
    fetchAllRecordsRest(NEW_BASE_COMMERCIAL_TABLE),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    masterNameById.set(m.id, nz(f.company_name) || nz(f["Company Name"]) || m.id);
  }

  const profileByMaster = new Map();
  for (const row of profileRows) {
    const mid = masterIdFromRow(row.fields);
    if (mid && !profileByMaster.has(mid)) profileByMaster.set(mid, row);
  }
  const platformByMaster = new Map();
  for (const row of platformRows) {
    const mid = masterIdFromRow(row.fields);
    if (mid && !platformByMaster.has(mid)) platformByMaster.set(mid, row);
  }
  const commercialByMaster = new Map();
  for (const row of commercialRows) {
    const mid = masterIdFromRow(row.fields);
    if (mid && !commercialByMaster.has(mid)) commercialByMaster.set(mid, row);
  }

  const plan = [];
  const verify = [];

  let index = 0;
  for (const master of masters) {
    if (MASTER_FILTER && master.id !== MASTER_FILTER) continue;

    const companyName = masterNameById.get(master.id) || master.id;
    const ctx = { index, companyName, existingFields: {} };
    const profile = profileByMaster.get(master.id);
    const platform = platformByMaster.get(master.id);
    const commercial = commercialByMaster.get(master.id);

    const patches = [];

    for (const tableName of [
      DNA_EXPLORER_JSON_TABLE.PROFILE,
      DNA_EXPLORER_JSON_TABLE.PLATFORM,
      DNA_EXPLORER_JSON_TABLE.COMMERCIAL,
    ]) {
      let row = null;
      if (tableName === DNA_EXPLORER_JSON_TABLE.PROFILE) row = profile;
      if (tableName === DNA_EXPLORER_JSON_TABLE.PLATFORM) row = platform;
      if (tableName === DNA_EXPLORER_JSON_TABLE.COMMERCIAL) row = commercial;
      if (!row) {
        plan.push({ masterId: master.id, companyName, table: tableName, action: "no-linked-row" });
        continue;
      }

      const existing = row.fields || {};
      ctx.existingFields = existing;
      if (hasDnaJsonSeed(existing, tableName) && !FORCE) {
        plan.push({ masterId: master.id, companyName, table: tableName, action: "skip" });
        continue;
      }

      const seedAll = seedFieldsForTable(tableName, ctx);
      const payload = filterEmptyOnly(seedAll, existing, FORCE);
      const keys = Object.keys(payload);
      if (!keys.length) {
        plan.push({ masterId: master.id, companyName, table: tableName, action: "skip-no-empty-fields" });
        continue;
      }

      plan.push({
        masterId: master.id,
        companyName,
        table: tableName,
        recordId: row.id,
        action: APPLY ? "patch" : "would-patch",
        fieldKeys: keys,
      });

      if (APPLY) {
        const { ok, status, json } = await patchTable(tableName, row.id, payload);
        if (!ok) {
          console.error("PATCH FAILED", companyName, tableName, status, JSON.stringify(json));
          process.exitCode = 1;
        } else {
          console.log("PATCHED", companyName, tableName, keys.length, "fields");
          Object.assign(existing, payload);
        }
        await new Promise((r) => setTimeout(r, 220));
      }

      patches.push({ tableName, keys });
    }

    if (APPLY && profile && platform && commercial) {
      const missing = await verifyMaster(
        master,
        { fields: profile.fields },
        { fields: platform.fields },
        { fields: commercial.fields }
      );
      verify.push({
        masterId: master.id,
        companyName,
        missingCount: missing.length,
        missing,
      });
    }

    index += 1;
  }

  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const outPath = path.join(REPORTS, `operator-dna-explorer-json-seed-${stamp}.json`);
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        apply: APPLY,
        force: FORCE,
        fieldCount: DNA_EXPLORER_JSON_FIELD_SPECS.length,
        keysByTable: {
          profile: dnaExplorerJsonKeysForTable(DNA_EXPLORER_JSON_TABLE.PROFILE),
          platform: dnaExplorerJsonKeysForTable(DNA_EXPLORER_JSON_TABLE.PLATFORM),
          commercial: dnaExplorerJsonKeysForTable(DNA_EXPLORER_JSON_TABLE.COMMERCIAL),
        },
        fieldToDnaTab: DNA_JSON_FIELD_TO_TAB,
        plan,
        verify,
      },
      null,
      2
    )
  );
  console.log("\nPlan:", outPath);
  if (verify.length) {
    const bad = verify.filter((v) => v.missingCount > 0);
    console.log("Prefill verify:", verify.length, "operators;", bad.length, "with missing keys after seed");
    if (bad.length) console.log(JSON.stringify(bad.slice(0, 3), null, 2));
  }
  if (!APPLY) console.log("Dry run. Re-run with --apply (add --force to overwrite existing JSON).");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
