#!/usr/bin/env node
/**
 * Seed Operator Setup - Explorer Materials rows (Brand Explorer Presentation parity).
 *
 *   node scripts/create-operator-setup-explorer-materials-table.mjs
 *   node scripts/seed-operator-explorer-materials-presentation.mjs
 *   node scripts/seed-operator-explorer-materials-presentation.mjs --apply
 *   node scripts/seed-operator-explorer-materials-presentation.mjs --apply --master recTUjuDxL96yWcQA
 *   node scripts/seed-operator-explorer-materials-presentation.mjs --apply --fixture fixtures/operator-materials-presentation-antillano-norte.json
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { buildOperatorMaterialsPresentationRows } from "../lib/operator-materials-explorer-presentation-seed-data.js";
import {
  OPERATOR_EXPLORER_MATERIALS_TABLE,
} from "../api/lib/operator-materials-explorer-presentation-map.js";
import {
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
  rowsLinkedToMaster,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const REPLACE = !process.argv.includes("--only-missing");
const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");
const fixtureArg = process.argv.find((a, i) => process.argv[i - 1] === "--fixture");

const LINK_FIELD_CANDIDATES = ["Operator", "Operator Setup - Master", "Operator Setup - Master Link"];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function loadFixture(filePath) {
  const abs = path.isAbsolute(filePath) ? filePath : path.join(ROOT, filePath);
  const raw = JSON.parse(fs.readFileSync(abs, "utf8"));
  return {
    masterId: nz(raw.masterId),
    companyName: nz(raw.companyName),
    rows: Array.isArray(raw.rows) ? raw.rows : [],
  };
}

function buildAirtableFields(masterId, companyName, row) {
  const fields = {
    Operator: [masterId],
    "Slot Key": row.slotKey,
    Title: row.title ?? "",
    Body: row.body ?? "",
    "Sort Order": typeof row.sort === "number" ? row.sort : Number(row.sort) || 0,
    Active: row.active !== false,
  };
  if (companyName) fields["Company Name"] = companyName;
  const imageUrl = nz(row.imageUrl);
  if (imageUrl && /^https?:\/\//i.test(imageUrl)) {
    fields.Image = [{ url: imageUrl }];
  }
  return fields;
}

async function deleteRecords(base, ids) {
  for (let i = 0; i < ids.length; i += 10) {
    await base(OPERATOR_EXPLORER_MATERIALS_TABLE).destroy(ids.slice(i, i + 10));
  }
}

async function createRows(base, masterId, companyName, rows) {
  let lastErr;
  for (const linkField of LINK_FIELD_CANDIDATES) {
    const payload = rows.map((r) => ({
      fields: { ...buildAirtableFields(masterId, companyName, r), [linkField]: [masterId] },
    }));
    try {
      const created = [];
      for (let i = 0; i < payload.length; i += 10) {
        const chunk = payload.slice(i, i + 10);
        const out = await base(OPERATOR_EXPLORER_MATERIALS_TABLE).create(chunk);
        created.push(...out.map((rec) => rec.id));
      }
      return { linkField, created };
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("Could not create materials rows (link field or table schema mismatch).");
}

async function main() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");

  const fixture = fixtureArg ? loadFixture(fixtureArg) : null;
  const base = new Airtable({ apiKey: key }).base(baseId);

  let masters = await fetchAllRecordsRest(NEW_BASE_MASTER_TABLE);
  if (masterArg || fixture?.masterId) {
    const filterId = fixture?.masterId || masterArg;
    masters = masters.filter((m) => m.id === filterId);
    if (!masters.length) throw new Error(`Master not found: ${filterId}`);
  }

  let allMaterials = [];
  try {
    allMaterials = await fetchAllRecordsRest(OPERATOR_EXPLORER_MATERIALS_TABLE);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (/not found|404|INVALID/i.test(msg)) {
      console.error(
        `Table "${OPERATOR_EXPLORER_MATERIALS_TABLE}" missing. Run:\n  node scripts/create-operator-setup-explorer-materials-table.mjs`
      );
      process.exit(1);
    }
    throw e;
  }

  const plan = [];
  let index = 0;

  for (const master of masters) {
    const mf = master.fields || {};
    const companyName =
      fixture?.companyName ||
      nz(mf.company_name) ||
      nz(mf["Company Name"]) ||
      nz(mf.companyName) ||
      master.id;

    const linked = rowsLinkedToMaster(allMaterials, master.id);
    if (linked.length && !FORCE && !fixture && !REPLACE) {
      plan.push({ masterId: master.id, companyName, action: "skip", reason: "rows exist (use --force or --replace)" });
      index += 1;
      continue;
    }

    const rows =
      fixture && (!fixture.masterId || fixture.masterId === master.id)
        ? fixture.rows
        : buildOperatorMaterialsPresentationRows({ companyName, index });

    plan.push({
      masterId: master.id,
      companyName,
      action: APPLY ? "seed" : "would-seed",
      rowCount: rows.length,
      deleteCount: REPLACE && linked.length ? linked.length : 0,
    });

    if (APPLY) {
      if (REPLACE && linked.length) {
        await deleteRecords(
          base,
          linked.map((r) => r.id)
        );
        console.log("DELETED", linked.length, "row(s) for", companyName);
      }
      const { linkField, created } = await createRows(base, master.id, companyName, rows);
      console.log("CREATED", created.length, "row(s) for", companyName, "via", linkField);
      await new Promise((r) => setTimeout(r, 220));
    }

    index += 1;
  }

  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const outPath = path.join(REPORTS, `operator-explorer-materials-presentation-seed-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ apply: APPLY, force: FORCE, replace: REPLACE, plan }, null, 2));
  console.log("\nPlan written:", outPath);
  if (!APPLY) console.log("\nDry run. Re-run with --apply.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
