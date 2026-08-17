#!/usr/bin/env node
/**
 * Ensure Operator Materials fields on Governance, then seed JSON + gallery images.
 *
 *   node scripts/ensure-operator-materials-explorer-schema.mjs --apply
 *   node scripts/seed-operator-materials-explorer-data.mjs
 *   node scripts/seed-operator-materials-explorer-data.mjs --apply
 *   node scripts/seed-operator-materials-explorer-data.mjs --apply --master recTUjuDxL96yWcQA
 *   node scripts/seed-operator-materials-explorer-data.mjs --apply --fixture fixtures/operator-materials-antillano-norte.json
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildOperatorMaterialsSeedFields } from "../lib/operator-materials-explorer-seed-data.js";
import {
  NEW_BASE_GOVERNANCE_TABLE,
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");
const fixtureArg = process.argv.find((a, i) => process.argv[i - 1] === "--fixture");

const SEED_KEYS = ["operator_materials_json", "operator_materials_gallery_json", "diligenceDocumentLinks"];

function enc(s) {
  return encodeURIComponent(s);
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function hasMaterialsSeed(fields) {
  return SEED_KEYS.some((k) => nz(fields[k]));
}

function masterIdFromGovernance(fields) {
  const op = fields && fields.Operator;
  return Array.isArray(op) && op[0] ? String(op[0]) : "";
}

function loadFixture(filePath) {
  const abs = path.isAbsolute(filePath) ? filePath : path.join(ROOT, filePath);
  const raw = JSON.parse(fs.readFileSync(abs, "utf8"));
  const materials = raw.operator_materials_json;
  const fields = {
    operator_materials_json:
      typeof materials === "string" ? materials : JSON.stringify(materials, null, 2),
  };
  if (raw.operator_materials_gallery_json) {
    fields.operator_materials_gallery_json =
      typeof raw.operator_materials_gallery_json === "string"
        ? raw.operator_materials_gallery_json
        : JSON.stringify(raw.operator_materials_gallery_json, null, 2);
  } else if (materials && Array.isArray(materials.gallery)) {
    fields.operator_materials_gallery_json = JSON.stringify(materials.gallery, null, 2);
  }
  if (raw.diligenceDocumentLinks) {
    fields.diligenceDocumentLinks = String(raw.diligenceDocumentLinks);
  }
  return {
    masterId: raw.masterId || "",
    companyName: raw.companyName || "",
    fields,
  };
}

async function patchGovernanceRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_GOVERNANCE_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok, status, json };
}

async function main() {
  const fixture = fixtureArg ? loadFixture(fixtureArg) : null;

  const [masters, governanceRows] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_GOVERNANCE_TABLE),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    const name =
      nz(f.company_name) || nz(f["Company Name"]) || nz(f.companyName) || m.id;
    masterNameById.set(m.id, name);
  }

  let targets = governanceRows.filter((r) => masterIdFromGovernance(r.fields));
  const filterMaster = fixture?.masterId || masterArg;
  if (filterMaster) {
    targets = targets.filter((r) => masterIdFromGovernance(r.fields) === filterMaster);
    if (!targets.length) {
      throw new Error(`No Governance row linked to Master ${filterMaster}`);
    }
  }

  const plan = [];
  let index = 0;
  for (const row of targets) {
    const fields = row.fields || {};
    const mid = masterIdFromGovernance(fields);
    const companyName = fixture?.companyName || masterNameById.get(mid) || mid;

    if (hasMaterialsSeed(fields) && !FORCE && !fixture) {
      plan.push({
        recordId: row.id,
        masterId: mid,
        companyName,
        action: "skip",
        reason: "already seeded (use --force or --fixture)",
      });
      index += 1;
      continue;
    }

    const seedFields =
      fixture && (!filterMaster || filterMaster === mid)
        ? fixture.fields
        : buildOperatorMaterialsSeedFields({ companyName, index });

    plan.push({
      recordId: row.id,
      masterId: mid,
      companyName,
      action: APPLY ? "patch" : "would-patch",
      fieldKeys: Object.keys(seedFields),
      preview: {
        fileCount: (() => {
          try {
            const p = JSON.parse(seedFields.operator_materials_json);
            return (p.files || []).length;
          } catch (e) {
            return 0;
          }
        })(),
        galleryCount: (() => {
          try {
            const g = JSON.parse(
              seedFields.operator_materials_gallery_json ||
                JSON.parse(seedFields.operator_materials_json).gallery ||
                "[]"
            );
            return Array.isArray(g) ? g.length : 0;
          } catch (e2) {
            return 0;
          }
        })(),
      },
    });

    if (APPLY) {
      const { ok, status, json } = await patchGovernanceRecord(row.id, seedFields);
      if (!ok) {
        console.error("PATCH FAILED", companyName, row.id, status, JSON.stringify(json));
        process.exitCode = 1;
      } else {
        console.log("PATCHED", companyName, mid, "→", Object.keys(seedFields).join(", "));
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    index += 1;
  }

  const stamp = new Date().toISOString().slice(0, 10);
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const outPath = path.join(REPORTS, `operator-materials-explorer-seed-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify({ apply: APPLY, force: FORCE, fixture: fixtureArg, plan }, null, 2));
  console.log("\nPlan written:", outPath);
  console.log(
    "Rows:",
    plan.length,
    "| patch:",
    plan.filter((p) => p.action === "patch" || p.action === "would-patch").length,
    "| skip:",
    plan.filter((p) => p.action === "skip").length
  );

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply to write to Airtable.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
