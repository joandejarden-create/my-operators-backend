#!/usr/bin/env node
/**
 * Create Playa Hotels & Resorts Operator Setup - Master (gated).
 * Field map: company_name (required), submission_status=Draft.
 *
 *   node scripts/create-playa-hotels-resorts-operator-master.mjs --dry-run
 *   node scripts/create-playa-hotels-resorts-operator-master.mjs --apply --approve-create-playa-operator-master
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

export const PLAYA_MASTER_PLAN = Object.freeze({
  slug: "playa-hotels-resorts",
  company_name: "Playa Hotels & Resorts",
  domain: "playaresorts.com",
  region: "CALA",
  submission_status: "Draft",
  searchNeedle: "Playa Hotels",
});

function parseArgs(argv) {
  const out = { apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-create-playa-operator-master") out.approve = true;
  }
  return out;
}

async function findByNeedle(base, needle) {
  const formula = `FIND("${needle.replace(/"/g, '\\"')}", {company_name}&"")`;
  const rows = await base(TABLE)
    .select({
      filterByFormula: formula,
      maxRecords: 10,
      fields: ["company_name", "submission_status"],
    })
    .firstPage();
  return rows.map((r) => ({
    id: r.id,
    company_name: r.get("company_name"),
    submission_status: r.get("submission_status"),
  }));
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-create-playa-operator-master");
    process.exit(1);
  }
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }
  const base = new Airtable({ apiKey }).base(baseId);
  const matches = await findByNeedle(base, PLAYA_MASTER_PLAN.searchNeedle);
  const exact = matches.find(
    (m) => String(m.company_name || "").trim() === PLAYA_MASTER_PLAN.company_name
  );

  const report = {
    version: "create-playa-hotels-resorts-operator-master-v1",
    dryRun: !args.apply,
    plan: PLAYA_MASTER_PLAN,
    existingMatches: matches,
    result: null,
  };

  if (exact) {
    report.result = { action: "skip_existing", recordId: exact.id, ...exact };
  } else if (!args.apply) {
    report.result = {
      action: "would_create",
      fields: {
        company_name: PLAYA_MASTER_PLAN.company_name,
        submission_status: PLAYA_MASTER_PLAN.submission_status,
      },
      fieldMapping: [
        { value: PLAYA_MASTER_PLAN.company_name, airtableField: "company_name" },
        { value: PLAYA_MASTER_PLAN.submission_status, airtableField: "submission_status" },
      ],
    };
  } else {
    const created = await base(TABLE).create({
      company_name: PLAYA_MASTER_PLAN.company_name,
      submission_status: PLAYA_MASTER_PLAN.submission_status,
    });
    report.result = {
      action: "created",
      recordId: created.id,
      company_name: created.get("company_name"),
      submission_status: created.get("submission_status"),
    };
  }

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "create-playa-hotels-resorts-operator-master.json");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report.result, null, 2));
  console.log("Wrote", jsonPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
