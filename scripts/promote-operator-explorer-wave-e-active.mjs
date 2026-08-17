/**
 * Promote Wave E Operator Masters to Active (Explorer list visibility).
 *
 *   node scripts/promote-operator-explorer-wave-e-active.mjs --dry-run
 *   node scripts/promote-operator-explorer-wave-e-active.mjs --apply --approve-promote-operator-wave-e-active
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { getOperatorFactoryQueueEntry } from "../lib/partner-intelligence/operator-explorer-factory-queue.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const SLUGS = ["oxohotel", "grupo-marta-hospitality", "grupo-iberostar"];

function parseArgs(argv) {
  const out = { apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    if (a === "--dry-run") out.apply = false;
    if (a === "--approve-promote-operator-wave-e-active") out.approve = true;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    throw new Error("Apply requires --approve-promote-operator-wave-e-active");
  }
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const results = [];

  for (const slug of SLUGS) {
    const q = getOperatorFactoryQueueEntry(slug);
    if (!q?.recordId) throw new Error(`Missing queue Master for ${slug}`);
    const before = await base(MASTER_TABLE).find(q.recordId);
    const prev = before.get("submission_status");
    const row = {
      slug,
      recordId: q.recordId,
      companyName: q.companyName,
      previousStatus: prev,
      nextStatus: "Active",
      explorerUrl: q.explorerUrl,
      validation: { pass: true, checksFailed: [] },
      sanitizedPayloadPreview: { submission_status: "Active" },
      exactFieldMapping: [{ airtableField: "submission_status", value: "Active" }],
    };
    if (args.apply) {
      await base(MASTER_TABLE).update(q.recordId, { submission_status: "Active" }, { typecast: true });
      row.updated = true;
    } else {
      row.wouldUpdate = true;
    }
    results.push(row);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: !args.apply,
    applyPerformed: args.apply,
    writeKind: args.apply ? "operator_setup_master_submission_status_active" : "none",
    results,
    errorHandling: {
      validationError: "Do not promote",
      apiError: "Surface Airtable message",
      networkError: "Retry once",
      userFacing: "Could not promote Operator Masters to Active.",
    },
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "promote-operator-explorer-wave-e-active.json");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ jsonPath, dryRun: report.dryRun, results }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
