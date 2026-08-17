/**
 * Create Aimbridge LATAM Operator Setup - Master (gated).
 * Field map: company_name (required), submission_status=Draft.
 *
 *   node scripts/create-aimbridge-latam-operator-master.mjs --dry-run
 *   node scripts/create-aimbridge-latam-operator-master.mjs --apply --approve-create-aimbridge-master
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

export const AIMBRIDGE_LATAM_MASTER_PLAN = Object.freeze({
  company_name: "Aimbridge Hospitality (LATAM)",
  submission_status: "Draft",
  slug: "aimbridge-latam",
  domain: "aimbridgelatam.com",
  parentDomain: "aimbridgehospitality.com",
});

function parseArgs(argv) {
  const out = { apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    if (a === "--dry-run") out.apply = false;
    if (a === "--approve-create-aimbridge-master") out.approve = true;
  }
  return out;
}

async function findExisting(base) {
  const formula = `FIND("Aimbridge", {company_name}&"")`;
  const rows = await base(TABLE)
    .select({ filterByFormula: formula, maxRecords: 10, fields: ["company_name", "submission_status"] })
    .firstPage();
  return rows.map((r) => ({
    id: r.id,
    company_name: r.get("company_name"),
    submission_status: r.get("submission_status"),
  }));
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");

  if (args.apply && !args.approve) {
    throw new Error("Apply requires --approve-create-aimbridge-master");
  }

  const base = new Airtable({ apiKey: key }).base(baseId);
  const existing = await findExisting(base);

  const sanitizedPayloadPreview = {
    table: TABLE,
    fields: {
      company_name: AIMBRIDGE_LATAM_MASTER_PLAN.company_name,
      submission_status: AIMBRIDGE_LATAM_MASTER_PLAN.submission_status,
    },
  };

  const validation = {
    pass: Boolean(AIMBRIDGE_LATAM_MASTER_PLAN.company_name),
    checksFailed: AIMBRIDGE_LATAM_MASTER_PLAN.company_name ? [] : ["company_name_required"],
  };

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: !args.apply,
    validation,
    sanitizedPayloadPreview,
    exactFieldMapping: [
      { value: AIMBRIDGE_LATAM_MASTER_PLAN.company_name, airtableField: "company_name" },
      { value: AIMBRIDGE_LATAM_MASTER_PLAN.submission_status, airtableField: "submission_status" },
    ],
    existingMatches: existing,
    errorHandling: {
      validationError: "Do not create; fix company_name",
      apiError: "Surface Airtable message; no partial Master",
      networkError: "Retry once; do not duplicate without re-search",
      userFacing: "Could not create Aimbridge Operator Master. Check Airtable connection and try again.",
    },
    created: null,
  };

  if (existing.length) {
    report.blocked = true;
    report.reason = "Aimbridge Master already exists — will not create duplicate";
    report.created = existing[0];
  } else if (args.apply && validation.pass) {
    const created = await base(TABLE).create(sanitizedPayloadPreview.fields, { typecast: true });
    report.created = {
      id: created.id,
      company_name: created.get("company_name"),
      submission_status: created.get("submission_status"),
    };
    report.airtableWritePerformed = true;
  }

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "create-aimbridge-latam-operator-master.json");
  const mdPath = path.join(reportsDir, "create-aimbridge-latam-operator-master.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(
    mdPath,
    [
      "# Create Aimbridge LATAM Operator Master",
      "",
      `dryRun: **${report.dryRun}**`,
      `validation: **${validation.pass}**`,
      existing.length
        ? `Existing: \`${existing[0].id}\` — ${existing[0].company_name}`
        : report.created
          ? `Created: \`${report.created.id}\``
          : "Would create new Master row",
      "",
      "```json",
      JSON.stringify(sanitizedPayloadPreview, null, 2),
      "```",
      "",
    ].join("\n")
  );

  console.log(JSON.stringify({ jsonPath, mdPath, ...report }, null, 2));
  if (!validation.pass) process.exit(2);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
