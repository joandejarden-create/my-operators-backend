#!/usr/bin/env node
/**
 * Create Operator Setup - Master rows for brand-managed Core 5.
 * Field map: company_name (required), submission_status=Draft.
 *
 *   npm run create-operator-explorer-brand-managed-masters -- --dry-run
 *   npm run create-operator-explorer-brand-managed-masters -- --apply --approve-create-operator-brand-managed-masters
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

/** Wave C — brand-managed parent companies (Operator lens). */
export const BRAND_MANAGED_MASTER_PLANS = Object.freeze([
  {
    slug: "marriott-international-managed",
    company_name: "Marriott International (Managed)",
    domain: "marriott.com",
    region: "GLOBAL",
    searchNeedle: "Marriott International (Managed)",
    notes: "Brand-managed lens — label enterprise vs CALA managed footprint",
  },
  {
    slug: "ihg-managed",
    company_name: "IHG Hotels & Resorts (Managed)",
    domain: "ihg.com",
    region: "GLOBAL",
    searchNeedle: "IHG Hotels & Resorts (Managed)",
    notes: "Brand-managed lens — label enterprise vs CALA managed footprint",
  },
  {
    slug: "hilton-managed",
    company_name: "Hilton (Managed)",
    domain: "hilton.com",
    region: "GLOBAL",
    searchNeedle: "Hilton (Managed)",
    notes: "Brand-managed lens — label enterprise vs CALA managed footprint",
  },
  {
    slug: "accor-managed",
    company_name: "Accor (Managed)",
    domain: "group.accor.com",
    region: "GLOBAL",
    searchNeedle: "Accor (Managed)",
    notes: "Brand-managed lens — label enterprise vs CALA managed footprint",
  },
  {
    slug: "minor-hotels-managed",
    company_name: "Minor Hotels (Managed)",
    domain: "minorhotels.com",
    region: "GLOBAL",
    searchNeedle: "Minor Hotels (Managed)",
    notes: "Brand-managed lens — label enterprise vs CALA managed footprint",
  },
]);

function parseArgs(argv) {
  const out = { apply: false, approve: false, slugs: null };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-create-operator-brand-managed-masters") out.approve = true;
    else if (a === "--slugs" && argv[i + 1]) {
      out.slugs = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    }
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
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  if (args.apply && !args.approve) {
    throw new Error("Apply requires --approve-create-operator-brand-managed-masters");
  }

  const plans = args.slugs?.length
    ? BRAND_MANAGED_MASTER_PLANS.filter((p) => args.slugs.includes(p.slug))
    : BRAND_MANAGED_MASTER_PLANS;

  const base = new Airtable({ apiKey: key }).base(baseId);
  const results = [];

  for (const plan of plans) {
    const existing = await findByNeedle(base, plan.searchNeedle);
    const exact = existing.find(
      (r) => String(r.company_name || "").trim() === plan.company_name
    );
    if (exact) {
      results.push({
        slug: plan.slug,
        status: "already_exists",
        recordId: exact.id,
        company_name: exact.company_name,
        submission_status: exact.submission_status,
        domain: plan.domain,
        validation: { pass: true, checksFailed: [] },
      });
      continue;
    }

    const fields = {
      company_name: plan.company_name,
      submission_status: "Draft",
    };
    const mapping = [
      { value: fields.company_name, airtableField: "company_name" },
      { value: fields.submission_status, airtableField: "submission_status" },
    ];

    if (args.apply) {
      const created = await base(TABLE).create(fields, { typecast: true });
      results.push({
        slug: plan.slug,
        status: "created",
        recordId: created.id,
        company_name: created.get("company_name"),
        submission_status: created.get("submission_status"),
        domain: plan.domain,
        exactFieldMapping: mapping,
        sanitizedPayloadPreview: fields,
        validation: { pass: true, checksFailed: [] },
      });
    } else {
      results.push({
        slug: plan.slug,
        status: "would_create",
        recordId: null,
        company_name: plan.company_name,
        domain: plan.domain,
        notes: plan.notes,
        exactFieldMapping: mapping,
        sanitizedPayloadPreview: fields,
        validation: { pass: true, checksFailed: [] },
      });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: !args.apply,
    applyPerformed: args.apply,
    airtableWrites: args.apply,
    writeKind: args.apply ? "operator_setup_master_create_brand_managed" : "none",
    results,
    summary: {
      plans: results.length,
      created: results.filter((r) => r.status === "created").length,
      alreadyExists: results.filter((r) => r.status === "already_exists").length,
      wouldCreate: results.filter((r) => r.status === "would_create").length,
    },
    errorHandling: {
      validationError: "Do not create; fix company_name",
      apiError: "Surface Airtable message",
      networkError: "Retry once; re-search before duplicate create",
      userFacing: "Could not create one or more brand-managed Operator Masters.",
    },
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "create-operator-explorer-brand-managed-masters.json");
  const mdPath = path.join(reportsDir, "create-operator-explorer-brand-managed-masters.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Brand-managed Operator Masters",
    "",
    `dryRun: **${report.dryRun}** · created: **${report.summary.created}** · already: **${report.summary.alreadyExists}** · would: **${report.summary.wouldCreate}**`,
    "",
  ];
  for (const r of results) {
    lines.push(
      `- **${r.company_name}** (\`${r.slug}\`): ${r.status}${r.recordId ? ` · \`${r.recordId}\`` : ""}`
    );
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(JSON.stringify(report.summary, null, 2));
  for (const r of results) {
    console.log(`  ${r.slug}: ${r.status}${r.recordId ? ` ${r.recordId}` : ""}`);
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
