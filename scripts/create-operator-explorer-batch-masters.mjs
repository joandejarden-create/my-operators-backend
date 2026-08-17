#!/usr/bin/env node
/**
 * Batch-create Operator Setup - Master rows for Operator Explorer factory queue.
 * Field map: company_name (required), submission_status=Draft.
 *
 *   npm run create-operator-explorer-batch-masters -- --dry-run
 *   npm run create-operator-explorer-batch-masters -- --apply --approve-create-operator-batch-masters
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

/**
 * Batch from founder list (2026-07-24). Arbor Lodging excluded — already quality baseline.
 * Domains are official sites for provenance (not Airtable fields).
 */
export const OPERATOR_BATCH_MASTER_PLANS = Object.freeze([
  {
    slug: "tafer-hotels-resorts",
    company_name: "Tafer Hotels & Resorts",
    domain: "taferresorts.com",
    region: "CALA",
    searchNeedle: "Tafer",
  },
  {
    slug: "grupo-presidente",
    company_name: "Grupo Presidente",
    domain: "grupopresidente.com.mx",
    region: "CALA",
    searchNeedle: "Grupo Presidente",
  },
  {
    slug: "highgate",
    company_name: "Highgate",
    domain: "highgate.com",
    region: "CALA",
    searchNeedle: "Highgate",
    notes: "Global platform — Explorer lens must label CALA vs enterprise",
  },
  {
    slug: "grupo-hotelero-santa-fe",
    company_name: "Grupo Hotelero Santa Fe",
    domain: "gsf-hotels.com",
    region: "CALA",
    searchNeedle: "Grupo Hotelero Santa Fe",
  },
  {
    slug: "arriva-hospitality-group",
    company_name: "Arriva Hospitality Group (AHG)",
    domain: "arrivahotels.mx",
    region: "CALA",
    searchNeedle: "Arriva",
  },
  {
    slug: "brittain-resorts-hotels",
    company_name: "Brittain Resorts & Hotels (BRH)",
    domain: "brittainresorts.com",
    region: "US",
    searchNeedle: "Brittain",
    notes: "US Southeast core — confirm CALA relevance before Active release",
  },
  {
    slug: "atlantica-hotels-international",
    company_name: "Atlantica Hotels International (AHI)",
    domain: "atlanticahotels.com.br",
    region: "CALA",
    searchNeedle: "Atlantica",
  },
  // Wave D — founder add 2026-07-24
  {
    slug: "royalton-hotels-resorts",
    company_name: "Royalton Hotels & Resorts",
    domain: "royaltonresorts.com",
    region: "CALA",
    searchNeedle: "Royalton",
    notes: "Formerly Blue Diamond Resorts (2025 identity). All-inclusive CALA.",
  },
  {
    slug: "driftwood-hospitality-management",
    company_name: "Driftwood Hospitality Management",
    domain: "driftwoodhospitality.com",
    region: "US",
    searchNeedle: "Driftwood",
    notes: "U.S. third-party — confirm CALA relevance before treating as CALA-core.",
  },
  {
    slug: "remington-hospitality",
    company_name: "Remington Hospitality",
    domain: "remingtonhospitality.com",
    region: "CALA",
    searchNeedle: "Remington",
    notes: "U.S. platform + CALA division (Miami). Label enterprise vs CALA.",
  },
  // Wave E — founder add 2026-07-24
  {
    slug: "oxohotel",
    company_name: "OxoHotel",
    domain: "oxohotel.com",
    region: "CALA",
    searchNeedle: "OxoHotel",
    notes: "Colombia multi-brand operator (Marriott/Hilton/IHG + proprietary). Founder label: Oxohotels.",
  },
  {
    slug: "grupo-marta-hospitality",
    company_name: "Grupo Marta Hospitality",
    domain: "grupomarta.com",
    region: "CALA",
    searchNeedle: "Grupo Marta",
    notes: "Costa Rica operator — IHG / Best Western / F&B / vacation rentals. Confirm CALA lens.",
  },
  {
    slug: "grupo-iberostar",
    company_name: "Grupo Iberostar",
    domain: "grupoiberostar.com",
    region: "CALA",
    searchNeedle: "Grupo Iberostar",
    notes: "Iberostar Group corporate — hotel division is Iberostar Hotels & Resorts. Label group vs beachfront brand lens; CALA portfolio is core for Explorer.",
  },
]);

function parseArgs(argv) {
  const out = { apply: false, approve: false, slugs: null };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-create-operator-batch-masters") out.approve = true;
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
    throw new Error("Apply requires --approve-create-operator-batch-masters");
  }

  const plans = args.slugs?.length
    ? OPERATOR_BATCH_MASTER_PLANS.filter((p) => args.slugs.includes(p.slug))
    : OPERATOR_BATCH_MASTER_PLANS;

  const base = new Airtable({ apiKey: key }).base(baseId);
  const results = [];

  for (const plan of plans) {
    const existing = await findByNeedle(base, plan.searchNeedle);
    const exact = existing.filter(
      (e) => String(e.company_name || "").trim() === plan.company_name
    );
    const fields = {
      company_name: plan.company_name,
      submission_status: "Draft",
    };
    const row = {
      slug: plan.slug,
      domain: plan.domain,
      region: plan.region,
      notes: plan.notes || null,
      validation: {
        pass: Boolean(plan.company_name),
        checksFailed: plan.company_name ? [] : ["company_name_required"],
      },
      sanitizedPayloadPreview: { table: TABLE, fields },
      exactFieldMapping: [
        { value: fields.company_name, airtableField: "company_name" },
        { value: fields.submission_status, airtableField: "submission_status" },
      ],
      existingMatches: existing,
      created: null,
      skipped: false,
      blocked: false,
    };

    if (exact.length || existing.length) {
      row.skipped = true;
      row.blocked = existing.length > 0 && exact.length === 0;
      row.created = exact[0] || existing[0];
      row.reason = exact.length
        ? "Exact company_name already exists — reuse Master"
        : "Needle match exists — review before create (not auto-creating to avoid duplicates)";
      results.push(row);
      continue;
    }

    if (args.apply && row.validation.pass) {
      const created = await base(TABLE).create(fields, { typecast: true });
      row.created = {
        id: created.id,
        company_name: created.get("company_name"),
        submission_status: created.get("submission_status"),
      };
      row.airtableWritePerformed = true;
    }
    results.push(row);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: !args.apply,
    applyPerformed: args.apply,
    airtableWrites: results.filter((r) => r.airtableWritePerformed).length,
    writeKind: args.apply ? "operator_setup_master_create" : "none",
    errorHandling: {
      validationError: "Skip create; fix company_name",
      apiError: "Surface Airtable message; re-search before retry",
      networkError: "Retry once; never blind-create duplicates",
      userFacing: "Could not create one or more Operator Masters.",
    },
    results,
    summary: {
      planned: results.length,
      created: results.filter((r) => r.airtableWritePerformed).length,
      reusedExisting: results.filter((r) => r.skipped && r.created).length,
      wouldCreate: results.filter((r) => !r.skipped && !args.apply).length,
    },
  };

  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "create-operator-explorer-batch-masters.json");
  const mdPath = path.join(reportsDir, "create-operator-explorer-batch-masters.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Create Operator Explorer batch Masters",
    "",
    `dryRun: **${report.dryRun}** · created: **${report.summary.created}** · reused: **${report.summary.reusedExisting}**`,
    "",
    "| Slug | company_name | recordId | status |",
    "| --- | --- | --- | --- |",
  ];
  for (const r of results) {
    const id = r.created?.id || "(pending)";
    const status = r.airtableWritePerformed
      ? "created"
      : r.skipped
        ? "existing"
        : report.dryRun
          ? "would_create"
          : "pending";
    lines.push(`| \`${r.slug}\` | ${r.sanitizedPayloadPreview.fields.company_name} | \`${id}\` | ${status} |`);
  }
  lines.push("");
  fs.writeFileSync(mdPath, lines.join("\n"));
  console.log(JSON.stringify({ jsonPath, mdPath, summary: report.summary, results: results.map((r) => ({
    slug: r.slug,
    id: r.created?.id || null,
    skipped: r.skipped,
    created: Boolean(r.airtableWritePerformed),
    reason: r.reason || null,
  })) }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
