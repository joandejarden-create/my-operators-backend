/**
 * Colombia RNT open-data dry-run (no Airtable writes).
 *
 * Usage:
 *   node scripts/census-colombia-rnt-dry-run.mjs
 *   node scripts/census-colombia-rnt-dry-run.mjs --year 2026 --max-rows 500 --hotels-only
 *   npm run census:colombia-rnt-dry-run -- --year 2026 --max-rows 200
 */
import "dotenv/config";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  COLOMBIA_RNT_ADAPTER_VERSION,
  MAP_COLOMBIA_RNT,
  runColombiaRntDryRun,
} from "../lib/research-engine-v2/colombia-rnt-open-data-adapter.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function parseArgs(argv = process.argv.slice(2)) {
  const out = {
    year: null,
    maxRows: 1500,
    pageSize: 1000,
    hotelsOnly: false,
    includeApartahotel: true,
    includeHostelLike: false,
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--year") out.year = argv[++i];
    else if (a === "--max-rows") out.maxRows = Number(argv[++i]) || out.maxRows;
    else if (a === "--page-size") out.pageSize = Number(argv[++i]) || out.pageSize;
    else if (a === "--hotels-only") out.hotelsOnly = true;
    else if (a === "--no-apartahotel") out.includeApartahotel = false;
    else if (a === "--include-hostel-like") out.includeHostelLike = true;
  }
  return out;
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

function renderMarkdown(result, args) {
  const s = result.summary || {};
  const sample = (result.candidates || []).slice(0, 15);
  const lines = [
    `# Colombia RNT Open-Data Dry-Run`,
    ``,
    `**Status:** \`colombia_rnt_open_data_dry_run_complete\``,
    `**Adapter:** \`${COLOMBIA_RNT_ADAPTER_VERSION}\``,
    `**Generated:** ${new Date().toISOString()}`,
    `**Airtable writes:** none (dry-run only)`,
    ``,
    `## Source`,
    ``,
    `- Dataset: [${MAP_COLOMBIA_RNT.sourceDatasetId}](${MAP_COLOMBIA_RNT.sourceDatasetUrl})`,
    `- Year filter: ${args.year ?? "(none — all years then dedupe by codigo_rnt)"}`,
    `- Subcategories: ${(result.source?.subcategories || []).join(", ") || "n/a"}`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Raw rows fetched | ${s.raw_rows_fetched ?? 0} |`,
    `| Unique codigo_rnt | ${s.unique_codigo_rnt ?? 0} |`,
    `| Validation pass | ${s.validation_pass ?? 0} |`,
    `| Validation fail | ${s.validation_fail ?? 0} |`,
    `| Rooms sanity Hold | ${s.rooms_sanity_hold ?? 0} |`,
    `| NIT present (ownership signal only) | ${s.ownership_nit_present ?? 0} |`,
    ``,
    `## Ownership lane`,
    ``,
    `NIT is captured on \`ownership_signal\` only. **Owner Name is not written** (Autopilot forbidden / blocked enrichment lane).`,
    ``,
    `## Sample candidates`,
    ``,
    `| Identity Key | Property Name | City | State | Rooms | NIT signal |`,
    `| --- | --- | --- | --- | ---: | --- |`,
    ...sample.map((c) => {
      const f = c.fields || {};
      return `| ${c.identity_key || ""} | ${f["Property Name"] || ""} | ${f.City || ""} | ${f["State / Region"] || ""} | ${f["Rooms / Keys"] ?? ""} | ${c.ownership_signal?.tax_id || ""} |`;
    }),
    ``,
    `## Field mapping`,
    ``,
    `- Object: \`MAP_COLOMBIA_RNT\``,
    `- Identity: \`gov_co_rnt_{codigo_rnt}\``,
    `- Inventory fields: Property Name, City, State / Region, Country, Rooms / Keys*, Source URL, Family / Source Family`,
    `- Forbidden: Owner Name / Operator / dates (never in patch)`,
    ``,
    `## Next steps`,
    ``,
    `1. Review dry-run sample for name/city quality.`,
    `2. Match \`gov_co_rnt_*\` identity keys against Hotel Property Census (read-only).`,
    `3. Only then design a controlled insert gate (separate approval) — still no Owner Name writes.`,
    ``,
  ];
  return lines.join("\n");
}

const args = parseArgs();
if (args.help) {
  console.log(`Colombia RNT dry-run (no Airtable writes)

Options:
  --year YYYY            Filter Socrata ano=
  --max-rows N           Cap fetched rows (default 1500)
  --page-size N          Socrata page size (default 1000)
  --hotels-only          Subcategory HOTEL only
  --no-apartahotel       Exclude APARTAHOTEL
  --include-hostel-like  Include hostels/glamping/etc.
`);
  process.exit(0);
}

console.log(`[colombia-rnt] dry-run start version=${COLOMBIA_RNT_ADAPTER_VERSION}`);
const result = await runColombiaRntDryRun({
  year: args.year,
  maxRows: args.maxRows,
  pageSize: args.pageSize,
  hotelsOnly: args.hotelsOnly,
  includeApartahotel: args.includeApartahotel,
  includeHostelLike: args.includeHostelLike,
});

if (!result.ok) {
  console.error(`[colombia-rnt] FAILED ${result.error_kind}: ${result.message}`);
  process.exit(1);
}

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });
const ts = stamp();
const jsonPath = join(REPORTS, `colombia-rnt-dry-run-${ts}.json`);
const mdPath = join(DOCS, "production-census-colombia-rnt-open-data-adapter.md");
const mdReportPath = join(REPORTS, `colombia-rnt-dry-run-${ts}.md`);

const slim = {
  ...result,
  candidates: (result.candidates || []).slice(0, 200),
};
writeFileSync(jsonPath, JSON.stringify(slim, null, 2));
const md = renderMarkdown(result, args);
writeFileSync(mdReportPath, md);
writeFileSync(mdPath, md);

console.log(`[colombia-rnt] ${JSON.stringify(result.summary)}`);
console.log(`[colombia-rnt] wrote ${jsonPath}`);
console.log(`[colombia-rnt] wrote ${mdReportPath}`);
console.log(`[colombia-rnt] wrote ${mdPath}`);
console.log(`[colombia-rnt] Airtable writes: none`);
