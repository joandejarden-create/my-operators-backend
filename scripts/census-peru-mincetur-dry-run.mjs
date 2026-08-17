/**
 * Peru MINCETUR hospedajes open-data dry-run (no Airtable writes).
 *
 * Usage:
 *   npm run census:peru-mincetur-dry-run
 *   npm run census:peru-mincetur-dry-run -- --hotels-only --max-rows 300
 */
import "dotenv/config";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  PERU_MINCETUR_ADAPTER_VERSION,
  MAP_PERU_MINCETUR,
  runPeruMinceturDryRun,
} from "../lib/research-engine-v2/peru-mincetur-open-data-adapter.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function parseArgs(argv = process.argv.slice(2)) {
  const out = {
    maxRows: 0,
    hotelsOnly: false,
    includeApartHotel: true,
    includeHostelLike: false,
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--max-rows") out.maxRows = Number(argv[++i]) || 0;
    else if (a === "--hotels-only") out.hotelsOnly = true;
    else if (a === "--no-apart-hotel") out.includeApartHotel = false;
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
  return [
    `# Peru MINCETUR Open-Data Dry-Run`,
    ``,
    `**Status:** \`peru_mincetur_open_data_dry_run_complete\``,
    `**Adapter:** \`${PERU_MINCETUR_ADAPTER_VERSION}\``,
    `**Generated:** ${new Date().toISOString()}`,
    `**Airtable writes:** none`,
    ``,
    `## Source`,
    ``,
    `- CSV: ${MAP_PERU_MINCETUR.sourceCsvUrl}`,
    `- Catalog: ${MAP_PERU_MINCETUR.sourceDatasetUrl}`,
    `- Classes: ${(result.source?.classes || []).join(", ")}`,
    `- FECHA_CORTE sample: ${result.source?.fecha_corte_sample || "n/a"}`,
    `- max-rows: ${args.maxRows || "all filtered"}`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Raw CSV rows | ${s.raw_rows_fetched ?? 0} |`,
    `| After lodging class filter | ${s.lodging_filtered ?? 0} |`,
    `| Validation pass | ${s.validation_pass ?? 0} |`,
    `| Validation fail | ${s.validation_fail ?? 0} |`,
    `| Official Property URL present | ${s.official_property_url_present ?? 0} |`,
    `| RUC ownership signal | ${s.ownership_ruc_present ?? 0} |`,
    ``,
    `## Ownership lane`,
    ``,
    `RUC is on \`ownership_signal\` only. **Owner Name is not written.**`,
    ``,
    `## Sample`,
    ``,
    `| Identity | Name | City | Rooms | URL? | RUC |`,
    `| --- | --- | --- | ---: | --- | --- |`,
    ...sample.map((c) => {
      const f = c.fields || {};
      return `| ${c.identity_key || ""} | ${f["Property Name"] || ""} | ${f.City || ""} | ${f["Rooms / Keys"] ?? ""} | ${f["Official Property URL"] ? "yes" : ""} | ${c.ownership_signal?.tax_id || ""} |`;
    }),
    ``,
    `## Notes vs Colombia RNT`,
    ``,
    `- Uses **NOMBRE_COMERCIAL** (better matchability than legal-only names)`,
    `- Often includes **PAGINA_WEB** → Official Property URL candidate`,
    `- Still steward-gated for apply; no auto-insert from this dry-run`,
    ``,
    `## Next`,
    ``,
    `1. \`npm run census:peru-mincetur-hpc-match-plan\` (when wired) or reuse Colombia match pattern`,
    `2. Wait for Wave 2 Webhound before prioritizing PR/Central America over Peru`,
    `3. Colombia apply remains paused`,
    ``,
  ].join("\n");
}

const args = parseArgs();
if (args.help) {
  console.log(`Peru MINCETUR dry-run (no Airtable writes)

Options:
  --max-rows N
  --hotels-only
  --no-apart-hotel
  --include-hostel-like
`);
  process.exit(0);
}

console.log(`[peru-mincetur] dry-run start version=${PERU_MINCETUR_ADAPTER_VERSION}`);
const result = await runPeruMinceturDryRun({
  maxRows: args.maxRows || undefined,
  hotelsOnly: args.hotelsOnly,
  includeApartHotel: args.includeApartHotel,
  includeHostelLike: args.includeHostelLike,
});

if (!result.ok) {
  console.error(`[peru-mincetur] FAILED ${result.error_kind}: ${result.message}`);
  process.exit(1);
}

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });
const ts = stamp();
const jsonPath = join(REPORTS, `peru-mincetur-dry-run-${ts}.json`);
const mdPath = join(DOCS, "production-census-peru-mincetur-open-data-adapter.md");
const mdReport = join(REPORTS, `peru-mincetur-dry-run-${ts}.md`);
const slim = { ...result, candidates: (result.candidates || []).slice(0, 250) };
const md = renderMarkdown(result, args);
writeFileSync(jsonPath, JSON.stringify(slim, null, 2));
writeFileSync(mdReport, md);
writeFileSync(mdPath, md);

console.log(`[peru-mincetur] ${JSON.stringify(result.summary)}`);
console.log(`[peru-mincetur] wrote ${jsonPath}`);
console.log(`[peru-mincetur] wrote ${mdPath}`);
console.log(`[peru-mincetur] Airtable writes: none`);
