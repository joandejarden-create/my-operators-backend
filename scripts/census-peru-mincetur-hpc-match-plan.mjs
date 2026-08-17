/**
 * Peru MINCETUR ↔ Hotel Property Census match + gated insert plan (READ-ONLY).
 *
 * Usage:
 *   npm run census:peru-mincetur-hpc-match-plan -- --hotels-only --max-rows 500
 *   npm run census:peru-mincetur-hpc-match-plan -- --input reports/research-engine-v2/peru-mincetur-dry-run-….json
 *
 * No Airtable writes. No Owner Name writes. No auto_insert.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  PERU_MINCETUR_ADAPTER_VERSION,
  MAP_PERU_MINCETUR,
  runPeruMinceturDryRun,
} from "../lib/research-engine-v2/peru-mincetur-open-data-adapter.js";
import {
  PERU_MINCETUR_HPC_PLAN_VERSION,
  buildPeruMinceturHpcPlan,
  toPeruMinceturHpcMatchInput,
} from "../lib/research-engine-v2/peru-mincetur-hpc-match-plan.js";
import {
  HPC_MATCH_VERSION,
  loadHotelPropertyCensusReadOnly,
  matchAllCandidatesToHotelPropertyCensus,
} from "../lib/independent-census/match-hotel-property-census.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function parseArgs(argv = process.argv.slice(2)) {
  if (argv.includes("--apply")) {
    throw new Error("--apply not supported. Match + plan are read-only / dry-run.");
  }
  const out = {
    input: "",
    maxRows: 500,
    hotelsOnly: true,
    includeApartHotel: false,
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--input") out.input = argv[++i];
    else if (a === "--max-rows") out.maxRows = Number(argv[++i]) || out.maxRows;
    else if (a === "--hotels-only") out.hotelsOnly = true;
    else if (a === "--include-apart-hotel") {
      out.includeApartHotel = true;
      out.hotelsOnly = false;
    }
  }
  return out;
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

function renderMarkdown(report) {
  const s = report.plan?.summary || {};
  const d = s.decisions || {};
  const sample = (report.plan?.insert_candidate_sample || []).slice(0, 12);
  return [
    `# Peru MINCETUR ↔ Hotel Property Census Match + Gated Plan`,
    ``,
    `**Status:** \`peru_mincetur_hpc_match_plan_dry_run_complete\``,
    `**Generated:** ${report.generated_at}`,
    `**Adapter:** \`${PERU_MINCETUR_ADAPTER_VERSION}\` / \`${PERU_MINCETUR_HPC_PLAN_VERSION}\` / \`${HPC_MATCH_VERSION}\``,
    `**Airtable writes:** none`,
    `**Owner Name writes:** none (RUC on ownership_signal only)`,
    `**Auto-insert:** disabled (steward gate required; PAGINA_WEB → Official Property URL when present)`,
    ``,
    `## Dedupe SoT`,
    ``,
    `- Hotel Property Census \`${report.hpc?.tableId}\` only`,
    `- Legacy Hotel Census: forbidden`,
    `- Peru pool size: ${report.hpc?.matching_pool ?? 0} (of ${report.hpc?.total_loaded ?? 0} loaded)`,
    ``,
    `## HPC match summary`,
    ``,
    `| Action | Count |`,
    `| --- | ---: |`,
    `| likely_existing | ${report.hpc_match_summary?.likely_existing ?? 0} |`,
    `| possible_duplicate_review | ${report.hpc_match_summary?.possible_duplicate_review ?? 0} |`,
    `| likely_new_candidate | ${report.hpc_match_summary?.likely_new_candidate ?? 0} |`,
    `| needs_research | ${report.hpc_match_summary?.needs_research ?? 0} |`,
    `| identity_key_collisions | ${report.hpc_match_summary?.identity_key_collisions ?? 0} |`,
    ``,
    `## Gated plan decisions`,
    ``,
    `| Decision | Count |`,
    `| --- | ---: |`,
    `| auto_enrich_only | ${d.auto_enrich_only ?? 0} |`,
    `| steward_hold | ${d.steward_hold ?? 0} |`,
    `| steward_hold_insert_candidate | ${d.steward_hold_insert_candidate ?? 0} |`,
    `| reject | ${d.reject ?? 0} |`,
    `| insert candidates with Official Property URL | ${s.insert_candidates_with_official_url ?? 0} |`,
    ``,
    `## Future apply confirms (not enabled)`,
    ``,
    ...(report.plan?.required_future_apply_confirms || []).map((c) => `- \`${c}\``),
    ``,
    `## Insert-candidate sample (steward review)`,
    ``,
    `| Identity | Name | City | Rooms | RUC signal | Official URL | HPC action |`,
    `| --- | --- | --- | ---: | --- | --- | --- |`,
    ...sample.map(
      (r) =>
        `| ${r.identity_key || ""} | ${r.property_name || ""} | ${r.city || ""} | ${r.rooms ?? ""} | ${r.ruc_signal || ""} | ${r.official_property_url ? "yes" : ""} | ${r.hpc_recommended_action || ""} |`
    ),
    ``,
    `## Field mapping`,
    ``,
    `- \`MAP_PERU_MINCETUR\` → inventory fields only`,
    `- Identity: \`gov_pe_mincetur_{NRO_CERTIFICADO}\` (fallback RUC+slug)`,
    `- Forbidden on insert preview: Owner Name, Operator, Opening Date, Brand Status, etc.`,
    ``,
  ].join("\n");
}

const args = parseArgs();
if (args.help) {
  console.log(`Peru MINCETUR HPC match + gated plan (read-only)

Options:
  --input PATH              Reuse prior peru-mincetur-dry-run JSON
  --max-rows N              Cap MINCETUR candidates (default 500)
  --hotels-only             HOTEL class only (default)
  --include-apart-hotel     Include APART HOTEL / RESORT default classes
`);
  process.exit(0);
}

console.log("=== Peru MINCETUR ↔ Hotel Property Census (read-only) ===\n");

let peCandidates = [];
let peMeta = {};
if (args.input) {
  const path = args.input.startsWith("/") || /^[A-Za-z]:/.test(args.input)
    ? args.input
    : join(process.cwd(), args.input);
  if (!existsSync(path)) throw new Error(`Not found: ${path}`);
  const dry = JSON.parse(readFileSync(path, "utf8"));
  peCandidates = dry.candidates || [];
  peMeta = { input: path, summary: dry.summary || null };
  console.log(`Loaded ${peCandidates.length} candidates from ${path}`);
} else {
  console.log(
    `Fetching MINCETUR dry-run maxRows=${args.maxRows} hotelsOnly=${args.hotelsOnly}…`
  );
  const dry = await runPeruMinceturDryRun({
    maxRows: args.maxRows,
    hotelsOnly: args.hotelsOnly,
    includeApartHotel: args.includeApartHotel,
  });
  if (!dry.ok) {
    console.error(`MINCETUR fetch failed: ${dry.error_kind} ${dry.message}`);
    process.exit(1);
  }
  peCandidates = dry.candidates || [];
  peMeta = { source: dry.source, summary: dry.summary };
  console.log(`MINCETUR candidates: ${peCandidates.length}`);
}

console.log("Loading Hotel Property Census (Peru filter)…");
const hpc = await loadHotelPropertyCensusReadOnly({ countryFilter: "Peru" });
console.log(
  `  ${hpc.table} (${hpc.tableId}) — total ${hpc.totalLoaded}, Peru pool ${hpc.rows.length}`
);

const matchInputs = peCandidates.map(toPeruMinceturHpcMatchInput);
const { rows: hpcMatches, summary: hpcSummary } = matchAllCandidatesToHotelPropertyCensus(
  matchInputs,
  hpc,
  { identityKeyFn: (c) => c.proposedIdentityKey || "" }
);

const plan = buildPeruMinceturHpcPlan(peCandidates, hpcMatches);

const report = {
  generated_at: new Date().toISOString(),
  version: PERU_MINCETUR_HPC_PLAN_VERSION,
  adapter_version: PERU_MINCETUR_ADAPTER_VERSION,
  hpc_match_version: HPC_MATCH_VERSION,
  dry_run: true,
  airtable_writes: false,
  ownership_writes: false,
  mincetur: peMeta,
  hpc: {
    table: hpc.table,
    tableId: hpc.tableId,
    baseName: hpc.baseName,
    total_loaded: hpc.totalLoaded,
    matching_pool: hpc.rows.length,
    legacy_hotel_census_used: false,
  },
  hpc_match_summary: hpcSummary,
  plan: {
    ...plan,
    rows: plan.rows.slice(0, 500),
  },
  field_mapping: MAP_PERU_MINCETUR,
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });
const ts = stamp();
const jsonPath = join(REPORTS, `peru-mincetur-hpc-match-plan-${ts}.json`);
const mdPath = join(REPORTS, `peru-mincetur-hpc-match-plan-${ts}.md`);
const docPath = join(DOCS, "production-census-peru-mincetur-hpc-match-plan.md");
const md = renderMarkdown(report);
writeFileSync(jsonPath, JSON.stringify(report, null, 2));
writeFileSync(mdPath, md);
writeFileSync(docPath, md);

console.log("\n--- HPC match ---");
console.log(`  likely_existing:           ${hpcSummary.likely_existing}`);
console.log(`  possible_duplicate_review: ${hpcSummary.possible_duplicate_review}`);
console.log(`  likely_new_candidate:      ${hpcSummary.likely_new_candidate}`);
console.log(`  needs_research:            ${hpcSummary.needs_research}`);
console.log("\n--- Gated plan ---");
console.log(`  auto_enrich_only:              ${plan.summary.decisions.auto_enrich_only}`);
console.log(`  steward_hold:                  ${plan.summary.decisions.steward_hold}`);
console.log(
  `  steward_hold_insert_candidate: ${plan.summary.decisions.steward_hold_insert_candidate}`
);
console.log(
  `  insert w/ Official Property URL: ${plan.summary.insert_candidates_with_official_url}`
);
console.log(`  reject:                        ${plan.summary.decisions.reject}`);
console.log(`\n  wrote: ${jsonPath}`);
console.log(`  wrote: ${docPath}`);
console.log("✓ Read-only. No Owner Name writes. Auto-insert disabled.");
