/**
 * Dominican Republic / independent census — brand-exclusion audit (READ-ONLY).
 *
 * Usage:
 *   node scripts/audit-independent-census-brand-exclusion.mjs \
 *     --input reports/independent-census-osm-dry-run-….json \
 *     [--match-report reports/independent-census-osm-current-match-….json]
 *
 * No Airtable writes. No --apply.
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname, basename } from "path";
import { fileURLToPath } from "url";
import { auditBrandExclusion } from "../lib/independent-census/brand-exclusion-audit.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");
const DOCS_DIR = join(__dirname, "..", "docs", "data-intelligence");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error("--apply is not supported. Brand-exclusion audit is read-only.");
  }

  let input = "";
  let matchReport = "";
  let batchId = "";
  let minQuality = 40;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--input" && argv[i + 1]) input = argv[++i];
    else if (a.startsWith("--input=")) input = a.slice("--input=".length);
    else if (a === "--match-report" && argv[i + 1]) matchReport = argv[++i];
    else if (a.startsWith("--match-report="))
      matchReport = a.slice("--match-report=".length);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i];
    else if (a.startsWith("--batch-id=")) batchId = a.slice("--batch-id=".length);
    else if (a === "--min-quality" && argv[i + 1])
      minQuality = parseInt(argv[++i], 10);
    else if (a.startsWith("--min-quality="))
      minQuality = parseInt(a.slice("--min-quality=".length), 10);
  }

  if (!input) {
    throw new Error("Missing --input OSM dry-run JSON");
  }

  return {
    inputPath: join(process.cwd(), input.replace(/^"|"$/g, "")),
    matchPath: matchReport
      ? join(process.cwd(), matchReport.replace(/^"|"$/g, ""))
      : "",
    batchId,
    minQuality,
  };
}

function loadMatchMap(matchPath) {
  const map = new Map();
  if (!matchPath) return map;
  if (!existsSync(matchPath)) {
    throw new Error(`Match report not found: ${matchPath}`);
  }
  const data = JSON.parse(readFileSync(matchPath, "utf8"));
  const rows = data.matches || data.rows || data.candidates || [];
  for (const row of rows) {
    const id = String(row.sourceRecordId || row.source_record_id || "");
    if (id) map.set(id, row);
  }
  return map;
}

function toMarkdown(report) {
  const t = report.taxonomy;
  const lines = [
    `# Independent Census — Brand Exclusion Audit`,
    ``,
    `**Status:** \`independent_census_brand_exclusion_audit_ready\``,
    `**Version:** ${report.version}`,
    `**Generated:** ${report.generated_at}`,
    `**Batch:** ${report.batch_id}`,
    `**Airtable writes:** no`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| OSM candidates | ${report.candidate_count} |`,
    `| Active Brand Setup brands (dictionary) | ${report.active_brand_count} |`,
    `| Route → branded Autopilot (Active/Live) | ${t.branded_route_to_autopilot} |`,
    `| Known chain hold (not Active/Live) | ${t.known_chain_hold_not_active} |`,
    `| Steward possible branded | ${t.steward_possible_branded} |`,
    `| Weak identity hold | ${t.weak_identity_hold} |`,
    `| Independent unaffiliated pool | ${t.independent_total} |`,
    `| Independent missing city | ${t.independent_missing_city} |`,
    `| Independent missing website | ${t.independent_missing_website} |`,
    `| Independent likely already in legacy Census | ${t.independent_likely_already_in_legacy_census} |`,
    `| Independent likely new vs legacy | ${t.independent_likely_new_vs_legacy} |`,
    `| Independent high quality (≥70) | ${t.independent_high_quality} |`,
    `| L1 promote-ready proxy (name+country+website+q≥55, not likely_existing) | ${t.independent_promote_ready_l1_proxy} |`,
    ``,
    `## Route counts`,
    ``,
    `| Route | Count |`,
    `| --- | ---: |`,
    ...Object.entries(report.route_counts).map(([k, v]) => `| \`${k}\` | ${v} |`),
    ``,
    `## Top matched brands (excluded from independent lane)`,
    ``,
    `| Brand | Count |`,
    `| --- | ---: |`,
    ...report.top_matched_brands.map((b) => `| ${b.brand} | ${b.count} |`),
    ``,
    `## Promote-ready sample (independent lane)`,
    ``,
    `| Name | City | Website | Quality |`,
    `| --- | --- | --- | ---: |`,
    ...report.promote_ready_sample.map(
      (r) =>
        `| ${r.name || ""} | ${r.city || ""} | ${r.website || ""} | ${r.quality ?? ""} |`
    ),
    ``,
    `## Learning taxonomy (batch-learning)`,
    ``,
    `- \`learned_code_rule\`: brand-exclusion gate via Active/Live dictionary + official domains`,
    `- \`learned_validation_rule\`: independent L1 proxy requires website + quality ≥ 55 + not legacy duplicate`,
    `- \`Webhound_candidate\`: residual hard cases = independent with no website + no city (after OSM+Wikidata)`,
    `- Do **not** send branded_route rows to Webhound; route to Autopilot coverage`,
    ``,
    `## Next`,
    ``,
    `1. Wikidata dry-run + evidence attach for promote-ready independents`,
    `2. Steward review \`steward_possible_branded\``,
    `3. Gated promote-plan dry-run into Hotel Property Census (Affiliation Status = Independent)`,
    `4. Optional Webhound only on 10–25 hard residual cases`,
    ``,
  ];
  return lines.join("\n");
}

async function main() {
  const args = parseArgs();
  if (!existsSync(args.inputPath)) {
    throw new Error(`Input not found: ${args.inputPath}`);
  }

  const osm = JSON.parse(readFileSync(args.inputPath, "utf8"));
  const candidates = osm.candidates || osm.filtering?.candidates;
  if (!Array.isArray(candidates)) {
    throw new Error("Invalid OSM report: missing candidates array");
  }

  const batchId =
    args.batchId ||
    osm.batchId ||
    basename(args.inputPath, ".json").replace(/^independent-census-osm-dry-run-/, "");

  const matchMap = loadMatchMap(args.matchPath);
  const audit = auditBrandExclusion(candidates, {
    region: "CALA",
    minQualityForIndependent: args.minQuality,
    matchRowsBySourceId: matchMap,
  });

  const report = {
    ...audit,
    batch_id: batchId,
    geography: osm.geography || { country: "Dominican Republic" },
    osm_input: args.inputPath,
    match_report: args.matchPath || null,
    prior_osm_baseline_batch: "osm-dominican-republic-hotel-focused-2026-05-20",
  };

  mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-brand-exclusion-${batchId}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-brand-exclusion-${batchId}.csv`
  );
  const mdPath = join(
    REPORTS_DIR,
    `independent-census-brand-exclusion-${batchId}.md`
  );
  const docPath = join(
    DOCS_DIR,
    `independent-census-brand-exclusion-dominican-republic.md`
  );

  writeJson(jsonPath, report);
  writeCsv(csvPath, report.rows, [
    "sourceRecordId",
    "rawHotelName",
    "rawCity",
    "rawCountry",
    "rawBrand",
    "rawWebsite",
    "qualityScore",
    "qualityTier",
    "missingFields",
    "route",
    "reason",
    "matchedBrand",
    "matchedFamily",
    "independentLaneEligible",
    "censusMatchConfidence",
    "censusRecommendedAction",
    "matchedCensusName",
    "signalKinds",
  ]);
  const md = toMarkdown(report);
  writeFileSync(mdPath, md, "utf8");
  mkdirSync(DOCS_DIR, { recursive: true });
  writeFileSync(docPath, md, "utf8");

  console.log(`Brand-exclusion audit: ${batchId}`);
  console.log(`  candidates: ${report.candidate_count}`);
  console.log(`  independent pool: ${report.taxonomy.independent_total}`);
  console.log(
    `  branded → autopilot: ${report.taxonomy.branded_route_to_autopilot}`
  );
  console.log(
    `  L1 promote-ready proxy: ${report.taxonomy.independent_promote_ready_l1_proxy}`
  );
  console.log(`  wrote: ${jsonPath}`);
  console.log(`  wrote: ${csvPath}`);
  console.log(`  wrote: ${mdPath}`);
  console.log(`  wrote: ${docPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
