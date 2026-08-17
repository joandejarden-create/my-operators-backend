#!/usr/bin/env node
/**
 * Merge high brand-domain Official URL proposals into dual-lane plan (report-only).
 *
 * Usage:
 *   node scripts/census-intake-merge-url-enrichment.mjs \
 *     --dual-lane reports/dual-lane-….json \
 *     --enrichment reports/census-intake-known-chain-url-enrichment-….json
 */
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { mergeUrlEnrichmentIntoDualLane } from "../lib/independent-census/merge-url-enrichment-into-dual-lane.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  return {
    dualLanePath: get(
      "--dual-lane",
      "reports/dual-lane-census-intake-plan-osm-dominican-republic-hotel-focused-2026-08-07.json"
    ),
    enrichmentPath: get(
      "--enrichment",
      "reports/census-intake-known-chain-url-enrichment-dry-run-osm-dominican-republic-hotel-focused-2026-08-07.json"
    ),
    output: get("--output", ""),
    summaryOut: get("--summary", ""),
    batchId: get("--batch-id", ""),
    allowMedium: argv.includes("--allow-medium"),
    allowOffBrand: argv.includes("--allow-off-brand"),
  };
}

function loadJson(rel) {
  const p = join(root, rel);
  if (!existsSync(p)) throw new Error(`Not found: ${rel}`);
  return JSON.parse(readFileSync(p, "utf8"));
}

function toMarkdown(summary) {
  return [
    `# Census intake URL enrichment merge`,
    ``,
    `**Version:** ${summary.version}`,
    `**Batch:** ${summary.batch_id}`,
    `**Airtable writes:** no`,
    ``,
    `## Policy`,
    ``,
    `- High confidence only: \`${summary.policy.require_high_confidence}\``,
    `- Brand-domain only: \`${summary.policy.require_brand_domain}\``,
    `- Overwrite existing Official URL: \`${summary.policy.overwrite_existing_official_url}\``,
    ``,
    `## Counts`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Applied | ${summary.applied_count} |`,
    `| Skipped (existing URL) | ${summary.skipped_existing_count} |`,
    `| Not found in dual-lane | ${summary.not_found_count} |`,
    ``,
    `## Applied sample`,
    ``,
    `| Name | Brand | URL |`,
    `| --- | --- | --- |`,
    ...(summary.applied || [])
      .slice(0, 25)
      .map(
        (a) =>
          `| ${a.property_name} | ${a.current_brand} | ${a.proposed_official_property_url} |`
      ),
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  const dual = loadJson(args.dualLanePath);
  const enrichmentReport = loadJson(args.enrichmentPath);

  const { dual_lane, summary } = mergeUrlEnrichmentIntoDualLane(
    dual,
    enrichmentReport.enrichments || [],
    {
      batchId: args.batchId || undefined,
      enrichmentReportPath: args.enrichmentPath,
      policy: {
        require_high_confidence: !args.allowMedium,
        require_brand_domain: !args.allowOffBrand,
        require_apply_candidate: true,
        overwrite_existing_official_url: false,
      },
    }
  );

  const outJson =
    args.output ||
    `reports/dual-lane-census-intake-plan-${dual_lane.batch_id}.json`;
  const summaryJson =
    args.summaryOut ||
    `reports/census-intake-url-enrichment-merge-${dual_lane.batch_id}.json`;
  const summaryMd = `docs/data-intelligence/census-intake-url-enrichment-merge.md`;

  mkdirSync(dirname(join(root, outJson)), { recursive: true });
  mkdirSync(dirname(join(root, summaryMd)), { recursive: true });
  writeFileSync(join(root, outJson), JSON.stringify(dual_lane, null, 2));
  writeFileSync(join(root, summaryJson), JSON.stringify(summary, null, 2));
  writeFileSync(join(root, summaryMd), toMarkdown(summary));

  console.log(
    JSON.stringify(
      {
        ok: true,
        dual_lane_output: outJson,
        summary_output: summaryJson,
        batch_id: dual_lane.batch_id,
        applied_count: summary.applied_count,
        skipped_existing_count: summary.skipped_existing_count,
        not_found_count: summary.not_found_count,
        airtable_writes: false,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
