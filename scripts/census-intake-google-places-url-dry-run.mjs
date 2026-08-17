#!/usr/bin/env node
/**
 * Census intake — Google Places hotel URL lookup (API, report-only).
 * Targets steward_hold rows missing Official Property URL.
 * Never writes Airtable.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { resolveGoogleApiKey } from "../lib/location-verification/google-api-config.js";
import { runGooglePlacesHotelUrlLookupBatch } from "../lib/independent-census/google-places-hotel-url-lookup.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  return {
    planPath: get(
      "--plan",
      "reports/census-intake-autopilot-plan-osm-dominican-republic-hotel-focused-2026-08-07.json"
    ),
    output: get("--output", ""),
    mdOutput: get("--md", ""),
    limit: Number(get("--limit", "40")) || 40,
    maxRequests: Number(get("--max-requests", "40")) || 40,
    delayMs: Number(get("--delay-ms", "250")) || 250,
    reasonFilter: get("--reason", "missing_official_property_url"),
    dryRunNoApi: argv.includes("--dry-run-no-api"),
  };
}

function loadJson(rel) {
  return JSON.parse(readFileSync(join(root, rel), "utf8"));
}

function toMarkdown(report) {
  const sample = (report.results || [])
    .filter((r) => r.suggested_official_property_url)
    .slice(0, 15);
  return [
    `# Google Places hotel URL lookup (dry-run)`,
    ``,
    `**Mode:** report-only (no Airtable writes)`,
    `**Version:** ${report.version}`,
    `**Generated:** ${report.generated_at}`,
    `**Batch / plan:** ${report.plan_batch_id || ""}`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Input (missing URL) | ${report.input_count} |`,
    `| Processed | ${report.processed_count} |`,
    `| API requests | ${report.request_count} |`,
    `| Matched | ${report.matched} |`,
    `| No match | ${report.no_match} |`,
    `| Proposed Official URL | ${report.proposed_official_url_count} |`,
    `| High-confidence proposals | ${report.high_confidence_proposals} |`,
    `| Skipped (budget) | ${report.skipped_budget} |`,
    ``,
    `## Policy`,
    ``,
    `- Source type: \`google_places\` (restricted_refresh_required)`,
    `- Store Place ID + websiteUri in reports only; no photos/reviews`,
    `- \`googleMapsUri\` never used as Official Property URL`,
    `- Apply to Census only after steward / known-chain corroboration merge`,
    ``,
    `## Proposed sample`,
    ``,
    `| Name | Brand | Confidence | Website host |`,
    `| --- | --- | --- | --- |`,
    ...sample.map(
      (r) =>
        `| ${r.property_name} | ${r.current_brand} | ${r.match_confidence} | ${r.website_proposal?.host || ""} |`
    ),
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  const plan = loadJson(args.planPath);
  const rows = (plan.rows || []).filter((r) =>
    (r.reasons || []).includes(args.reasonFilter)
  );

  if (!rows.length) {
    console.error(`No rows with reason ${args.reasonFilter}`);
    process.exit(1);
  }

  const apiKey = resolveGoogleApiKey();
  if (!apiKey && !args.dryRunNoApi) {
    console.error(
      "Missing GOOGLE_PLACES_API_KEY / GOOGLE_MAPS_API_KEY. Use --dry-run-no-api for structure-only."
    );
    process.exit(1);
  }

  const batch = await runGooglePlacesHotelUrlLookupBatch(rows, {
    limit: args.limit,
    maxRequests: args.maxRequests,
    delayMs: args.delayMs,
    apiKey: args.dryRunNoApi ? "" : apiKey,
    searchTextFn: args.dryRunNoApi
      ? async () => []
      : undefined,
  });

  const slug =
    plan.batch_id ||
    plan.batchId ||
    "census-intake-google-places-url";
  const outJson =
    args.output ||
    `reports/census-intake-google-places-url-dry-run-${slug}.json`;
  const outMd =
    args.mdOutput ||
    `docs/data-intelligence/census-intake-google-places-url-dry-run.md`;

  const report = {
    ...batch,
    plan_path: args.planPath,
    plan_batch_id: slug,
    reason_filter: args.reasonFilter,
  };

  mkdirSync(dirname(join(root, outJson)), { recursive: true });
  mkdirSync(dirname(join(root, outMd)), { recursive: true });
  writeFileSync(join(root, outJson), JSON.stringify(report, null, 2));
  writeFileSync(join(root, outMd), toMarkdown(report));

  console.log(
    JSON.stringify(
      {
        ok: true,
        output: outJson,
        md: outMd,
        input_count: report.input_count,
        matched: report.matched,
        proposed_official_url_count: report.proposed_official_url_count,
        high_confidence_proposals: report.high_confidence_proposals,
        request_count: report.request_count,
        airtable_write: false,
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
