#!/usr/bin/env node
/**
 * Census intake — known-chain Official URL enrichment (report-only).
 * Optionally merges Google Places dry-run results, then simulates re-gates.
 * Never writes Airtable.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { runKnownChainOfficialUrlEnrichmentBatch } from "../lib/independent-census/known-chain-official-url-enrichment.js";

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
    googlePath: get("--google", ""),
    output: get("--output", ""),
    mdOutput: get("--md", ""),
    reasonFilter: get("--reason", "missing_official_property_url"),
  };
}

function loadJson(rel) {
  return JSON.parse(readFileSync(join(root, rel), "utf8"));
}

function toMarkdown(report) {
  const proposed = (report.enrichments || [])
    .filter((e) => e.apply_as_official_url_candidate)
    .slice(0, 20);
  const lift = (report.enrichments || [])
    .filter((e) => e.re_gate?.new_decision === "auto_insert" && e.re_gate?.decision_changed)
    .slice(0, 15);
  return [
    `# Known-chain Official URL enrichment (dry-run)`,
    ``,
    `**Mode:** report-only (no Airtable writes)`,
    `**Version:** ${report.version}`,
    `**Generated:** ${report.generated_at}`,
    `**Plan batch:** ${report.plan_batch_id || ""}`,
    `**Google Places input:** ${report.google_path || "(none)"}`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Input (missing URL) | ${report.input_count} |`,
    `| Proposed Official URL | ${report.proposed_official_url_count} |`,
    `| High confidence | ${report.high_confidence_count} |`,
    `| Brand search leads | ${report.brand_search_leads} |`,
    `| Brand hosts unmapped | ${report.brand_hosts_unmapped} |`,
    `| Simulated auto_insert lift | ${report.simulated_auto_insert_lift} |`,
    ``,
    `## Policy`,
    ``,
    `- Brand homepage alone ≠ Official Property URL`,
    `- Search lead URLs are discovery aids only`,
    `- Google websiteUri only promoted when usable / non-denylist`,
    `- Brand-domain corroboration preferred for known chains`,
    ``,
    `## Proposed sample`,
    ``,
    `| Name | Brand | Source | Confidence | Re-gate |`,
    `| --- | --- | --- | --- | --- |`,
    ...proposed.map(
      (e) =>
        `| ${e.property_name} | ${e.current_brand} | ${e.proposal_source} | ${e.proposal_confidence} | ${e.re_gate?.new_decision || ""} |`
    ),
    ``,
    `## Simulated auto_insert lift sample`,
    ``,
    `| Name | Prior | New | URL host |`,
    `| --- | --- | --- | --- |`,
    ...lift.map((e) => {
      let host = "";
      try {
        host = new URL(e.proposed_official_property_url).hostname;
      } catch {
        host = "";
      }
      return `| ${e.property_name} | ${e.re_gate.prior_decision} | ${e.re_gate.new_decision} | ${host} |`;
    }),
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  const plan = loadJson(args.planPath);
  const rows = (plan.rows || []).filter((r) => {
    if (args.reasonFilter === "steward_hold") {
      return r.decision === "steward_hold";
    }
    if (args.reasonFilter.includes(",")) {
      const set = new Set(args.reasonFilter.split(",").map((s) => s.trim()));
      return (r.reasons || []).some((x) => set.has(x));
    }
    return (r.reasons || []).includes(args.reasonFilter);
  });

  let googleById = new Map();
  let googlePath = args.googlePath;
  if (!googlePath) {
    const slug = plan.batch_id || "osm-dominican-republic-hotel-focused-2026-08-07";
    const guess = `reports/census-intake-google-places-url-dry-run-${slug}.json`;
    if (existsSync(join(root, guess))) googlePath = guess;
  }
  if (googlePath) {
    const google = loadJson(googlePath);
    for (const r of google.results || []) {
      if (r.source_record_id) googleById.set(String(r.source_record_id), r);
    }
  }

  const batch = runKnownChainOfficialUrlEnrichmentBatch(rows, googleById);
  const slug =
    plan.batch_id ||
    plan.batchId ||
    "census-intake-known-chain-url";
  const outJson =
    args.output ||
    `reports/census-intake-known-chain-url-enrichment-dry-run-${slug}.json`;
  const outMd =
    args.mdOutput ||
    `docs/data-intelligence/census-intake-known-chain-url-enrichment-dry-run.md`;

  const report = {
    ...batch,
    plan_path: args.planPath,
    plan_batch_id: slug,
    google_path: googlePath || null,
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
        proposed_official_url_count: report.proposed_official_url_count,
        high_confidence_count: report.high_confidence_count,
        simulated_auto_insert_lift: report.simulated_auto_insert_lift,
        brand_hosts_unmapped: report.brand_hosts_unmapped,
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
