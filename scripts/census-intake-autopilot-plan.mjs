/**
 * Census Intake Autopilot — PLAN mode (read-only).
 *
 * Runs deterministic gates on dual-lane intake payloads.
 * No Hotel Property Census writes. Legacy Hotel Census forbidden.
 *
 * Usage:
 *   node scripts/census-intake-autopilot-plan.mjs \
 *     --dual-lane reports/dual-lane-census-intake-plan-….json \
 *     --hpc-match reports/independent-census-hpc-match-….json \
 *     [--osm reports/independent-census-osm-dry-run-….json] \
 *     [--promote-plan reports/independent-census-dr-promote-plan-….json]
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  planIntakeAutopilot,
  INTAKE_AUTOPILOT_GATES_VERSION,
} from "../lib/independent-census/intake-autopilot-gates.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");
const DOCS_DIR = join(__dirname, "..", "docs", "data-intelligence");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      "--apply not supported on plan script. Use a future controlled/apply runner with confirms."
    );
  }
  let dualLane = "";
  let hpcMatch = "";
  let osm = "";
  let promote = "";
  let batchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dual-lane" && argv[i + 1]) dualLane = argv[++i];
    else if (a.startsWith("--dual-lane=")) dualLane = a.slice("--dual-lane=".length);
    else if (a === "--hpc-match" && argv[i + 1]) hpcMatch = argv[++i];
    else if (a.startsWith("--hpc-match=")) hpcMatch = a.slice("--hpc-match=".length);
    else if (a === "--osm" && argv[i + 1]) osm = argv[++i];
    else if (a.startsWith("--osm=")) osm = a.slice("--osm=".length);
    else if (a === "--promote-plan" && argv[i + 1]) promote = argv[++i];
    else if (a.startsWith("--promote-plan="))
      promote = a.slice("--promote-plan=".length);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i];
    else if (a.startsWith("--batch-id=")) batchId = a.slice("--batch-id=".length);
  }

  if (!dualLane || !hpcMatch) {
    throw new Error("Required: --dual-lane and --hpc-match");
  }

  return {
    dualLanePath: join(process.cwd(), dualLane),
    hpcPath: join(process.cwd(), hpcMatch),
    osmPath: osm ? join(process.cwd(), osm) : "",
    promotePath: promote ? join(process.cwd(), promote) : "",
    batchId,
  };
}

function loadJson(p) {
  if (!existsSync(p)) throw new Error(`Not found: ${p}`);
  return JSON.parse(readFileSync(p, "utf8"));
}

function toMarkdown(report) {
  const c = report.counts;
  return [
    `# Census Intake Autopilot — Plan`,
    ``,
    `**Status:** \`census_intake_autopilot_plan_ready\``,
    `**Gates version:** ${report.version}`,
    `**Batch:** ${report.batch_id}`,
    `**Generated:** ${report.generated_at}`,
    `**Mode:** plan (no Airtable writes)`,
    `**Dedupe SoT:** Hotel Property Census only`,
    `**Legacy Hotel Census:** forbidden`,
    ``,
    `## Decision counts`,
    ``,
    `| Decision | Count |`,
    `| --- | ---: |`,
    `| auto_insert (total) | ${c.auto_insert || 0} |`,
    `| — no Human Review | ${c.auto_insert_no_hr || 0} |`,
    `| — with Human Review | ${c.auto_insert_human_review || 0} |`,
    `| production_writable_insert | ${c.production_writable_insert || 0} |`,
    `| auto_enrich_only (already in HPC) | ${c.auto_enrich_only || 0} |`,
    `| steward_hold | ${c.steward_hold || 0} |`,
    `| reject | ${c.reject || 0} |`,
    `| input rows | ${report.input_count} |`,
    ``,
    `## Top gate reasons`,
    ``,
    `| Reason | Count |`,
    `| --- | ---: |`,
    ...report.top_reasons.map((r) => `| \`${r.reason}\` | ${r.count} |`),
    ``,
    `## Auto-insert sample`,
    ``,
    `| Name | Brand | City | HR | Class |`,
    `| --- | --- | --- | --- | --- |`,
    ...report.auto_insert_sample.map(
      (r) =>
        `| ${r.property_name} | ${r.current_brand} | ${r.city} | ${r.human_review_required} | ${r.intake_class} |`
    ),
    ``,
    `## Steward hold sample`,
    ``,
    `| Name | Reasons |`,
    `| --- | --- |`,
    ...report.steward_sample.map(
      (r) => `| ${r.property_name} | ${(r.reasons || []).join("; ")} |`
    ),
    ``,
    `## Reject sample`,
    ``,
    `| Name | Reasons |`,
    `| --- | --- |`,
    ...report.reject_sample.map(
      (r) => `| ${r.property_name} | ${(r.reasons || []).join("; ")} |`
    ),
    ``,
    `## Next`,
    ``,
    `1. Review steward_hold / reject reason distribution (tighten gates via fixtures)`,
    `2. Controlled mode: propose patches only for \`production_writable_insert\``,
    `3. Apply only with Autopilot confirms (HPC only; no Brand Explorer; no legacy)`,
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  const dual = loadJson(args.dualLanePath);
  const hpc = loadJson(args.hpcPath);
  if (hpc.legacy_hotel_census_used === true) {
    throw new Error("HPC match used legacy Hotel Census — forbidden");
  }
  if (dual.legacy_hotel_census_used === true) {
    throw new Error("Dual-lane plan used legacy Hotel Census — forbidden");
  }

  const batchId =
    args.batchId || dual.batch_id || hpc.batchId || "intake-autopilot-plan";

  const hpcBySourceId = new Map();
  for (const m of hpc.matches || []) {
    hpcBySourceId.set(String(m.sourceRecordId || ""), m);
  }

  const qualityBySourceId = new Map();
  if (args.osmPath) {
    const osm = loadJson(args.osmPath);
    for (const c of osm.candidates || []) {
      if (c.sourceRecordId != null && typeof c.qualityScore === "number") {
        qualityBySourceId.set(String(c.sourceRecordId), c.qualityScore);
      }
    }
  }

  const wikidataBySourceId = new Map();
  if (args.promotePath) {
    const promote = loadJson(args.promotePath);
    for (const r of promote.rows || []) {
      if (r.sourceRecordId) {
        wikidataBySourceId.set(String(r.sourceRecordId), {
          matchConfidence: r.wikidataMatchConfidence || "",
        });
      }
    }
  }

  const plan = planIntakeAutopilot(dual, {
    hpcBySourceId,
    qualityBySourceId,
    wikidataBySourceId,
  });

  const report = {
    ...plan,
    batch_id: batchId,
    mode: "plan",
    dual_lane_report: args.dualLanePath,
    hpc_match_report: args.hpcPath,
    scope: "independent-and-known-chain-intake",
  };

  mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = join(REPORTS_DIR, `census-intake-autopilot-plan-${batchId}.json`);
  const csvPath = join(REPORTS_DIR, `census-intake-autopilot-plan-${batchId}.csv`);
  const mdPath = join(REPORTS_DIR, `census-intake-autopilot-plan-${batchId}.md`);
  const docPath = join(DOCS_DIR, `census-intake-autopilot-plan.md`);

  writeJson(jsonPath, report);
  writeCsv(
    csvPath,
    report.rows.map((r) => ({
      source_record_id: r.source_record_id,
      property_name: r.property_name,
      current_brand: r.current_brand,
      city: r.city,
      lane: r.lane,
      intake_class: r.intake_class,
      decision: r.decision,
      identity_confidence: r.identity_confidence,
      human_review_required: r.human_review_required,
      production_writable_insert: r.production_writable_insert,
      queue_autopilot_enrichment: r.queue_autopilot_enrichment,
      reasons: (r.reasons || []).join("|"),
      checks_passed: (r.checks_passed || []).join("|"),
    }))
  );
  const md = toMarkdown(report);
  writeFileSync(mdPath, md, "utf8");
  mkdirSync(DOCS_DIR, { recursive: true });
  writeFileSync(docPath, md, "utf8");
  if (String(batchId).includes("url-enriched")) {
    writeFileSync(
      join(DOCS_DIR, `census-intake-autopilot-plan-url-enriched.md`),
      md,
      "utf8"
    );
  }

  console.log(`Census Intake Autopilot PLAN (${INTAKE_AUTOPILOT_GATES_VERSION})`);
  console.log(`  batch: ${batchId}`);
  console.log(`  input: ${report.input_count}`);
  console.log(`  auto_insert: ${report.counts.auto_insert} (no HR ${report.counts.auto_insert_no_hr}, HR ${report.counts.auto_insert_human_review})`);
  console.log(`  production_writable_insert: ${report.counts.production_writable_insert}`);
  console.log(`  auto_enrich_only: ${report.counts.auto_enrich_only}`);
  console.log(`  steward_hold: ${report.counts.steward_hold}`);
  console.log(`  reject: ${report.counts.reject}`);
  console.log(`  wrote: ${jsonPath}`);
  console.log(`  wrote: ${docPath}`);
  console.log("✓ Plan only — no Airtable writes.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
