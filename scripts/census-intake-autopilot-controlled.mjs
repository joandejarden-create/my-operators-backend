/**
 * Census Intake Autopilot — controlled dry-run (no writes by default).
 *
 * Builds exact Hotel Property Census insert proposals from plan auto_insert rows.
 *
 * Usage:
 *   node scripts/census-intake-autopilot-controlled.mjs \
 *     --plan reports/census-intake-autopilot-plan-….json \
 *     [--cohort all|no_hr|hr_only] [--max-records N]
 *
 * --apply is rejected until controlled dry-run is approved and a dedicated
 * apply path is enabled with full confirms.
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildIntakeControlledDryRun,
  INTAKE_CONTROLLED_VERSION,
  INTAKE_APPLY_CONFIRMS,
} from "../lib/independent-census/intake-autopilot-controlled.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");
const DOCS_DIR = join(__dirname, "..", "docs", "data-intelligence");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      [
        "--apply is not enabled on this controlled dry-run script.",
        "Review the dry-run approval bundle first.",
        `Future apply will require: ${INTAKE_APPLY_CONFIRMS.join(" ")}`,
      ].join(" ")
    );
  }

  let plan = "";
  let cohort = "all";
  let maxRecords = null;
  let batchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--plan" && argv[i + 1]) plan = argv[++i];
    else if (a.startsWith("--plan=")) plan = a.slice("--plan=".length);
    else if (a === "--cohort" && argv[i + 1]) cohort = argv[++i];
    else if (a.startsWith("--cohort=")) cohort = a.slice("--cohort=".length);
    else if (a === "--max-records" && argv[i + 1])
      maxRecords = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-records="))
      maxRecords = parseInt(a.slice("--max-records=".length), 10);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i];
    else if (a.startsWith("--batch-id=")) batchId = a.slice("--batch-id=".length);
  }

  if (!plan) throw new Error("Missing --plan (intake Autopilot plan JSON)");
  if (!["all", "no_hr", "hr_only"].includes(cohort)) {
    throw new Error("--cohort must be all|no_hr|hr_only");
  }

  return {
    planPath: join(process.cwd(), plan),
    cohort,
    maxRecords,
    batchId,
  };
}

function toMarkdown(report) {
  const c = report.counts;
  const passSample = report.proposals
    .filter((p) => p.validation_pass)
    .slice(0, 25);
  const failSample = report.proposals
    .filter((p) => !p.validation_pass)
    .slice(0, 15);

  return [
    `# Census Intake Autopilot — Controlled Dry-Run`,
    ``,
    `**Status:** \`${report.approval_bundle_ready ? "census_intake_controlled_dry_run_ready_for_apply_gate" : "census_intake_controlled_dry_run_validation_failures"}\``,
    `**Version:** ${report.version}`,
    `**Batch:** ${report.batch_id}`,
    `**Generated:** ${report.generated_at}`,
    `**Airtable writes:** no`,
    `**Write target:** ${report.write_target.base} → ${report.write_target.table} (\`${report.write_target.table_id}\`)`,
    `**Legacy Hotel Census:** forbidden`,
    `**Cohort:** ${report.cohort}`,
    ``,
    `## Counts`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Proposals | ${c.proposals} |`,
    `| Validation pass | ${c.validation_pass} |`,
    `| Validation fail | ${c.validation_fail} |`,
    `| No Human Review | ${c.no_hr} |`,
    `| With Human Review | ${c.with_hr} |`,
    `| Queue Autopilot enrichment | ${c.queue_enrichment} |`,
    `| Approval bundle ready | ${report.approval_bundle_ready} |`,
    ``,
    `## Apply confirms (future — not run)`,
    ``,
    ...report.apply_confirms_required.map((f) => `- \`${f}\``),
    ``,
    `## Validation-pass sample`,
    ``,
    `| Name | Brand | City | HR | Identity |`,
    `| --- | --- | --- | --- | --- |`,
    ...passSample.map(
      (p) =>
        `| ${p.sanitized_payload_preview["Property Name"]} | ${p.sanitized_payload_preview["Current Brand"]} | ${p.sanitized_payload_preview.City} | ${p.human_review_required} | ${p.identity_confidence} |`
    ),
    ``,
    `## Validation-fail sample`,
    ``,
    `| Name | Failures |`,
    `| --- | --- |`,
    ...failSample.map(
      (p) =>
        `| ${p.sanitized_payload_preview?.["Property Name"] || p.source_record_id} | ${(p.validation_failures || []).join("; ")} |`
    ),
    ``,
    `## Data contract snapshot`,
    ``,
    `- **Table:** Hotel Property Census`,
    `- **Field mapping:** intake dual-lane → \`INTAKE_INSERT_ALLOWED_FIELDS\``,
    `- **Required:** Property Name, Property Identity Key, Country, City, Current Brand, Affiliation Status, Family / Source Family, Source URL, VIC Freeze Hash, Production Use Status, Enrichment Status, Human Review Required`,
    `- **Forbidden:** owner/operator/dates/momentum/Brand Explorer/Company Validated`,
    ``,
    `## Change impact`,
    ``,
    `- **Classification:** High (Census inserts)`,
    `- **Rollback:** delete by VIC Freeze Hash / Property Identity Key prefix \`osm_do_\` for this batch`,
    `- **Modules:** intake-autopilot-controlled, Hotel Property Census only`,
    ``,
    `## Next`,
    ``,
    `1. Spot-check validation-fail rows (if any)`,
    `2. Prefer first apply cohort \`--cohort no_hr\``,
    `3. Explicit founder approval + apply script with all confirms`,
    ``,
  ].join("\n");
}

async function main() {
  const args = parseArgs();
  if (!existsSync(args.planPath)) throw new Error(`Not found: ${args.planPath}`);
  const plan = JSON.parse(readFileSync(args.planPath, "utf8"));
  if (plan.legacy_hotel_census_used === true) {
    throw new Error("Plan used legacy Hotel Census — forbidden");
  }

  const batchId = args.batchId || plan.batch_id || "intake-controlled";
  const dry = buildIntakeControlledDryRun(plan, {
    cohort: args.cohort,
    maxRecords: args.maxRecords,
  });

  const report = {
    ...dry,
    batch_id: batchId,
    plan_report: args.planPath,
    scope: "independent-and-known-chain-intake",
  };

  mkdirSync(REPORTS_DIR, { recursive: true });
  const slug = `${batchId}-${args.cohort}`;
  const jsonPath = join(REPORTS_DIR, `census-intake-autopilot-controlled-${slug}.json`);
  const csvPath = join(REPORTS_DIR, `census-intake-autopilot-controlled-${slug}.csv`);
  const mdPath = join(REPORTS_DIR, `census-intake-autopilot-controlled-${slug}.md`);
  const docPath = join(DOCS_DIR, `census-intake-autopilot-controlled-dry-run.md`);
  const bundlePath = join(
    REPORTS_DIR,
    `census-intake-autopilot-approval-bundle-${slug}.json`
  );

  writeJson(jsonPath, report);
  writeCsv(
    csvPath,
    report.proposals.map((p) => ({
      source_record_id: p.source_record_id,
      validation_pass: p.validation_pass,
      validation_failures: (p.validation_failures || []).join("|"),
      lane: p.lane,
      intake_class: p.intake_class,
      human_review_required: p.human_review_required,
      identity_confidence: p.identity_confidence,
      queue_autopilot_enrichment: p.queue_autopilot_enrichment,
      property_name: p.sanitized_payload_preview["Property Name"],
      current_brand: p.sanitized_payload_preview["Current Brand"],
      city: p.sanitized_payload_preview.City,
      country: p.sanitized_payload_preview.Country,
      affiliation_status: p.sanitized_payload_preview["Affiliation Status"],
      official_property_url: p.sanitized_payload_preview["Official Property URL"],
      property_identity_key: p.sanitized_payload_preview["Property Identity Key"],
      vic_freeze_hash: p.sanitized_payload_preview["VIC Freeze Hash"],
    }))
  );

  const md = toMarkdown(report);
  writeFileSync(mdPath, md, "utf8");
  mkdirSync(DOCS_DIR, { recursive: true });
  writeFileSync(docPath, md, "utf8");
  if (String(batchId).includes("url-enriched")) {
    writeFileSync(
      join(DOCS_DIR, `census-intake-autopilot-controlled-dry-run-url-enriched.md`),
      md,
      "utf8"
    );
  }

  // Compact approval bundle for future apply
  writeJson(bundlePath, {
    version: INTAKE_CONTROLLED_VERSION,
    batch_id: batchId,
    cohort: args.cohort,
    generated_at: report.generated_at,
    airtable_writes: false,
    approval_bundle_ready: report.approval_bundle_ready,
    write_target: report.write_target,
    apply_confirms_required: report.apply_confirms_required,
    legacy_hotel_census_used: false,
    inserts: report.proposals
      .filter((p) => p.validation_pass)
      .map((p) => ({
        source_record_id: p.source_record_id,
        lane: p.lane,
        intake_class: p.intake_class,
        human_review_required: p.human_review_required,
        identity_confidence: p.identity_confidence,
        quality_score: p.quality_score ?? null,
        fields: p.sanitized_payload_preview,
      })),
  });

  console.log(`Census Intake Autopilot CONTROLLED dry-run (${INTAKE_CONTROLLED_VERSION})`);
  console.log(`  batch: ${batchId} cohort=${args.cohort}`);
  console.log(`  proposals: ${report.counts.proposals}`);
  console.log(`  validation pass/fail: ${report.counts.validation_pass}/${report.counts.validation_fail}`);
  console.log(`  no HR / HR: ${report.counts.no_hr}/${report.counts.with_hr}`);
  console.log(`  approval_bundle_ready: ${report.approval_bundle_ready}`);
  console.log(`  wrote: ${jsonPath}`);
  console.log(`  wrote: ${bundlePath}`);
  console.log(`  wrote: ${docPath}`);
  console.log("✓ Controlled dry-run only — no Airtable writes. --apply disabled.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
