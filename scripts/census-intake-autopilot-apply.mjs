#!/usr/bin/env node
/**
 * Census Intake Autopilot — gated Hotel Property Census INSERT apply.
 *
 * Default: dry-run (re-dedupe + preview). Live writes require:
 *   --apply --enable-production-writes + all INTAKE_APPLY_CONFIRMS + env flags.
 *
 * First apply cohort: no_hr only.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  INTAKE_APPLY_CONFIRMS,
  INTAKE_APPLY_STATUS,
  INTAKE_APPLY_VERSION,
  checkIntakeApplyEnv,
  defaultIntakeApplyReportPath,
  parseIntakeApplyArgs,
  runIntakeAutopilotApply,
} from "../lib/independent-census/intake-autopilot-apply.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const DOCS_DIR = join(root, "docs", "data-intelligence");

function toMarkdown(report) {
  return [
    `# Census Intake Autopilot — Apply`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Version:** ${report.version}`,
    `**Batch:** ${report.batch_id || ""}`,
    `**Cohort:** ${report.cohort || ""}`,
    `**Apply executed:** ${report.apply_executed}`,
    `**Airtable writes:** ${report.airtable_writes}`,
    `**Legacy Hotel Census:** forbidden (\`${report.legacy_hotel_census_used}\`)`,
    ``,
    `## Counts`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Inserts in bundle | ${report.inserts_in_bundle ?? 0} |`,
    `| Writable after re-dedupe | ${report.writable_after_rededupe ?? 0} |`,
    `| Blocked | ${report.blocked_count ?? 0} |`,
    `| Created | ${report.created_count ?? 0} |`,
    ``,
    `## Write target`,
    ``,
    `- ${report.write_target?.base} → ${report.write_target?.table} (\`${report.write_target?.table_id}\`)`,
    ``,
    `## Writable preview`,
    ``,
    `| Name | Brand | City | Identity Key |`,
    `| --- | --- | --- | --- |`,
    ...(report.writable_preview || [])
      .slice(0, 30)
      .map(
        (r) =>
          `| ${r.property_name} | ${r.current_brand} | ${r.city} | \`${r.identity_key}\` |`
      ),
    ``,
    `## Note`,
    ``,
    report.note || "",
    ``,
  ].join("\n");
}

async function main() {
  const args = parseIntakeApplyArgs();
  if (!args.approvalBundlePath) {
    console.error(
      "Usage: node scripts/census-intake-autopilot-apply.mjs --approval-bundle <path> [--cohort no_hr] [--apply --enable-production-writes ...confirms]"
    );
    console.error(`Required confirms:\n  ${INTAKE_APPLY_CONFIRMS.join("\n  ")}`);
    process.exit(1);
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);

  if (args.apply && !doWrite) {
    console.error("Apply blocked — missing confirms and/or env flags:");
    const missingConfirms = Object.entries(args.confirms)
      .filter(([, v]) => !v)
      .map(([k]) => k);
    if (missingConfirms.length) {
      console.error(`  confirms: ${missingConfirms.join(", ")}`);
    }
    if (envCheck.missing.length) {
      console.error(`  env: ${envCheck.missing.join(", ")}`);
    }
    process.exit(1);
  }

  const report = await runIntakeAutopilotApply({
    args,
    doWrite,
    useLiveAirtable: doWrite,
    env: process.env,
  });

  const batchId =
    report.batch_id ||
    "census-intake-apply";
  const outJson =
    args.output ||
    `reports/census-intake-autopilot-apply-${batchId}${doWrite ? "-applied" : "-dry-run"}.json`;
  const absJson = join(root, outJson);
  mkdirSync(dirname(absJson), { recursive: true });
  writeFileSync(absJson, JSON.stringify(report, null, 2));

  const md = toMarkdown(report);
  writeFileSync(
    join(root, "reports", `census-intake-autopilot-apply-${batchId}${doWrite ? "-applied" : "-dry-run"}.md`),
    md
  );
  mkdirSync(DOCS_DIR, { recursive: true });
  writeFileSync(
    join(DOCS_DIR, doWrite ? "census-intake-autopilot-apply.md" : "census-intake-autopilot-apply-dry-run.md"),
    md
  );

  console.log(`Census Intake Autopilot APPLY (${INTAKE_APPLY_VERSION})`);
  console.log(`  status: ${report.status}`);
  console.log(`  apply_executed: ${report.apply_executed}`);
  console.log(`  airtable_writes: ${report.airtable_writes}`);
  console.log(`  writable: ${report.writable_after_rededupe} / bundle ${report.inserts_in_bundle}`);
  console.log(`  blocked: ${report.blocked_count}`);
  console.log(`  created: ${report.created_count}`);
  console.log(`  wrote: ${outJson}`);
  if (report.status === INTAKE_APPLY_STATUS.BLOCKED) {
    console.error(`  blocked_reason: ${report.blocked_reason}`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
