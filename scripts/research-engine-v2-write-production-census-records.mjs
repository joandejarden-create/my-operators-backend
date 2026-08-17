/**
 * Write Mexico VIC freeze into production Hotel Property Census tables.
 *
 *   npm run research-engine-v2:write-production-census-records -- --dry-run
 *   npm run research-engine-v2:write-production-census-records -- --apply ...confirms
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseWriteArgs,
  runCensusWriteDryRun,
  runCensusWriteApply,
  renderWriteDryRunMarkdown,
  renderWriteApplyMarkdown,
  STATUS,
  checkEnvFlags,
} from "../lib/research-engine-v2/production-census-write.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

async function main() {
  const args = parseWriteArgs();
  const env = checkEnvFlags();
  console.log(
    `[census-write] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`
  );

  if (!env.allOk && args.apply) {
    const blocked = {
      status: STATUS.CONFIRMATION_MISSING,
      apply_executed: false,
      env_flags: env.flags,
    };
    writeJson(join(REPORTS, "production-census-write-apply.json"), blocked);
    writeMd(
      join(REPORTS, "production-census-write-apply.md"),
      `# Blocked\n\n\`${STATUS.CONFIRMATION_MISSING}\`\n`
    );
    console.log(JSON.stringify(blocked, null, 2));
    process.exitCode = 1;
    return;
  }

  if (args.apply) {
    const report = await runCensusWriteApply(process.argv.slice(2));
    writeJson(join(REPORTS, "production-census-write-apply.json"), report);
    writeMd(join(REPORTS, "production-census-write-apply.md"), renderWriteApplyMarkdown(report));
    console.log(
      JSON.stringify(
        {
          status: report.status,
          created: report.records_created_by_table,
          updated: report.records_updated_by_table,
          reconciliation: report.reconciliation,
          errors: report.airtable_errors?.length || 0,
          duration_ms: report.duration_ms,
        },
        null,
        2
      )
    );
    if (report.status !== STATUS.APPLIED) process.exitCode = 1;
    return;
  }

  const dry = await runCensusWriteDryRun();
  writeJson(join(REPORTS, "production-census-write-dry-run.json"), dry);
  writeMd(join(REPORTS, "production-census-write-dry-run.md"), renderWriteDryRunMarkdown(dry));
  console.log(
    JSON.stringify(
      {
        status: dry.status,
        dry_run_pass: dry.dry_run_pass,
        counts: dry.counts,
        env_ok_for_apply: dry.env_ok_for_apply,
        conflicts: dry.conflicts?.length,
      },
      null,
      2
    )
  );
  if (!dry.dry_run_pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-write] FAILED", err);
  process.exitCode = 1;
});
