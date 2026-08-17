/**
 * Create production Census Airtable schema (tables/fields only).
 *
 *   npm run research-engine-v2:create-production-census-schema -- --dry-run
 *   npm run research-engine-v2:create-production-census-schema -- --apply \
 *     --confirm-production-census-schema-create \
 *     --confirm-schema-only \
 *     --confirm-no-record-writes \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-brand-basics-writes \
 *     --confirm-no-presentation-writes \
 *     --confirm-no-vic-mutation \
 *     --confirm-no-frozen-62-mutation
 *
 * Requires ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1 for apply.
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseArgs,
  runSchemaCreateDryRun,
  runSchemaCreateApply,
  renderDryRunMarkdown,
  renderApplyMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-schema-create.js";

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
  const args = parseArgs();
  console.log(
    `[census-schema] mode=${args.apply ? "apply" : "dry-run"} allow=${process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE === "1"}`
  );

  if (args.apply) {
    const report = await runSchemaCreateApply(process.argv.slice(2));
    writeJson(join(REPORTS, "production-census-schema-create-apply.json"), report);
    writeMd(join(REPORTS, "production-census-schema-create-apply.md"), renderApplyMarkdown(report));
    // Always also refresh dry-run artifact from embedded preflight
    if (report.dry_run_ref || report.tables_to_create) {
      /* apply report is primary */
    }
    console.log(
      JSON.stringify(
        {
          status: report.status,
          apply_executed: report.apply_executed,
          tables: (report.tables_created || []).map((t) => t.name),
          zero_records: report.zero_record_writes,
          errors: report.errors?.length || 0,
        },
        null,
        2
      )
    );
    if (report.status !== STATUS.APPLIED) process.exitCode = 1;
    return;
  }

  const dry = await runSchemaCreateDryRun();
  writeJson(join(REPORTS, "production-census-schema-create-dry-run.json"), dry);
  writeMd(join(REPORTS, "production-census-schema-create-dry-run.md"), renderDryRunMarkdown(dry));
  console.log(
    JSON.stringify(
      {
        status: dry.status,
        dry_run_pass: dry.dry_run_pass,
        tables: dry.tables_to_create?.length,
        fields: dry.fields_to_create?.length,
        conflicts: dry.conflicts?.length,
        allow_env: dry.allow_env_present,
      },
      null,
      2
    )
  );
  if (!dry.dry_run_pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-schema] FAILED", err);
  process.exitCode = 1;
});
