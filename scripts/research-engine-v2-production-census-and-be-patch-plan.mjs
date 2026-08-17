/**
 * Production Census schema plan + Mexico VIC dry-run + Brand Explorer patch path.
 * Read-only — no Airtable writes.
 *
 *   npm run research-engine-v2:production-census-and-be-patch-plan
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runProductionCensusAndBePatchPlan,
  renderSchemaPlanMarkdown,
  renderCensusDryRunMarkdown,
  renderBePatchPathMarkdown,
} from "../lib/research-engine-v2/production-census-and-be-patch-plan.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}

function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

async function main() {
  console.log("[production-census-be-plan] read-only plan + dry-run (execute:false)");
  const report = await runProductionCensusAndBePatchPlan();

  const schemaMd = renderSchemaPlanMarkdown(report.schema_plan);
  const dryMd = renderCensusDryRunMarkdown(report.census_dry_run);
  const beMd = renderBePatchPathMarkdown(report.brand_explorer_patch_path);

  writeJson(join(REPORTS, "production-census-airtable-schema-plan.json"), {
    ...report.schema_plan,
    acceptance_status: report.acceptance_status,
    generated_at: report.generated_at,
  });
  writeMd(join(REPORTS, "production-census-airtable-schema-plan.md"), schemaMd);
  writeMd(
    join(DOCS, "production-census-airtable-schema-plan.md"),
    `${schemaMd}\n\n## Scope\n\nSchema plan only. No Airtable table creation or writes from this command.\n`
  );

  writeJson(join(REPORTS, "mexico-vic-production-census-dry-run.json"), {
    ...report.census_dry_run,
    acceptance_status: report.acceptance_status,
    generated_at: report.generated_at,
  });
  writeMd(join(REPORTS, "mexico-vic-production-census-dry-run.md"), dryMd);
  writeMd(
    join(DOCS, "mexico-vic-production-census-dry-run.md"),
    `${dryMd}\n\n## Scope\n\nDry-run only. No production Census writes. Frozen VIC artifacts unmodified.\n`
  );

  writeJson(join(REPORTS, "brand-explorer-production-patch-path.json"), {
    ...report.brand_explorer_patch_path,
    acceptance_status: report.acceptance_status,
    generated_at: report.generated_at,
  });
  writeMd(join(REPORTS, "brand-explorer-production-patch-path.md"), beMd);
  writeMd(
    join(DOCS, "brand-explorer-production-patch-path.md"),
    `${beMd}\n\n## Scope\n\nPath design only. No Brand Explorer production records modified. Frozen 62 baseline unmodified.\n`
  );

  // Combined summary for operators
  writeJson(join(REPORTS, "production-census-and-be-patch-plan-summary.json"), {
    acceptance_status: report.acceptance_status,
    generated_at: report.generated_at,
    summary: report.summary,
    production_writes_occurred: report.production_writes_occurred,
    brand_explorer_production_modified: report.brand_explorer_production_modified,
    frozen_vic_modified: report.frozen_vic_modified,
    frozen_62_modified: report.frozen_62_modified,
  });

  console.log(
    JSON.stringify(
      {
        acceptance_status: report.acceptance_status,
        tables_already_exist: report.summary.tables_already_exist,
        schema_must_be_created_manually: report.summary.schema_must_be_created_manually,
        dry_run_total: report.summary.dry_run_count_666,
        to_create: report.summary.records_to_create,
        held: report.summary.held_records,
        production_census_write_may_proceed: report.production_census_write_may_proceed,
        be_patch_blocked: report.brand_explorer_production_patch_remains_blocked,
        be_recommendation: report.recommended_be_option,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[production-census-be-plan] FAILED", err);
  process.exitCode = 1;
});
