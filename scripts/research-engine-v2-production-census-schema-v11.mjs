/**
 * Production Census schema v1.1 — add future hotel intelligence fields.
 *
 *   npm run research-engine-v2:production-census-schema-v11 -- --dry-run
 *   npm run research-engine-v2:production-census-schema-v11 -- --apply ...confirms
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseV11Args,
  checkV11EnvFlags,
  runV11DryRun,
  runV11Apply,
  renderV11DryRunMarkdown,
  renderV11ApplyMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-schema-v11.js";

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
  const args = parseV11Args();
  const env = checkV11EnvFlags();
  console.log(`[census-v11] mode=${args.apply ? "apply" : "dry-run"} env_ok=${env.allOk}`);

  if (args.apply) {
    const report = await runV11Apply(process.argv.slice(2));
    writeJson(join(REPORTS, "production-census-schema-v11-apply.json"), report);
    writeMd(join(REPORTS, "production-census-schema-v11-apply.md"), renderV11ApplyMarkdown(report));

    const doc = [
      `# Production Census Schema v1.1`,
      ``,
      `**Acceptance:** \`${report.status}\``,
      `**Base:** \`${report.base_id_masked}\``,
      `**Census records:** ${report.census_record_count_after}`,
      `**Fields added:** ${(report.fields_added || []).length}`,
      ``,
      `## Principle`,
      ``,
      `Hotel Property Census remains the master property record. v1.1 adds future enrichment columns only.`,
      ``,
      `## Safety`,
      ``,
      `- Brand Explorer untouched: ${report.brand_explorer_untouched}`,
      `- No fake owner/operator/rooms/dates: ${report.no_fake_owner_operator_rooms_dates}`,
      `- No 0,0: ${report.no_zero_zero}`,
      `- Production Use Status preserved: ${report.production_use_status_ok}`,
      ``,
      `## Safe backfill`,
      ``,
      "```json",
      JSON.stringify(report.safe_backfill_applied, null, 2),
      "```",
      ``,
      `## Next`,
      ``,
      `Future source-backed enrichment only. Brand Explorer production patch remains blocked.`,
      ``,
    ].join("\n");
    writeMd(join(DOCS, "production-census-schema-v11.md"), doc);

    console.log(
      JSON.stringify(
        {
          status: report.status,
          fields_added: report.fields_added?.length,
          census_count: report.census_record_count_after,
          backfill: report.safe_backfill_applied?.records_patched,
          be_untouched: report.brand_explorer_untouched,
          errors: (report.field_errors?.length || 0) + (report.safe_backfill_applied?.errors?.length || 0),
        },
        null,
        2
      )
    );
    if (report.status !== STATUS.APPLIED) process.exitCode = 1;
    return;
  }

  const dry = await runV11DryRun();
  writeJson(join(REPORTS, "production-census-schema-v11-dry-run.json"), dry);
  writeMd(join(REPORTS, "production-census-schema-v11-dry-run.md"), renderV11DryRunMarkdown(dry));
  console.log(
    JSON.stringify(
      {
        status: dry.status,
        dry_run_pass: dry.dry_run_pass,
        to_add: dry.fields_to_add?.length,
        existed: dry.fields_already_existed?.length,
        conflicts: dry.conflicts?.length,
        census: dry.census_record_count,
        env_ok: dry.env_ok_for_apply,
      },
      null,
      2
    )
  );
  if (!dry.dry_run_pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-v11] FAILED", err);
  process.exitCode = 1;
});
