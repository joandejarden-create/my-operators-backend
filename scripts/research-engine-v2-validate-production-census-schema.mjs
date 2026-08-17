/**
 * Validate production Census schema after create.
 *
 *   npm run research-engine-v2:validate-production-census-schema
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runSchemaValidation,
  renderValidationMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-schema-create.js";

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
  console.log("[census-schema-validate] validating Hotel Property * tables");
  const report = await runSchemaValidation();
  writeJson(join(REPORTS, "production-census-schema-validation.json"), report);
  writeMd(join(REPORTS, "production-census-schema-validation.md"), renderValidationMarkdown(report));

  const doc = [
    `# Production Census Schema Created`,
    ``,
    `**Validation status:** \`${report.status}\``,
    `**Base:** \`${report.base_id_masked}\``,
    `**Generated:** ${report.generated_at}`,
    ``,
    `## Intent`,
    ``,
    `- Four production Census tables on Deal Capture Platform`,
    `- Schema only — zero data records`,
    `- Brand Explorer / legacy census / VIC freeze / frozen 62 untouched`,
    ``,
    `## Checks`,
    ``,
    ...report.checks.map((c) => `- **${c.id}:** ${c.pass ? "PASS" : "FAIL"}`),
    ``,
    `## Next`,
    ``,
    report.status === STATUS.VALIDATION_PASS
      ? "Re-run `npm run research-engine-v2:production-census-and-be-patch-plan` — expect `production_census_dry_run_ready_for_founder_approval`."
      : "Fix validation failures before census record writes.",
    ``,
  ].join("\n");

  writeMd(join(DOCS, "production-census-schema-created.md"), doc);

  console.log(
    JSON.stringify(
      {
        status: report.status,
        can_proceed_dry_run: report.production_census_dry_run_can_proceed,
        failed: report.checks.filter((c) => !c.pass).map((c) => c.id),
      },
      null,
      2
    )
  );
  if (report.status !== STATUS.VALIDATION_PASS) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-schema-validate] FAILED", err);
  process.exitCode = 1;
});
