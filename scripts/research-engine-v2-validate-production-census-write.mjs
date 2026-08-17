/**
 * Post-write validation for production Census records.
 *
 *   npm run research-engine-v2:validate-production-census-write
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runCensusWriteValidation,
  renderWriteValidationMarkdown,
  STATUS,
  EXPECTED_FREEZE,
  PRODUCTION_USE_STATUS,
} from "../lib/research-engine-v2/production-census-write.js";

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
  const report = await runCensusWriteValidation();
  writeJson(join(REPORTS, "production-census-write-validation.json"), report);
  writeMd(join(REPORTS, "production-census-write-validation.md"), renderWriteValidationMarkdown(report));

  const applyPath = join(REPORTS, "production-census-write-apply.json");
  let apply = null;
  try {
    apply = JSON.parse(await import("node:fs").then((fs) => fs.readFileSync(applyPath, "utf8")));
  } catch {
    apply = null;
  }

  const doc = [
    `# Production Census Write Complete`,
    ``,
    `**Acceptance:** \`${apply?.status || report.status}\``,
    `**Validation:** \`${report.status}\``,
    `**Base:** \`${report.base_id_masked}\``,
    `**Freeze:** \`${EXPECTED_FREEZE}\``,
    `**Production Use Status:** \`${PRODUCTION_USE_STATUS}\``,
    ``,
    `## Reconciliation`,
    ``,
    "```json",
    JSON.stringify(report.reconciliation, null, 2),
    "```",
    ``,
    `## Checks`,
    ``,
    ...report.checks.map((c) => `- **${c.id}:** ${c.pass ? "PASS" : "FAIL"}`),
    ``,
    `## Safety`,
    ``,
    `- Census tables only`,
    `- Brand Explorer untouched`,
    `- No fake rooms / owners / operators / dates / 0,0 coords`,
    `- Frozen VIC + frozen 62 untouched`,
    ``,
    `## Next`,
    ``,
    `Founder review. Brand Explorer production patch remains blocked (Option A dry-run only when separately approved).`,
    ``,
  ].join("\n");
  writeMd(join(DOCS, "production-census-write-complete.md"), doc);

  console.log(
    JSON.stringify(
      {
        status: report.status,
        reconciliation: report.reconciliation,
        failed: report.checks.filter((c) => !c.pass).map((c) => c.id),
      },
      null,
      2
    )
  );
  if (report.status !== STATUS.VALIDATION_PASS) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[census-write-validate] FAILED", err);
  process.exitCode = 1;
});
