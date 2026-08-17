/**
 * Read-only founder review of VIC → BE medium sandbox pilot.
 *
 *   npm run research-engine-v2:mexico-vic-be-medium-sandbox-founder-review
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  runMediumSandboxFounderReview,
  renderMediumFounderReviewMarkdown,
  STATUS,
} from "../lib/research-engine-v2/mexico-vic-be-medium-sandbox-founder-review.js";

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
  console.log("[medium-founder-review] read-only review of vic.pilot.medium.*");
  const report = await runMediumSandboxFounderReview();
  const md = renderMediumFounderReviewMarkdown(report);

  writeJson(join(REPORTS, "mexico-vic-be-medium-sandbox-founder-review.json"), report);
  writeMd(join(REPORTS, "mexico-vic-be-medium-sandbox-founder-review.md"), md);
  writeMd(
    join(DOCS, "mexico-vic-be-medium-sandbox-founder-review.md"),
    `${md}\n\n## Scope\n\nRead-only founder packet. No sandbox or production writes.\n`
  );

  console.log(`[medium-founder-review] status=${report.status}`);
  console.log(`[medium-founder-review] lane=${report.lane_decision}`);
  console.log(
    `[medium-founder-review] rows=${report.row_inventory?.found}/${report.row_inventory?.expected} ok=${report.row_inventory?.row_count_ok} small_preserved=${report.small_pilot_preservation?.preserved}`
  );

  if (report.status === STATUS.HOLD || report.status === STATUS.ROW_MISMATCH) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error("[medium-founder-review] FATAL", err);
  process.exit(1);
});
