#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DOC_MD_NAME,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerSlotCompletionReviewPackageMarkdown,
  buildBrandExplorerSlotCompletionReviewPackageReport,
} from "../lib/partner-intelligence/brand-explorer-slot-completion-review-package.js";

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const raw = argv[i];
    if (!raw.startsWith("--")) continue;
    const key = raw.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      i += 1;
    }
  }
  return out;
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const DOC_MD = join(ROOT, "docs", "data-intelligence", DOC_MD_NAME);

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const brandIdOrName = args.brand || args["brand-id"] || args.brandId || undefined;
  const report = await buildBrandExplorerSlotCompletionReviewPackageReport({ brandIdOrName });
  const markdown = buildBrandExplorerSlotCompletionReviewPackageMarkdown(report);
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  mkdirSync(dirname(DOC_MD), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, markdown, "utf8");
  writeFileSync(DOC_MD, markdown, "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${DOC_MD}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`v20A selected slots: ${report.slotsSelectedCount}`);
  console.log(`Projected score if applied: ${report.projectedScoreIfFirstWaveApplied}`);
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
