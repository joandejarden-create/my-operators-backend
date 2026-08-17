#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerPresentationCopyPromotionWriterMarkdown,
  buildBrandExplorerPresentationCopyPromotionWriterReport,
} from "../lib/partner-intelligence/brand-explorer-presentation-copy-promotion-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const apply = process.argv.includes("--apply");
  const applyApproved = process.argv.includes("--approve-brand-explorer-copy-promotion");
  if (apply && !applyApproved) {
    console.error(
      "[brand-explorer-presentation-copy-promotion-writer] --apply requires --approve-brand-explorer-copy-promotion"
    );
    process.exit(1);
  }

  const report = await buildBrandExplorerPresentationCopyPromotionWriterReport({
    brandKey: argValue("--brand", "tribute-portfolio"),
    apply,
    applyApproved,
    allowNonblankCopyOverwrite: process.argv.includes("--allow-nonblank-copy-overwrite"),
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildBrandExplorerPresentationCopyPromotionWriterMarkdown(report), "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
