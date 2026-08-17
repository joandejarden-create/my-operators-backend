#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildTributeBrandExplorerContentPromotionWriterMarkdown,
  buildTributeBrandExplorerContentPromotionWriterReport,
} from "../lib/partner-intelligence/tribute-brand-explorer-content-promotion-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

function hasFlag(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx < 0) return fallback;
  return process.argv[idx + 1] || fallback;
}

async function main() {
  const apply = hasFlag("--apply");
  const applyApproved = hasFlag("--approve-tribute-brand-explorer-content-promotion");
  const allowHumanReviewCopy = hasFlag("--allow-human-review-copy");
  const report = await buildTributeBrandExplorerContentPromotionWriterReport({
    brandKey: argValue("--brand", "tribute-portfolio"),
    apply,
    applyApproved,
    allowHumanReviewCopy,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildTributeBrandExplorerContentPromotionWriterMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
