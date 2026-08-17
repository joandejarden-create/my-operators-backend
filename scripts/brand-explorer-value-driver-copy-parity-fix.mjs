#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  buildBrandExplorerValueDriverCopyParityFixMarkdown,
  buildBrandExplorerValueDriverCopyParityFixReport,
} from "../lib/partner-intelligence/brand-explorer-value-driver-copy-parity-fix.js";

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
  const applyApproved = process.argv.includes("--approve-brand-explorer-value-driver-copy-fix");
  if (apply && !applyApproved) {
    console.error(
      "[brand-explorer-value-driver-copy-parity-fix] --apply requires --approve-brand-explorer-value-driver-copy-fix"
    );
    process.exit(1);
  }

  const report = await buildBrandExplorerValueDriverCopyParityFixReport({
    brandKey: argValue("--brand", "tribute-portfolio"),
    apply,
    applyApproved,
  });

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildBrandExplorerValueDriverCopyParityFixMarkdown(report), "utf8");

  console.log(`Wrote ${REPORT_MD}`);
  console.log(`Wrote ${REPORT_JSON}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
