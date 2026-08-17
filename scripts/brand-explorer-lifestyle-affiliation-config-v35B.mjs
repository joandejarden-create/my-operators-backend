#!/usr/bin/env node
/**
 * Brand Explorer Lifestyle / Affiliation Config v35B.
 *
 *   npm run brand-explorer-lifestyle-affiliation-config-v35B
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35B_PACK_VERSION,
  buildLifestyleAffiliationConfigV35BMarkdown,
  buildLifestyleAffiliationConfigV35BReport,
} from "../lib/partner-intelligence/brand-explorer-lifestyle-affiliation-config-v35B.js";
import { buildSourceCapturePlanMarkdown } from "../lib/partner-intelligence/brand-explorer-lifestyle-affiliation-source-capture.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildLifestyleAffiliationConfigV35BReport();
  const markdown = buildLifestyleAffiliationConfigV35BMarkdown(report);

  const jsonPath = join(ROOT, "reports", "brand-explorer-lifestyle-affiliation-config-v35B.json");
  const mdPath = join(ROOT, "reports", "brand-explorer-lifestyle-affiliation-config-v35B.md");
  const docPath = join(ROOT, "docs/data-intelligence/brand-explorer-lifestyle-affiliation-config-v35B.md");

  mkdirSync(dirname(jsonPath), { recursive: true });
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(mdPath, `${markdown}\n`);
  writeFileSync(docPath, `${markdown}\n`);

  const sourceCaptureReports = [
    { slug: "design-hotels", file: "brand-explorer-source-capture-plan-design-hotels-v35B.md" },
    { slug: "small-luxury-hotels-of-the-world", file: "brand-explorer-source-capture-plan-small-luxury-hotels-v35B.md" },
    { slug: "tribute-portfolio", file: "brand-explorer-source-capture-plan-tribute-portfolio-benchmark-v35B.md" },
  ];

  for (const { slug, file } of sourceCaptureReports) {
    const planMd = buildSourceCapturePlanMarkdown(report.sourceCapturePlans[slug]);
    const planPath = join(ROOT, "reports", file);
    writeFileSync(planPath, `${planMd}\n`);
    console.log(`Wrote ${planPath}`);
  }

  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(`Wrote ${docPath}`);
  console.log(`Lifestyle affiliation config pack ${V35B_PACK_VERSION}`);
  console.log(`Brands registered: ${report.summary.brandsRegistered}/${report.summary.brandsTotal}`);
  console.log(`All configs pass: ${report.summary.allConfigsPass}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
