#!/usr/bin/env node
/**
 * Brand Explorer Lifestyle / Independent Collection Priority Pack v35.
 *
 *   node scripts/brand-explorer-lifestyle-collection-priority-pack.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  PRIORITY_PACK_VERSION,
  buildLifestyleCollectionPriorityPackMarkdown,
  buildLifestyleCollectionPriorityPackReport,
} from "../lib/partner-intelligence/brand-explorer-lifestyle-collection-priority-pack.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildLifestyleCollectionPriorityPackReport();
  const markdown = buildLifestyleCollectionPriorityPackMarkdown(report);

  const jsonPath = join(ROOT, "reports", "brand-explorer-lifestyle-collection-priority-pack.json");
  const mdPath = join(ROOT, "reports", "brand-explorer-lifestyle-collection-priority-pack.md");
  const docPath = join(
    ROOT,
    "docs/data-intelligence/brand-explorer-lifestyle-collection-priority-pack-v35.md"
  );

  mkdirSync(dirname(jsonPath), { recursive: true });
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(mdPath, `${markdown}\n`);
  writeFileSync(docPath, `${markdown}\n`);

  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(`Wrote ${docPath}`);
  console.log(`Priority pack ${PRIORITY_PACK_VERSION}`);
  console.log(`Brands audited: ${report.brands.filter((b) => b.found).length}/${report.brands.length}`);
  console.log(
    `Top 2: ${report.recommendedActivationSequence.firstTwoForLiveOwnerProject.join(", ")}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
