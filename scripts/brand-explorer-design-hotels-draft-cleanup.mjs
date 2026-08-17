#!/usr/bin/env node
/**
 * Design Hotels Draft Cleanup v35E — duplicate openings, CALA labels, registry promotion.
 *
 *   npm run brand-explorer-design-hotels-draft-cleanup -- --brand design-hotels --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35E_VERSION,
  buildDesignHotelsDraftCleanupV35EReport,
} from "../lib/partner-intelligence/brand-explorer-design-hotels-draft-cleanup-v35E.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildDesignHotelsDraftCleanupV35EReport();

  const jsonPath = join(ROOT, "reports", `brand-explorer-design-hotels-draft-cleanup-${V35E_VERSION}.json`);
  const mdPath = join(ROOT, "reports", `brand-explorer-design-hotels-draft-cleanup-${V35E_VERSION}.md`);
  const docPath = join(
    ROOT,
    "docs/data-intelligence/brand-explorer-design-hotels-draft-cleanup-v35E.md"
  );

  mkdirSync(dirname(jsonPath), { recursive: true });
  mkdirSync(dirname(docPath), { recursive: true });
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(mdPath, `${report.markdown}\n`);
  writeFileSync(docPath, `${report.markdown}\n`);

  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(`Wrote ${docPath}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Openings visible before/after: ${report.openingRowsBefore.visibleCount} → ${report.openingRowsAfter.visibleCount}`);
  console.log(`Registry promotions: ${report.registryPromotions.length}`);
  console.log(`Dry-run clean: ${report.dryRunClean}`);
  console.log(`Expected founder review pass: ${report.expectedFounderReview?.pass ?? "unknown"}`);

  if (report.applyBlockers.length) {
    console.log("Apply blockers:");
    for (const b of report.applyBlockers) console.log(`  - ${b}`);
  }
  if (report.exactApplyCommand) {
    console.log("Apply command:");
    console.log(`  ${report.exactApplyCommand}`);
  }
  if (report.applyResults?.errors?.length) {
    console.error("Apply errors:");
    for (const e of report.applyResults.errors) console.error(`  ${e.recordId}: ${e.message}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
