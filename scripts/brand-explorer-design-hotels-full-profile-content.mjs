#!/usr/bin/env node
/**
 * Design Hotels Full Profile Content Population v35F.
 *
 *   npm run brand-explorer-design-hotels-full-profile-content -- --brand design-hotels --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35F_VERSION,
  buildDesignHotelsFullProfileContentV35FReport,
} from "../lib/partner-intelligence/brand-explorer-design-hotels-full-profile-content-v35F.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildDesignHotelsFullProfileContentV35FReport();

  const jsonPath = join(ROOT, "reports", `brand-explorer-design-hotels-full-profile-content-${V35F_VERSION}.json`);
  const mdPath = join(ROOT, "reports", `brand-explorer-design-hotels-full-profile-content-${V35F_VERSION}.md`);
  const docPath = join(
    ROOT,
    "docs/data-intelligence/brand-explorer-design-hotels-full-profile-content-v35F.md"
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
  console.log(`Packages planned: ${report.packagesPlanned}`);
  console.log(`Creates: ${report.rowsCreated.length} · Patches: ${report.rowsPatched.length}`);
  console.log(`Approved sources: ${report.approvedSourceCount}`);
  console.log(`Dry-run clean: ${report.dryRunClean}`);
  console.log(`Proof label after: ${report.proofSectionLabelAfter}`);

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
    for (const e of report.applyResults.errors) console.error(`  ${e.recordKey || e.recordId || e.slotKey}: ${e.message}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
