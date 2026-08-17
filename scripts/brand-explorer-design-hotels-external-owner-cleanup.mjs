#!/usr/bin/env node
/**
 * Design Hotels External Owner Content Cleanup v35F-R1.
 *
 *   npm run brand-explorer-design-hotels-external-owner-cleanup -- --brand design-hotels --dry-run
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35F_R1_VERSION,
  buildDesignHotelsExternalOwnerCleanupV35FR1Report,
} from "../lib/partner-intelligence/brand-explorer-design-hotels-external-owner-cleanup-v35F-R1.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildDesignHotelsExternalOwnerCleanupV35FR1Report();

  const jsonPath = join(ROOT, "reports", `brand-explorer-design-hotels-external-owner-cleanup-${V35F_R1_VERSION}.json`);
  const mdPath = join(ROOT, "reports", `brand-explorer-design-hotels-external-owner-cleanup-${V35F_R1_VERSION}.md`);
  const docPath = join(
    ROOT,
    "docs/data-intelligence/brand-explorer-design-hotels-external-owner-cleanup-v35F-R1.md"
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
  console.log(`Rows audited: ${report.rowsAudited}`);
  console.log(`Hits before: ${report.rowsWithHitsBefore}`);
  console.log(`Patches: ${report.rowsPatched.length}`);
  console.log(`Dry-run clean: ${report.dryRunClean}`);
  console.log(`External owner ready: ${report.externalOwnerReadiness?.pass ? "yes" : "no"}`);

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
    for (const e of report.applyResults.errors) {
      console.error(`  ${e.recordKey || e.recordId || e.slotKey}: ${e.message}`);
    }
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
