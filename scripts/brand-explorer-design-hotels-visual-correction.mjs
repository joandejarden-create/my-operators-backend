#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35F_R3_VERSION,
  buildDesignHotelsVisualCorrectionV35FR3Report,
} from "../lib/partner-intelligence/brand-explorer-design-hotels-visual-correction-v35F-R3.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildDesignHotelsVisualCorrectionV35FR3Report();
  const jsonPath = join(ROOT, "reports", `brand-explorer-design-hotels-visual-correction-${V35F_R3_VERSION}.json`);
  mkdirSync(dirname(jsonPath), { recursive: true });
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Wrote ${jsonPath}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Creates: ${report.rowsCreated.length}`);
  console.log(`Patches: ${report.rowsPatched.length}`);
  console.log(`Dry-run clean: ${report.dryRunClean}`);
  if (report.applyBlockers.length) {
    console.log("Apply blockers:");
    for (const b of report.applyBlockers) console.log(`  - ${b}`);
  }
  if (report.exactApplyCommand) console.log(`Apply: ${report.exactApplyCommand}`);
  if (report.applyResults?.errors?.length) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
