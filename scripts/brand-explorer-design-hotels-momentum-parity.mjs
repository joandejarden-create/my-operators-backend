#!/usr/bin/env node
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  V35F_R4_VERSION,
  buildDesignHotelsMomentumParityV35FR4Report,
} from "../lib/partner-intelligence/brand-explorer-design-hotels-momentum-parity-v35F-R4.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  const report = await buildDesignHotelsMomentumParityV35FR4Report();
  const jsonPath = join(ROOT, "reports", `brand-explorer-design-hotels-momentum-parity-${V35F_R4_VERSION}.json`);
  mkdirSync(dirname(jsonPath), { recursive: true });
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Wrote ${jsonPath}`);
  console.log(`Approach: ${report.approach}`);
  console.log(`Mode: ${report.mode}`);
  console.log(`Creates: ${report.rowsCreated.length}`);
  console.log(`Patches: ${report.rowsPatched.length}`);
  console.log(`Dry-run clean: ${report.dryRunClean}`);
  if (report.applyBlockers.length) {
    console.log("Apply blockers:");
    for (const b of report.applyBlockers) console.log(`  - ${b}`);
  }
  if (report.founderReviewQueue.length) {
    console.log("Founder review queue:");
    for (const item of report.founderReviewQueue) console.log(`  - ${item.slotKey}: ${item.reason}`);
  }
  if (report.exactApplyCommand) console.log(`Apply: ${report.exactApplyCommand}`);
  if (report.applyResults?.errors?.length) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
