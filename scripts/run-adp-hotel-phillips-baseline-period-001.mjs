#!/usr/bin/env node
/**
 * Hotel Phillips Kansas City — first official ADP property baseline.
 *
 * Usage:
 *   node scripts/run-adp-hotel-phillips-baseline-period-001.mjs --preflight-only
 *   node scripts/run-adp-hotel-phillips-baseline-period-001.mjs --dry-run
 *   node scripts/run-adp-hotel-phillips-baseline-period-001.mjs --apply
 */

import "../load-env.js";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  buildHotelPhillipsPreflight,
  executeHotelPhillipsBaselinePeriod001,
  HOTEL_PHILLIPS_COST_CAP_USD,
} from "../lib/ai-demand-positioning/execution/hotel-phillips-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const preflightOnly = args.includes("--preflight-only");
const dryRun = !apply;

async function main() {
  console.log("\n=== ADP HOTEL PHILLIPS BASELINE PERIOD 001 ===\n");

  const preflight = buildHotelPhillipsPreflight();
  console.log(JSON.stringify(preflight, null, 2));

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "adp-hotel-phillips-baseline-preflight.json"),
    JSON.stringify(preflight, null, 2)
  );

  if (preflight.PREFLIGHT !== "PASS") {
    console.error("\nBASELINE_ABORTED_PRE_RUN_GATE");
    console.error("blockers:", preflight.blockers);
    process.exit(2);
  }

  if (preflight.TOTAL_ESTIMATED_COST > HOTEL_PHILLIPS_COST_CAP_USD) {
    console.error(`\nCOST > $${HOTEL_PHILLIPS_COST_CAP_USD} — founder approval required`);
    process.exit(2);
  }

  if (preflightOnly) {
    console.log("\n--preflight-only: stopping before provider calls.");
    console.log("PREFLIGHT: PASS");
    console.log("PLANNED_PROVIDER_CALLS:", preflight.TOTAL_PLANNED_CALLS);
    console.log("ESTIMATED_COST: $", preflight.TOTAL_ESTIMATED_COST);
    process.exit(0);
  }

  console.log(dryRun ? "\nRunning DRY RUN..." : "\nRunning LIVE four-provider measurement...");
  const result = await executeHotelPhillipsBaselinePeriod001({
    dryRun,
    certify: false,
    onProgress: ({ completed, total }) => {
      if (completed % 20 === 0 || completed === total) {
        process.stdout.write(`\r  progress ${completed}/${total}`);
      }
    },
  });
  console.log("\n");
  console.log(JSON.stringify(result, null, 2));

  if (!result.ok && !dryRun) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
