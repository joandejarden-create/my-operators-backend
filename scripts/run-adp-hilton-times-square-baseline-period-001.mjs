#!/usr/bin/env node
/**
 * Hilton Times Square — first official ADP baseline.
 *
 *   node scripts/run-adp-hilton-times-square-baseline-period-001.mjs --preflight-only
 *   node scripts/run-adp-hilton-times-square-baseline-period-001.mjs --dry-run
 *   node scripts/run-adp-hilton-times-square-baseline-period-001.mjs --apply --certify
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  buildHiltonTsPreflight,
  executeHiltonTsBaselinePeriod001,
  HILTON_TS_COST_CAP_USD,
} from "../lib/ai-demand-positioning/execution/hilton-times-square-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const certify = args.includes("--certify");
const preflightOnly = args.includes("--preflight-only");
const dryRun = !apply;

async function main() {
  console.log("\n=== ADP Hilton Times Square BASELINE PERIOD 001 ===\n");
  const preflight = buildHiltonTsPreflight();
  console.log(JSON.stringify(preflight, null, 2));

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "adp-hilton-times-square-baseline-preflight.json"),
    JSON.stringify(preflight, null, 2)
  );

  if (preflight.PREFLIGHT !== "PASS") {
    console.error("\nBASELINE_ABORTED_PRE_RUN_GATE");
    console.error("blockers:", preflight.blockers);
    process.exit(2);
  }
  if (preflight.TOTAL_ESTIMATED_COST > HILTON_TS_COST_CAP_USD) {
    console.error(`\nCOST > $${HILTON_TS_COST_CAP_USD}`);
    process.exit(2);
  }
  if (preflightOnly) {
    console.log("\n--preflight-only: stopping.");
    process.exit(0);
  }

  console.log(dryRun ? "\nDRY RUN..." : "\nLIVE measurement...");
  const result = await executeHiltonTsBaselinePeriod001({
    dryRun,
    certify: certify && !dryRun,
    onProgress: ({ completed, total }) => {
      if (completed % 10 === 0 || completed === total) {
        process.stdout.write(`\r  progress ${completed}/${total}`);
      }
    },
  });
  console.log("\n");
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok && !dryRun) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
