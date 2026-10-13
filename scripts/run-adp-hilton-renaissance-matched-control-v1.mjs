#!/usr/bin/env node
/**
 * Hilton × Renaissance matched-control ADP run.
 *
 *   node scripts/run-adp-hilton-renaissance-matched-control-v1.mjs --preflight-only
 *   node scripts/run-adp-hilton-renaissance-matched-control-v1.mjs --dry-run
 *   node scripts/run-adp-hilton-renaissance-matched-control-v1.mjs --apply --certify
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  buildMatchedControlPreflight,
  executeMatchedControlPair,
  MATCHED_COST_CAP_USD,
} from "../lib/ai-demand-positioning/execution/hilton-renaissance-matched-control-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const certify = args.includes("--certify");
const preflightOnly = args.includes("--preflight-only");
const dryRun = !apply;

async function main() {
  console.log("\n=== ADP Hilton × Renaissance MATCHED CONTROL V1 ===\n");
  const preflight = buildMatchedControlPreflight();
  console.log(JSON.stringify(preflight, null, 2));

  const outDir = join(process.cwd(), "reports/adp/hilton-renaissance-matched-control");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(join(outDir, "PREFLIGHT.json"), JSON.stringify(preflight, null, 2));

  if (preflight.PREFLIGHT !== "PASS") {
    console.error("\nABORTED_PREFLIGHT");
    process.exit(2);
  }
  if (preflight.TOTAL_ESTIMATED_COST > MATCHED_COST_CAP_USD) {
    console.error(`\nCOST > $${MATCHED_COST_CAP_USD}`);
    process.exit(2);
  }
  if (preflightOnly) {
    console.log("\n--preflight-only: stopping.");
    process.exit(0);
  }

  console.log(dryRun ? "\nDRY RUN..." : "\nLIVE matched measurement (Hilton then Renaissance)...");
  const result = await executeMatchedControlPair({
    dryRun,
    certify: certify && !dryRun,
    onProgress: ({ propertyId, completed, total }) => {
      if (completed % 10 === 0 || completed === total) {
        process.stdout.write(`\r  ${propertyId} ${completed}/${total}   `);
      }
    },
  });
  console.log("\n");
  const slim = {
    ok: result.ok,
    status: result.status,
    pairId: result.pairId,
    FORMALLY_COMPARABLE: result.FORMALLY_COMPARABLE,
    hilton: result.hilton,
    renaissance: result.renaissance,
  };
  console.log(JSON.stringify(slim, null, 2));
  writeFileSync(join(outDir, "RUN_RESULT.json"), JSON.stringify(slim, null, 2));
  if (!result.ok && !dryRun) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
