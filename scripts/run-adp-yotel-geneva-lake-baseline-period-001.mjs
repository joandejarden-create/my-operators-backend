#!/usr/bin/env node
/**
 * YOTEL Geneva Lake — first official ADP baseline.
 *
 *   node scripts/run-adp-yotel-geneva-lake-baseline-period-001.mjs --preflight-only
 *   node scripts/run-adp-yotel-geneva-lake-baseline-period-001.mjs --dry-run
 *   node scripts/run-adp-yotel-geneva-lake-baseline-period-001.mjs --apply --certify
 */

import "dotenv/config";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  buildYotelGenevaLakePreflight,
  executeYotelGenevaLakeBaselinePeriod001,
} from "../lib/ai-demand-positioning/execution/yotel-geneva-lake-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const preflightOnly = args.includes("--preflight-only");
const dryRun = args.includes("--dry-run") || (!args.includes("--apply") && !preflightOnly);
const apply = args.includes("--apply");
const certify = args.includes("--certify");

const outDir = join(process.cwd(), "reports/ai-demand-positioning");
mkdirSync(outDir, { recursive: true });

const preflight = buildYotelGenevaLakePreflight();
writeFileSync(join(outDir, "adp-yotel-geneva-lake-baseline-preflight.json"), JSON.stringify(preflight, null, 2));
console.log(JSON.stringify(preflight, null, 2));

if (preflightOnly) {
  process.exit(preflight.PREFLIGHT === "PASS" ? 0 : 2);
}

if (!apply && !dryRun) {
  console.error("Specify --dry-run or --apply");
  process.exit(1);
}

const report = await executeYotelGenevaLakeBaselinePeriod001({
  dryRun: !apply,
  certify: apply && certify,
  onProgress: ({ completed, total }) => {
    process.stdout.write(`\r  YOTEL Geneva Lake ${completed}/${total}   `);
  },
});
console.log("\n");
console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
