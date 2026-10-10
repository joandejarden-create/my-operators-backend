#!/usr/bin/env node
/**
 * The Westin Grand München — first official ADP baseline.
 *
 *   node scripts/run-adp-westin-grand-munchen-baseline-period-001.mjs --preflight-only
 *   node scripts/run-adp-westin-grand-munchen-baseline-period-001.mjs --dry-run
 *   node scripts/run-adp-westin-grand-munchen-baseline-period-001.mjs --apply --certify
 */

import "dotenv/config";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  buildWestinGrandMunchenPreflight,
  executeWestinGrandMunchenBaselinePeriod001,
} from "../lib/ai-demand-positioning/execution/westin-grand-munchen-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const preflightOnly = args.includes("--preflight-only");
const dryRun = args.includes("--dry-run") || (!args.includes("--apply") && !preflightOnly);
const apply = args.includes("--apply");
const certify = args.includes("--certify");

const outDir = join(process.cwd(), "reports/ai-demand-positioning");
mkdirSync(outDir, { recursive: true });

const preflight = buildWestinGrandMunchenPreflight();
writeFileSync(join(outDir, "adp-westin-grand-munchen-baseline-preflight.json"), JSON.stringify(preflight, null, 2));
console.log(JSON.stringify(preflight, null, 2));

if (preflightOnly) {
  process.exit(preflight.PREFLIGHT === "PASS" ? 0 : 2);
}

if (!apply && !dryRun) {
  console.error("Specify --dry-run or --apply");
  process.exit(1);
}

const report = await executeWestinGrandMunchenBaselinePeriod001({
  dryRun: !apply,
  certify: apply && certify,
  onProgress: ({ completed, total }) => {
    process.stdout.write(`\r  The Westin Grand München ${completed}/${total}   `);
  },
});
console.log("\n");
console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
