#!/usr/bin/env node
/**
 * Certify ADP Official Baseline Period 001 after calculation/source/Playwright gates pass.
 *
 * Usage:
 *   node scripts/certify-adp-official-baseline-period-001.mjs --from-run-report
 *   node scripts/certify-adp-official-baseline-period-001.mjs --period adp_waterstone_boca_raton=adp_period_...
 */

import "../load-env.js";
import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { certifyOfficialBaselinePeriods, loadFrozenContractHash } from "../lib/ai-demand-positioning/execution/official-baseline-period-001-v1.js";
import { OFFICIAL_BASELINE_PERIOD_MARKER } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const args = process.argv.slice(2);
const fromRun = args.includes("--from-run-report");
const map = {};

if (fromRun) {
  const runPath = join(
    process.cwd(),
    "reports/ai-demand-positioning/adp-official-baseline-period-001-run.json"
  );
  if (!existsSync(runPath)) {
    console.error("Run report missing:", runPath);
    process.exit(1);
  }
  const run = JSON.parse(readFileSync(runPath, "utf8"));
  if (run.status === "BASELINE_RUN_PARTIAL_REMEDIATION_REQUIRED") {
    console.error("Refusing to certify incomplete baseline run");
    process.exit(2);
  }
  for (const row of run.propertyResults || []) {
    map[row.propertyId] = row.PERIOD_ID;
  }
}

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--period" && args[i + 1]) {
    const [propertyId, periodId] = args[i + 1].split("=");
    map[propertyId] = periodId;
  }
}

if (!Object.keys(map).length) {
  console.error("No periods to certify. Use --from-run-report or --period propertyId=periodId");
  process.exit(1);
}

const hash = loadFrozenContractHash();
const results = await certifyOfficialBaselinePeriods(map, { measurementContractHash: hash });
const out = {
  certifiedAt: new Date().toISOString(),
  baselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
  measurementContractHash: hash,
  results,
  ADP_OFFICIAL_BASELINE_PERIOD_001_CERTIFIED: results.every((r) => r.ok) ? "YES" : "NO",
};
const outPath = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-official-baseline-period-001-certification.json"
);
mkdirSync(dirname(outPath), { recursive: true });
writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify(out, null, 2));
process.exit(results.every((r) => r.ok) ? 0 : 1);
