#!/usr/bin/env node
/**
 * Mallorca Son Vida dual ADP baselines (Castillo + Sheraton) — shared executor path.
 *
 *   node scripts/run-adp-mallorca-son-vida-dual-baseline-period-001.mjs --hotel castillo --preflight-only
 *   node scripts/run-adp-mallorca-son-vida-dual-baseline-period-001.mjs --hotel sheraton --preflight-only
 *   node scripts/run-adp-mallorca-son-vida-dual-baseline-period-001.mjs --hotel both --dry-run
 *   node scripts/run-adp-mallorca-son-vida-dual-baseline-period-001.mjs --hotel castillo --apply --certify
 *   node scripts/run-adp-mallorca-son-vida-dual-baseline-period-001.mjs --hotel sheraton --apply --certify
 */

import "dotenv/config";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  buildCastilloSonVidaPreflight,
  buildSheratonMallorcaArabellaPreflight,
  executeCastilloSonVidaBaselinePeriod001,
  executeSheratonMallorcaArabellaBaselinePeriod001,
} from "../lib/ai-demand-positioning/execution/mallorca-son-vida-dual-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const hotelArg = (() => {
  const i = args.indexOf("--hotel");
  return i >= 0 ? String(args[i + 1] || "").toLowerCase() : "both";
})();
const preflightOnly = args.includes("--preflight-only");
const dryRun = args.includes("--dry-run") || (!args.includes("--apply") && !preflightOnly);
const apply = args.includes("--apply");
const certify = args.includes("--certify");

const outDir = join(process.cwd(), "reports/ai-demand-positioning");
mkdirSync(outDir, { recursive: true });

const hotels =
  hotelArg === "castillo"
    ? ["castillo"]
    : hotelArg === "sheraton"
      ? ["sheraton"]
      : ["castillo", "sheraton"];

let exitCode = 0;

for (const hotel of hotels) {
  const preflight =
    hotel === "castillo"
      ? buildCastilloSonVidaPreflight()
      : buildSheratonMallorcaArabellaPreflight();
  const preflightName =
    hotel === "castillo"
      ? "adp-castillo-hotel-son-vida-baseline-preflight.json"
      : "adp-sheraton-mallorca-arabella-golf-baseline-preflight.json";
  writeFileSync(join(outDir, preflightName), JSON.stringify(preflight, null, 2));
  console.log(`\n=== ${hotel.toUpperCase()} PREFLIGHT ===`);
  console.log(JSON.stringify(preflight, null, 2));

  if (preflightOnly) {
    if (preflight.PREFLIGHT !== "PASS") exitCode = 2;
    continue;
  }

  if (!apply && !dryRun) {
    console.error("Specify --dry-run or --apply");
    process.exit(1);
  }

  const label =
    hotel === "castillo"
      ? "Castillo Hotel Son Vida"
      : "Sheraton Mallorca Arabella Golf";
  const report =
    hotel === "castillo"
      ? await executeCastilloSonVidaBaselinePeriod001({
          dryRun: !apply,
          certify: apply && certify,
          onProgress: ({ completed, total }) => {
            process.stdout.write(`\r  ${label} ${completed}/${total}   `);
          },
        })
      : await executeSheratonMallorcaArabellaBaselinePeriod001({
          dryRun: !apply,
          certify: apply && certify,
          onProgress: ({ completed, total }) => {
            process.stdout.write(`\r  ${label} ${completed}/${total}   `);
          },
        });
  console.log("\n");
  console.log(JSON.stringify(report, null, 2));
  if (!report.ok) exitCode = 1;
}

process.exit(exitCode);
