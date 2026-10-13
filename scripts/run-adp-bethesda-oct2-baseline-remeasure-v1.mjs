#!/usr/bin/env node
/**
 * Bethesda Marriott Oct 2 2026 official ADP baseline remasurement.
 *
 * Usage:
 *   node scripts/run-adp-bethesda-oct2-baseline-remeasure-v1.mjs --preflight-only
 *   node scripts/run-adp-bethesda-oct2-baseline-remeasure-v1.mjs --dry-run
 *   node scripts/run-adp-bethesda-oct2-baseline-remeasure-v1.mjs --apply
 *
 * Founder decision: Oct 2 is the official pilot baseline measurement date.
 * Cost cap: $12. Locks Sept 63-query control set.
 */

import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  buildBethesdaOct2RemeasurePreflight,
  executeBethesdaOct2OfficialBaseline,
  freezeBethesdaControlQuerySetDoc,
  BETHESDA_OCT2_BASELINE_ID,
} from "../lib/ai-demand-positioning/execution/bethesda-marriott-oct2-baseline-remeasure-v1.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { countActiveAdpAttributes } from "../lib/hotel-intelligence/onboarding/adp-attribute-counts.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const preflightOnly = args.includes("--preflight-only");
const dryRun = !apply;

const HPC = "recLuxvwwxID7U2B8";
const OUT = join(process.cwd(), "reports/bethesda-pilot/adp-baseline/2026-10-02");

async function runHiPreflight() {
  const hi = await isHotelIntelligenceComplete(HPC, {});
  const attrs = await countActiveAdpAttributes(HPC);
  return {
    HI_COMPLETE: hi?.complete === true || hi?.HI_COMPLETE === true || hi === true,
    hiDetail: typeof hi === "object" ? hi : { value: hi },
    activeAdpAttributes: attrs?.count ?? attrs?.active ?? attrs,
    attrsDetail: attrs,
  };
}

async function main() {
  console.log("\n=== BETHESDA OCT 2 ADP OFFICIAL BASELINE REMEASURE ===\n");
  console.log(
    apply
      ? "MODE: LIVE APPLY"
      : dryRun && !preflightOnly
        ? "MODE: DRY RUN"
        : "MODE: PREFLIGHT"
  );
  console.log("OFFICIAL_BASELINE_ID:", BETHESDA_OCT2_BASELINE_ID);

  mkdirSync(OUT, { recursive: true });
  const controlDoc = freezeBethesdaControlQuerySetDoc();
  writeFileSync(
    join(OUT, "CONTROL_QUERY_SET.json"),
    JSON.stringify(controlDoc, null, 2) + "\n"
  );

  const pf = buildBethesdaOct2RemeasurePreflight();
  writeFileSync(join(OUT, "PREFLIGHT.json"), JSON.stringify(pf, null, 2) + "\n");

  console.log("PREFLIGHT:", pf.PREFLIGHT);
  console.log("QUERY_COUNT:", pf.QUERY_COUNT);
  console.log("DEMAND_FAMILIES:", JSON.stringify(pf.DEMAND_FAMILIES));
  console.log("METHODOLOGY_VERSION:", pf.METHODOLOGY_VERSION);
  console.log("EST_COST:", pf.TOTAL_ESTIMATED_COST);
  if (pf.blockers?.length) console.error("blockers:", pf.blockers);

  let hiGate = null;
  try {
    hiGate = await runHiPreflight();
    writeFileSync(
      join(OUT, "HI_ATTR_PREFLIGHT.json"),
      JSON.stringify(hiGate, null, 2) + "\n"
    );
    console.log(
      "HI_COMPLETE:",
      hiGate.HI_COMPLETE,
      "| ADP attrs:",
      hiGate.activeAdpAttributes
    );
  } catch (err) {
    console.warn("HI/attr preflight warning:", err?.message || err);
    hiGate = { error: String(err?.message || err) };
    writeFileSync(
      join(OUT, "HI_ATTR_PREFLIGHT.json"),
      JSON.stringify(hiGate, null, 2) + "\n"
    );
  }

  if (pf.PREFLIGHT !== "PASS") {
    console.error("\nBASELINE_ABORTED_PRE_RUN_GATE");
    process.exit(2);
  }

  if (preflightOnly) {
    console.log("\n--preflight-only: stopping before provider calls.");
    process.exit(0);
  }

  console.log(`\n=== EXECUTING (${dryRun ? "DRY RUN" : "LIVE"}) ===`);
  const result = await executeBethesdaOct2OfficialBaseline({
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
  writeFileSync(
    join(OUT, dryRun ? "OCT2_DRY_RUN.json" : "OCT2_LIVE_RUN.json"),
    JSON.stringify(result, null, 2) + "\n"
  );

  if (!result.ok) process.exit(3);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
