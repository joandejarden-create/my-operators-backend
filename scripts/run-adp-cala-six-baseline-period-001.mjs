#!/usr/bin/env node
/**
 * CALA six-hotel ADP first-official baselines.
 *
 * Usage:
 *   node scripts/run-adp-cala-six-baseline-period-001.mjs --preflight-only
 *   node scripts/run-adp-cala-six-baseline-period-001.mjs --dry-run
 *   node scripts/run-adp-cala-six-baseline-period-001.mjs --apply
 *   node scripts/run-adp-cala-six-baseline-period-001.mjs --apply --property adp_st_regis_mexico_city
 *
 * Founder cost approval required before --apply (est. ~$8–12/hotel, ~$50–72 cohort).
 */

import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  CALA_SIX_PROPERTY_IDS,
  CALA_SIX_COST_CAP_USD,
  buildCalaSixPropertyPreflight,
  executeCalaSixBaselinePeriod001,
} from "../lib/ai-demand-positioning/execution/cala-six-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const preflightOnly = args.includes("--preflight-only");
const dryRun = !apply;
const onlyIdx = args.indexOf("--property");
const only =
  onlyIdx >= 0 && args[onlyIdx + 1]
    ? args[onlyIdx + 1]
    : args.find((a) => a.startsWith("--property="))?.slice("--property=".length) || null;

async function main() {
  console.log("\n=== ADP CALA SIX BASELINE PERIOD 001 ===\n");
  console.log(
    apply
      ? "MODE: LIVE APPLY (founder cost approved)"
      : dryRun && !preflightOnly
        ? "MODE: DRY RUN"
        : "MODE: PREFLIGHT"
  );

  const targets = only ? [only] : [...CALA_SIX_PROPERTY_IDS];
  for (const id of targets) {
    if (!CALA_SIX_PROPERTY_IDS.includes(id)) {
      console.error("Unknown property:", id);
      process.exit(1);
    }
  }

  const preflights = targets.map((propertyId) => buildCalaSixPropertyPreflight(propertyId));
  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "adp-cala-six-baseline-period-001-preflight.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), rows: preflights }, null, 2) + "\n"
  );

  for (const pf of preflights) {
    console.log(`\n--- ${pf.propertyId} ---`);
    console.log(`PREFLIGHT: ${pf.PREFLIGHT}`);
    console.log(
      `SCENARIOS: ${pf.TOTAL_SCENARIOS} | CALLS: ${pf.TOTAL_PLANNED_CALLS} | EST: $${pf.TOTAL_ESTIMATED_COST}`
    );
    if (pf.blockers?.length) console.error("blockers:", pf.blockers);
  }

  const anyFail = preflights.some((p) => p.PREFLIGHT !== "PASS");
  if (anyFail) {
    console.error("\nBASELINE_ABORTED_PRE_RUN_GATE");
    process.exit(2);
  }

  const totalEst = Math.round(preflights.reduce((n, p) => n + p.TOTAL_ESTIMATED_COST, 0) * 100) / 100;
  if (totalEst > CALA_SIX_COST_CAP_USD * targets.length) {
    console.error(`\nCOST > $${CALA_SIX_COST_CAP_USD * targets.length} — founder approval required`);
    process.exit(2);
  }

  if (preflightOnly) {
    console.log("\n--preflight-only: stopping before provider calls.");
    console.log("TOTAL_ESTIMATED_COST:", totalEst);
    process.exit(0);
  }

  const results = [];
  for (const propertyId of targets) {
    console.log(`\n=== EXECUTING ${propertyId} (${dryRun ? "DRY RUN" : "LIVE"}) ===`);
    const result = await executeCalaSixBaselinePeriod001({
      propertyId,
      dryRun,
      certify: false,
      onProgress: ({ completed, total }) => {
        if (completed % 20 === 0 || completed === total) {
          process.stdout.write(`\r  [${propertyId}] ${completed}/${total}`);
        }
      },
    });
    console.log("\n");
    console.log(
      JSON.stringify(
        {
          propertyId,
          status: result.status,
          PERIOD_ID: result.PERIOD_ID,
          CALLS_ATTEMPTED: result.CALLS_ATTEMPTED,
          CALLS_SUCCESSFUL: result.CALLS_SUCCESSFUL,
          CALLS_FAILED: result.CALLS_FAILED,
          ACTUAL_SPEND: result.ACTUAL_SPEND,
          incompleteProviders: result.incompleteProviders,
          published: result.published?.ok || false,
        },
        null,
        2
      )
    );
    results.push(result);
    if (!result.ok && !dryRun) {
      writeFileSync(
        join(outDir, "adp-cala-six-baseline-period-001-batch-run.json"),
        JSON.stringify({ ok: false, results }, null, 2) + "\n"
      );
      process.exit(1);
    }
  }

  const batch = {
    ok: results.every((r) => r.ok),
    mode: dryRun ? "DRY_RUN" : "LIVE_APPLY",
    founderCostApproved: apply === true,
    TOTAL_ESTIMATED_COST: totalEst,
    results,
  };
  writeFileSync(
    join(outDir, "adp-cala-six-baseline-period-001-batch-run.json"),
    JSON.stringify(batch, null, 2) + "\n"
  );
  console.log("\n=== BATCH COMPLETE ===");
  console.log(
    JSON.stringify(
      { ok: batch.ok, mode: batch.mode, properties: results.map((r) => r.propertyId) },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
