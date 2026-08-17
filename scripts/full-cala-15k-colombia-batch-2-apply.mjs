#!/usr/bin/env node
/**
 * Colombia Batch 2 allowlist-bound shell apply (Hotel Property Census only).
 *
 * Dry-run revalidation:
 *   node scripts/full-cala-15k-colombia-batch-2-apply.mjs
 *
 * Production apply:
 *   ALLOW_CENSUS_AUTOPILOT_APPLY=1 ... --enable-production-writes
 */
import "dotenv/config";
import { runFullCala15kColombiaBatch2ApplyV1 } from "../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js";

const argv = process.argv.slice(2);
const enableProductionWrites =
  argv.includes("--enable-production-writes") &&
  String(process.env.ALLOW_CENSUS_AUTOPILOT_APPLY || "0") === "1" &&
  String(process.env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "0") === "1" &&
  String(process.env.CONFIRM_NO_BRAND_SETUP_WRITES || "0") === "1" &&
  String(process.env.ENABLE_FULL_CALA_15K_CENSUS_SHELL || "0") === "1" &&
  String(process.env.ENABLE_CENSUS_SHELL_INSERTS || "0") === "1" &&
  String(process.env.ENABLE_CURRENT_BRAND_WRITES || "0") === "0" &&
  String(process.env.ENABLE_BRAND_FAMILY_WRITES || "0") === "0" &&
  String(process.env.ENABLE_ROOMS_WRITES || "0") === "0";

const report = await runFullCala15kColombiaBatch2ApplyV1({
  enableProductionWrites,
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      status: report.status,
      census_before_count: report.census_before_count,
      insert_attempted: report.insert_attempted,
      inserts_applied: report.inserts_applied,
      census_after_estimate: report.census_after_estimate,
      revalidation_skips_count: report.revalidation_skips_count,
      source_mix_attempted: report.source_mix_attempted,
      current_brand_writes: report.current_brand_writes,
      brand_family_writes: report.brand_family_writes,
      reason: report.reason || null,
      NEXT_RECOMMENDED_ACTION: report.NEXT_RECOMMENDED_ACTION,
      production_writes: report.production_writes,
      production_table_id: report.production_table_id,
    },
    null,
    2
  )
);

process.exit(report.ok ? 0 : 1);
