#!/usr/bin/env node
/**
 * Full CALA 15K — next shell batch inventory + preflight (NO Airtable writes).
 *
 *   node scripts/full-cala-15k-next-batch-preflight.mjs
 *   npm run census:full-cala-15k-next-batch-preflight
 */
import "dotenv/config";
import { runFullCala15kNextShellBatchPreflightV1 } from "../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js";

const report = await runFullCala15kNextShellBatchPreflightV1({
  maxInserts: 500,
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      status: report.status,
      census_before_count: report.census_before_count,
      recommended_next_country: report.recommended_next_country,
      recommended_batch_label: report.recommended_batch_label,
      expected_insert_count: report.batch_preflight?.expected_insert_count,
      hbx_only: report.batch_preflight?.hbx_only,
      cvent_plus_hbx: report.batch_preflight?.cvent_plus_hbx,
      cvent_only: report.batch_preflight?.cvent_only,
      proposed_current_brand_writes: report.proposed_current_brand_writes,
      proposed_brand_family_writes: report.proposed_brand_family_writes,
      NEXT_RECOMMENDED_ACTION: report.NEXT_RECOMMENDED_ACTION,
      report_slug: report.report_slug,
      colombia_batch_2_evaluation: report.colombia_batch_2_evaluation,
      mexico_hold: report.mexico_hold,
      production_writes: report.production_writes,
    },
    null,
    2
  )
);

process.exit(report.ok ? 0 : 1);
