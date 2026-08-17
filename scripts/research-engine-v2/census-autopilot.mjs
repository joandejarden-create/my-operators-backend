#!/usr/bin/env node
/**
 * Census Autopilot CLI v2
 *
 * Production:
 *   npm run census:autopilot -- --region CALA --parent-company IHG --mode apply \
 *     --run-until-complete --batch-size 100 \
 *     --confirm-safe-writes --confirm-write-to-production-census \
 *     --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
 *     --confirm-no-date-writes --confirm-no-recent-momentum \
 *     --confirm-no-company-validation --confirm-webhound-not-production-source
 *
 * Resume:
 *   npm run census:autopilot -- --resume <run-id>
 *
 * Safety: production Airtable writes require ALLOW_CENSUS_AUTOPILOT_APPLY=1 and
 * CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 plus all CLI confirms. Pass --enable-production-writes
 * explicitly when founder intends live apply.
 */

import { runCensusAutopilot, parseAutopilotArgs } from "../../lib/research-engine-v2/census-autopilot-runner.js";

const argv = process.argv.slice(2);
const args = parseAutopilotArgs(argv);
const enableProductionWrites =
  argv.includes("--enable-production-writes") && args.mode === "apply";

if (args.warnings?.length) {
  for (const w of args.warnings) console.warn(`[census:autopilot] ${w}`);
}

const result = await runCensusAutopilot(argv, {
  liveDryRun: Boolean(args.live),
  enableProductionWrites,
});

if (!result.ok && result.error) {
  console.error(JSON.stringify({ ok: false, error: result.error, status: result.status }, null, 2));
  process.exit(1);
}

console.log(
  JSON.stringify(
    {
      ok: result.ok !== false,
      status: result.status,
      run_id: result.run_id,
      run_dir: result.run_dir,
      mode: args.mode,
      parent_company: args.parentCompany || result.summary?.parent_company,
      region: args.region || result.summary?.region,
      batch_size: args.batchSize,
      max_records: args.maxRecords,
      run_until_complete: args.runUntilComplete,
      completion_status: result.summary?.completion_status || result.batch_result?.completion_status,
      airtable_writes: result.airtable_writes,
      brand_explorer_writes: false,
      summary: result.summary,
      warnings: result.warnings || args.warnings,
    },
    null,
    2
  )
);
