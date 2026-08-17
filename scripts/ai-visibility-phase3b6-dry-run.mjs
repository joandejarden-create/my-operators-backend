#!/usr/bin/env node
/**
 * Phase 3B.6 — Recurring monitoring dry-run / manual execution CLI.
 * DEFAULT: NO LIVE PROVIDER CALLS.
 *
 * Usage:
 *   node scripts/ai-visibility-phase3b6-dry-run.mjs --dry-run
 *   node scripts/ai-visibility-phase3b6-dry-run.mjs --create-recurring-period
 *   node scripts/ai-visibility-phase3b6-dry-run.mjs --resume --period-id=<id>
 *   node scripts/ai-visibility-phase3b6-dry-run.mjs --execute  (BLOCKED — not permitted in 3B.6)
 */
import { executePhase3b6 } from "../lib/ai-visibility/phase3b6-orchestrator.js";
import {
  dryRunRecurringPeriod,
  createRecurringPeriod,
  buildPeriodResumeState,
  MANUAL_EXECUTION_COMMANDS,
} from "../lib/ai-visibility/recurring-period-orchestrator.js";

const args = process.argv.slice(2);
const flags = new Set(args.filter((a) => a.startsWith("--")));
const periodIdArg = args.find((a) => a.startsWith("--period-id="));
const periodId = periodIdArg ? periodIdArg.split("=")[1] : null;

async function main() {
  if (flags.has("--execute")) {
    console.error("\nBLOCKED: --execute is not permitted in Phase 3B.6. Use --dry-run only.\n");
    process.exit(1);
  }

  if (flags.has("--create-recurring-period")) {
    const result = createRecurringPeriod({ periodId, periodNumber: 2 });
    console.log(JSON.stringify(result, null, 2));
    process.exit(result.ok ? 0 : 1);
  }

  if (flags.has("--resume")) {
    if (!periodId) {
      console.error("\n--resume requires --period-id=<periodId>\n");
      process.exit(1);
    }
    const state = buildPeriodResumeState(periodId);
    console.log(JSON.stringify(state, null, 2));
    process.exit(state.ok ? 0 : 1);
  }

  if (flags.has("--dry-run") || flags.size === 0) {
    const report = await executePhase3b6({
      dryRun: true,
      periodId: periodId || undefined,
      createPeriod: false,
    });
    console.log("\n=== Phase 3B.6 Dry Run ===\n");
    console.log(`BUILD_STATUS: ${report.BUILD_STATUS}`);
    console.log(`PERIOD_2_DRY_RUN_VALID: ${report.PERIOD_2_DRY_RUN?.VALID}`);
    console.log(`REQUESTS_BUILDABLE: ${report.PERIOD_2_DRY_RUN?.TOTAL}`);
    console.log(`BASELINE_FREEZE_VALID: ${report.BASELINE_FREEZE?.VALID}`);
    console.log(`SCHEDULER_ENABLED: ${report.SCHEDULER?.ENABLED}`);
    console.log(`LIVE_CALLS: 0`);
    console.log(`Report: ${report.reportPath}`);
    console.log("\nManual commands (DO NOT RUN execute):");
    console.log(`  ${MANUAL_EXECUTION_COMMANDS.CREATE}`);
    console.log(`  ${MANUAL_EXECUTION_COMMANDS.DRY_RUN}`);
    console.log(`  ${MANUAL_EXECUTION_COMMANDS.RESUME}`);
    process.exit(report.BUILD_STATUS.includes("PASS") ? 0 : 1);
  }

  console.error("\nUnknown flags. Use --dry-run, --create-recurring-period, or --resume\n");
  process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
