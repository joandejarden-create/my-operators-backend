#!/usr/bin/env node
/**
 * Node CLI for GDI universal operational workflow.
 * Invoked by scripts/gdi-workflow.ps1 — do not duplicate in PowerShell.
 *
 *   node scripts/gdi-universal-operational-workflow-v1.mjs --hotel <id> --action Validate --dry-run
 */
import "dotenv/config";
import {
  runGdiUniversalOperationalWorkflow,
  WORKFLOW_ACTIONS,
} from "../lib/group-demand-intelligence/workflow/gdi-universal-operational-workflow-v1.js";

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

async function main() {
  const action = argValue("--action") || "Validate";
  if (!WORKFLOW_ACTIONS.includes(action)) {
    console.error(
      JSON.stringify({
        ok: false,
        error: "UNKNOWN_ACTION",
        allowed: WORKFLOW_ACTIONS,
      }),
    );
    process.exit(2);
  }

  const result = await runGdiUniversalOperationalWorkflow({
    hotelId: argValue("--hotel") || argValue("--hotel-id"),
    action,
    dryRun: !hasFlag("--apply"),
    environment: argValue("--environment") || "Local",
    resumeRun: argValue("--resume-run"),
    approvalReference: argValue("--approval-reference"),
    shareToken: argValue("--share-token"),
    prodBase: argValue("--prod-base"),
    reportPath: argValue("--report-path"),
    runId: argValue("--run-id"),
    executeResearch: hasFlag("--execute-research"),
  });

  console.log(
    JSON.stringify(
      {
        ok: result.ok,
        status: result.status,
        paused: result.paused === true,
        reportFile: result.reportFile,
        summary: {
          hotelId: result.report?.hotelId,
          action: result.report?.action,
          dryRun: result.report?.dryRun,
          researchStatus: result.report?.researchStatus,
          opportunitiesDiscovered: result.report?.opportunitiesDiscovered,
          airtableCreated: result.report?.airtableCreated,
          airtableUpdated: result.report?.airtableUpdated,
          airtableFailed: result.report?.airtableFailed,
          approvalStatus: result.report?.approvalStatus,
          publicationVersion: result.report?.publicationVersion,
          publishedOpportunityCount: result.report?.publishedOpportunityCount,
          fullContentParityResult: result.report?.fullContentParityResult,
          errorCount: result.report?.errors?.length || 0,
          nextActions: result.report?.nextActions || [],
        },
      },
      null,
      2,
    ),
  );

  if (!result.ok) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
