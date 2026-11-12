#!/usr/bin/env node
/**
 * Bethesda 88 commercial surface reconciliation.
 *
 * Dry-run:
 *   node scripts/gdi-bethesda-88-commercial-surface-reconciliation-v1.mjs
 *
 * Apply:
 *   node scripts/gdi-bethesda-88-commercial-surface-reconciliation-v1.mjs \
 *     --apply --founder-authorized \
 *     --approval-token FOUNDER_APPROVE_BETHESDA_COMMERCIAL_SURFACE_RECON_20261112
 */
import {
  runBethesda88CommercialSurfaceReconciliation,
  BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
  SURFACE_RECON_APPROVAL_TOKEN,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-surface-reconciliation-v1.js";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const founderAuthorization = args.includes("--founder-authorized");
const tokenIdx = args.indexOf("--approval-token");
const approvalToken =
  tokenIdx >= 0
    ? args[tokenIdx + 1]
    : apply && founderAuthorization
      ? SURFACE_RECON_APPROVAL_TOKEN
      : null;

const report = runBethesda88CommercialSurfaceReconciliation({
  apply,
  founderAuthorization,
  approvalToken,
});

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
      ok: report.ok === true,
      beforeListCount: report.beforeListCount,
      afterCommercialCount: report.afterCommercialCount,
      removedCount: report.removedCount,
      answers: report.answers || null,
      acceptance: report.acceptance || null,
      writeApplied: report.writeApplied === true,
      storeShaBefore: report.storeShaBefore ?? null,
      storeShaAfter: report.storeShaAfter ?? null,
      approvalTokenRequired: SURFACE_RECON_APPROVAL_TOKEN,
      reportDir: "reports/gdi-bethesda-88-commercial-surface-reconciliation-v1",
      code: report.code || null,
    },
    null,
    2,
  ),
);

if (report.ok !== true) process.exit(1);
