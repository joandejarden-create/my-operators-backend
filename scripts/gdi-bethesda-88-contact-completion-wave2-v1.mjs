#!/usr/bin/env node
/**
 * Bethesda 88 contact completion wave 2 — zero-paid named recoveries.
 *
 *   node scripts/gdi-bethesda-88-contact-completion-wave2-v1.mjs
 *   node scripts/gdi-bethesda-88-contact-completion-wave2-v1.mjs --apply-historical
 *
 * Canonical opportunities.json writes require:
 *   --apply-canonical --founder-authorized --approval-token FOUNDER_APPROVE_BETHESDA_WAVE2_CONTACT_PATCHES_20261112
 *
 * Never runs paid research.
 */
import {
  runBethesda88ContactCompletionWave2,
  BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
  WAVE2_CANONICAL_APPROVAL_TOKEN,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-completion-wave2-v1.js";

const args = process.argv.slice(2);
const applyHistorical = args.includes("--apply-historical") || !args.includes("--dry-run-only");
const applyCanonical = args.includes("--apply-canonical");
const founderAuthorization = args.includes("--founder-authorized");
const tokenIdx = args.indexOf("--approval-token");
const approvalToken = tokenIdx >= 0 ? args[tokenIdx + 1] : null;

const report = runBethesda88ContactCompletionWave2({
  applyHistorical,
  applyCanonical,
  founderAuthorization,
  approvalToken,
});

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
      ok: report.ok === true,
      beforeNamed: report.before?.raw?.named ?? null,
      afterLiveNamed: report.afterLive?.raw?.named ?? null,
      afterProjectedNamed:
        report.afterProjectedIncludingCanonicalDryRun?.raw?.named ?? null,
      uniqueMissingBefore: report.before?.uniqueMissingNamed ?? null,
      uniqueMissingAfterProjected:
        report.afterProjectedIncludingCanonicalDryRun?.uniqueMissingNamed ?? null,
      historicalWriteApplied: report.historicalPersist?.writeApplied === true,
      canonicalWriteApplied: report.canonicalPersist?.writeApplied === true,
      canonicalDryRunPatches: report.canonicalDryRunManifest?.patchCount ?? null,
      srEligible: report.engineEligibilityProjected?.salesReadyEligible ?? null,
      adEligible:
        report.engineEligibilityProjected?.accountDevelopmentEligible ?? null,
      paidAuthorized: report.proposedPaidBatch?.authorized === true,
      approvalTokenRequired: WAVE2_CANONICAL_APPROVAL_TOKEN,
      reportDir: "reports/gdi-bethesda-88-contact-completion-e2e-v1",
    },
    null,
    2
  )
);

if (report.ok !== true) process.exit(1);
