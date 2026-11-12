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
  applyWave2CanonicalContactPatches,
  BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
  WAVE2_CANONICAL_APPROVAL_TOKEN,
  WAVE2_FOUNDER_APPROVED_CANONICAL_IDS,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-completion-wave2-v1.js";

const args = process.argv.slice(2);
const applyHistorical = args.includes("--apply-historical") || !args.includes("--dry-run-only");
const applyCanonical = args.includes("--apply-canonical");
const founderAuthorization = args.includes("--founder-authorized");
const tokenIdx = args.indexOf("--approval-token");
const approvalToken = tokenIdx >= 0 ? args[tokenIdx + 1] : null;
const onlyIdsIdx = args.indexOf("--only-ids");
const onlyIdsRaw = onlyIdsIdx >= 0 ? args[onlyIdsIdx + 1] : null;
const onlyIds = onlyIdsRaw
  ? onlyIdsRaw.split(",").map((s) => s.trim()).filter(Boolean)
  : args.includes("--founder-approved-four")
    ? [...WAVE2_FOUNDER_APPROVED_CANONICAL_IDS]
    : null;

// Direct atomic apply path for founder-approved subset (skips full orchestration re-run).
if (applyCanonical && founderAuthorization && onlyIds?.length) {
  const applied = applyWave2CanonicalContactPatches({
    founderAuthorization: true,
    approvalToken,
    dryRun: false,
    onlyIds,
  });
  console.log(
    JSON.stringify(
      {
        version: BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
        mode: "apply-canonical-subset",
        ok: applied.ok === true,
        writeApplied: applied.writeApplied === true,
        appliedCount: applied.appliedCount ?? 0,
        appliedIds: (applied.applied || []).map((a) => a.id),
        deferredIds: applied.deferredIds || [],
        beforeSha256: applied.beforeSha256 || null,
        afterSha256: applied.afterSha256 || null,
        backupPath: applied.backupPath || null,
        code: applied.code || null,
        approvalTokenRequired: WAVE2_CANONICAL_APPROVAL_TOKEN,
      },
      null,
      2
    )
  );
  if (applied.ok !== true) process.exit(1);
  process.exit(0);
}

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
