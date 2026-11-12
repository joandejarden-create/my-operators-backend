#!/usr/bin/env node
/**
 * Bethesda 88 full commercial certification + contact research completion.
 *
 *   node scripts/gdi-bethesda-88-commercial-certification-v1.mjs
 *   node scripts/gdi-bethesda-88-commercial-certification-v1.mjs --skip-historical-apply
 *
 * Never writes opportunities.json. Never runs paid research.
 */
import {
  runBethesda88CommercialCertification,
  BETHESDA_88_COMMERCIAL_CERTIFICATION_V1,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-certification-v1.js";

const args = process.argv.slice(2);
const skipHistorical = args.includes("--skip-historical-apply");

const report = runBethesda88CommercialCertification({
  applyHistoricalContacts: !skipHistorical,
});

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_COMMERCIAL_CERTIFICATION_V1,
      ok: true,
      union: report.sourceOfTruth.union,
      storeSha: report.sourceOfTruth.storeSha256,
      storeShaUnchanged: report.sourceOfTruth.storeShaUnchanged,
      scalingCompaniesIntact: report.sourceOfTruth.scalingCompaniesIntact,
      contact: {
        namedBefore: report.contactCoverage.before.namedRelevantPrimary,
        namedAfterHistorical: report.contactCoverage.afterHistoricalApply.namedRelevantPrimary,
        namedAfterProjected:
          report.contactCoverage.afterIncludingCanonicalDryRunProjection.namedRelevantPrimary,
        namedPlusRoute:
          report.contactCoverage.afterIncludingCanonicalDryRunProjection.namedPlusUsableRoute,
        stillMissingNamed:
          report.contactCoverage.afterIncludingCanonicalDryRunProjection.stillMissingNamed,
        recoveredNamed:
          report.contactCoverage.afterIncludingCanonicalDryRunProjection.recoveredNamedCount,
      },
      certification: {
        commerciallyCertified: report.meetingCommercialCertification,
        conditionallyReady: report.conditionallyReady,
        researchIncomplete: report.researchIncomplete,
        disqualified: report.disqualifiedWithEvidence,
      },
      productMaturity: report.productMaturityCounts,
      historicalApply: report.historicalApply,
      canonicalDryRunPatches: report.canonicalDryRun.patchCount,
      proposedPaidAuthorized: report.proposedPaidBatch.authorized,
      reportDir: "reports/gdi-bethesda-88-commercial-certification-v1",
    },
    null,
    2
  )
);
