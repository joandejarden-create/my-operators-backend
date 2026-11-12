#!/usr/bin/env node
/**
 * Bethesda 88 Full E2E opportunity quality orchestration.
 *
 *   node scripts/gdi-bethesda-88-full-e2e-opportunity-quality-v1.mjs
 *   node scripts/gdi-bethesda-88-full-e2e-opportunity-quality-v1.mjs --apply-historical
 *
 * Never writes opportunities.json.
 * Never runs paid research.
 * Historical research-stage merges only with --apply-historical.
 */
import {
  runGdiOpportunityFullE2eQualityOrchestration,
  GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1,
} from "../lib/group-demand-intelligence/gdi-opportunity-full-e2e-quality-orchestrator-v1.js";

const args = process.argv.slice(2);
const applyHistorical = args.includes("--apply-historical");
const applyCanonical = args.includes("--apply-canonical");
const founderAuthorization = args.includes("--founder-authorized");

const report = runGdiOpportunityFullE2eQualityOrchestration({
  applyHistorical,
  applyCanonical,
  founderAuthorization,
  paidResearchAuthorized: false,
});

console.log(
  JSON.stringify(
    {
      version: GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1,
      ok: report.ok === true,
      code: report.code || null,
      processedCount: report.execution?.processedCount ?? null,
      allIdsProcessed: report.execution?.allIdsProcessed ?? false,
      uniqueCommercial:
        report.execution?.uniqueCommercialOpportunities ?? null,
      beforeNamed: report.before?.namedRelevantPrimary ?? null,
      afterNamed: report.afterProjected?.namedRelevantPrimary ?? null,
      beforeTitleOk: report.before?.titleOk ?? null,
      afterTitleOk: report.afterProjected?.titleOk ?? null,
      commercialCertification: report.afterProjected?.commercialCertification ?? null,
      researchIntegrity: report.afterProjected?.researchIntegrity ?? null,
      canonicalDryRunPatches:
        report.canonicalDryRunManifest?.patchCount ?? null,
      historicalWriteApplied:
        report.historicalPersist?.writeApplied === true,
      canonicalWriteApplied: report.canonicalPersist?.writeApplied === true,
      paidResearchAuthorized: report.proposedPaidBatch?.authorized === true,
      proposedPaidCeilingUsd: report.proposedPaidBatch?.ceilingUsd ?? null,
      storeSha: report.sourceOfTruth?.canonicalSha256 ?? null,
      reportDir: "reports/gdi-bethesda-88-full-e2e-opportunity-quality-v1",
    },
    null,
    2
  )
);

if (report.ok !== true) process.exit(1);
