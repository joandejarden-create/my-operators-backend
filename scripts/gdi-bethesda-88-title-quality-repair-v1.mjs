#!/usr/bin/env node
/**
 * Bethesda 88 title quality repair.
 *
 *   node scripts/gdi-bethesda-88-title-quality-repair-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-88-title-quality-repair-v1.mjs --apply-historical
 *
 * Canonical opportunities.json is NEVER written. Dry-run patches only.
 */
import {
  runBethesda88TitleQualityRepair,
  BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
} from "../lib/group-demand-intelligence/research/bethesda-88-title-quality-repair-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply-historical");

const result = runBethesda88TitleQualityRepair({ dryRun });
const audit = result.afterAudit || result.historical?.audit || {};

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
      dryRun,
      ok: result.ok,
      union: audit.union,
      total: audit.total,
      defectiveCount: audit.defectiveCount,
      changeCount: audit.changeCount,
      byCause: audit.byCause,
      historicalApplied: result.historical?.applied?.length || 0,
      writeApplied: result.historical?.writeApplied === true,
      canonicalDryRunPatches: result.canonical?.patches?.length || 0,
      credentialContaminationCount: audit.credentialContaminationCount,
      customerFacingDefects: audit.customerFacingDefects,
      store: audit.store,
      publicationExposure: {
        meetingCredentialInShareRegistry:
          result.publicationExposure?.meetingCredentialInShareRegistry,
        remediationRequired: result.publicationExposure?.remediationRequired,
      },
    },
    null,
    2
  )
);

if (!result.ok) process.exitCode = 2;
