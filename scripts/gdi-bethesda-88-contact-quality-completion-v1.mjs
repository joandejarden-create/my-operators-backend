#!/usr/bin/env node
/**
 * Bethesda 88 contact quality completion.
 *
 *   node scripts/gdi-bethesda-88-contact-quality-completion-v1.mjs --dry-run
 *   node scripts/gdi-bethesda-88-contact-quality-completion-v1.mjs --apply-historical
 *
 * Canonical opportunities.json is NEVER written. Dry-run patches only.
 */
import {
  runBethesda88ContactQualityCompletion,
  BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
  PROPOSED_PAID_CONTACT_BATCH_V1,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply-historical");

const result = runBethesda88ContactQualityCompletion({ dryRun });
const audit = result.afterAudit || result.historical?.audit || {};

console.log(
  JSON.stringify(
    {
      version: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
      dryRun,
      ok: result.ok,
      union: audit.union,
      distribution: audit.distribution,
      historicalDistribution: audit.historicalDistribution,
      canonicalDistribution: audit.canonicalDistribution,
      namedRelevantPrimary: audit.namedRelevantPrimary,
      directOutreachRoutes: audit.directOutreachRoutes,
      orgPathOnly: audit.orgPathOnly,
      historicalApplied: result.historical?.applied?.length || 0,
      writeApplied: result.historical?.writeApplied === true,
      canonicalDryRunPatches: result.canonical?.patches?.length || 0,
      proposedPaidBatch: {
        authorized: PROPOSED_PAID_CONTACT_BATCH_V1.authorized,
        ceilingUsd: PROPOSED_PAID_CONTACT_BATCH_V1.ceilingUsd,
        opportunityIds: PROPOSED_PAID_CONTACT_BATCH_V1.opportunityIds,
      },
      store: audit.store,
    },
    null,
    2
  )
);

if (!result.ok) process.exitCode = 2;
