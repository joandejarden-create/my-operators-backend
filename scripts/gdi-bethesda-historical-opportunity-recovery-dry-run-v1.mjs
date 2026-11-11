#!/usr/bin/env node
/**
 * Dry-run Bethesda historical opportunity recovery.
 * Does NOT write hotel stores, publish, or call paid providers.
 */
import { runBethesdaHistoricalRecoveryDryRun } from "../lib/group-demand-intelligence/research/bethesda-historical-opportunity-recovery-v1.js";

const { manifest, written } = runBethesdaHistoricalRecoveryDryRun();
const c = manifest.coverage;
console.log(
  JSON.stringify(
    {
      ok: true,
      dryRun: true,
      hotelStoreWrites: false,
      baselineCount: manifest.baseline.opportunityCount,
      historicalAdditionalUnique: c.historicalAdditionalUniqueOpportunities,
      cateringRecovered: c.cateringOpportunitiesRecovered,
      partnershipsRecovered: c.partnershipOpportunitiesRecovered,
      researchOnly: c.researchOnlyContinuationRecords,
      duplicatesExcluded: c.duplicatesExcluded,
      defensibleHistoricalPopulation: c.defensibleHistoricalPopulation,
      missingArtifacts: manifest.missingArtifacts.length,
      reports: written,
    },
    null,
    2
  )
);
