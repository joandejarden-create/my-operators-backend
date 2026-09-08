#!/usr/bin/env node
/**
 * CLI: Full ADP vs Lite Leak Audit comparison (read-only).
 * npm run adp-leak-audit-full-vs-lite-comparison-v1
 */
import { runFullVsLiteComparison } from "../lib/ai-demand-positioning/leak-audit/full-vs-lite-comparison-v1.js";

const result = runFullVsLiteComparison();
console.log(
  JSON.stringify(
    {
      ok: result.ok,
      outDir: result.outDir,
      hotelsCompared: result.hotelsCompared,
      averageScore: result.averageScore,
      portfolioSafeForProspectUse: result.portfolioSafeForProspectUse,
      recommendedDefaultScope: result.recommendedDefaultScope,
      providerFailureRule: result.providerFailureRule,
      fingerprintsUnchanged: result.fingerprintsUnchanged,
      productionAdpWrites: result.productionAdpWrites,
      hotelFiles: result.hotelFiles,
    },
    null,
    2
  )
);
if (!result.ok) process.exitCode = 1;
