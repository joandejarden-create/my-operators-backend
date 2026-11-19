#!/usr/bin/env node
/**
 * Offline validation runner for gdi-discovery-intake-disposition-v1.
 * $0 · no Airtable · no customer publication · excludes Phase 2B CMAA/DDAA IDs.
 */
import { runOfflineValidation } from "../lib/group-demand-intelligence/experiments/gdi-discovery-intake-disposition-v1.js";

const report = runOfflineValidation({ writeReports: true });
console.log(
  JSON.stringify(
    {
      ok: report.acceptancePass,
      paidUsd: report.paidUsd,
      accuracy: report.disposition.accuracy,
      competitorPrecision: report.disposition.competitorPlacement.precision,
      competitorRecall: report.disposition.competitorPlacement.recall,
      reportDir: report.reportDir,
      disclaimer: report.disclaimer,
    },
    null,
    2,
  ),
);
process.exitCode = report.acceptancePass ? 0 : 1;
