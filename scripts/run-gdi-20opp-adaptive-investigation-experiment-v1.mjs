#!/usr/bin/env node
/**
 * GDI Agent Test 001 — 20-opportunity controlled experiment runner.
 * Default: $0 paid providers, no production writes, offline adaptive planning.
 *
 * Usage:
 *   node scripts/run-gdi-20opp-adaptive-investigation-experiment-v1.mjs
 *   node scripts/run-gdi-20opp-adaptive-investigation-experiment-v1.mjs --authorize-external-retrieval
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import { runFullExperiment } from "../lib/group-demand-intelligence/experiments/gdi-20opp-adaptive-investigation-experiment-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const authorizeExternalRetrieval = process.argv.includes("--authorize-external-retrieval");

const { report, outDir, fixtureDir } = await runFullExperiment({
  root: ROOT,
  authorizeExternalRetrieval,
});

console.log(
  JSON.stringify(
    {
      ok: true,
      experiment: report.schema,
      version: report.version,
      caseCount: report.totals.cases,
      recommendation: report.decision.recommendation,
      avgDelta: report.decision.avgDelta,
      actualPaidUsdTotal: report.totals.actualPaidUsdTotal,
      improvedCount: report.totals.improvedCount,
      worsenedCount: report.totals.worsenedCount,
      outDir: path.relative(ROOT, outDir).replace(/\\/g, "/"),
      fixtureDir: path.relative(ROOT, fixtureDir).replace(/\\/g, "/"),
      gatedLive: report.gatedLiveProtocol.status,
    },
    null,
    2,
  ),
);
