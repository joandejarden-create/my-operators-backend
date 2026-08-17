#!/usr/bin/env node
/**
 * Census Autopilot V2.1 — Production Readiness + Controlled Scale Wave
 *
 * npm run census:autopilot-v2-1-production-readiness
 *
 * No Airtable writes. No Webhound. No full 14.3k SerpApi spend.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { runCensusAutopilotV21 } from "../lib/research-engine-v2/census-autopilot-v2-1/index.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

const waveSize = Number(process.env.CAV21_WAVE_SIZE || 250);
const searchCeiling = Number(process.env.CAV21_SEARCH_CEILING || 400);

const result = await runCensusAutopilotV21({
  root: ROOT,
  waveSize,
  searchCeiling,
  log: console.log,
});

console.log(
  JSON.stringify(
    {
      outDir: result.outDir,
      wave: result.wave.results.length,
      confirmed: result.strata.overall.independently_confirmed,
      rate: result.strata.overall.rate_pct,
      research: result.scorecard.research_verdict,
      airtable: result.scorecard.airtable_verdict,
      serpapi: result.scorecard.serpapi_verdict,
      revised_forecast: result.audit.revised_forecast.total,
    },
    null,
    2
  )
);
