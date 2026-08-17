#!/usr/bin/env node
/**
 * Census Autopilot V2.2 — Official-first + Rooms breakthrough + real 500 production wave
 *
 * npm run census:autopilot-v2-2-official-first-rooms
 *
 * No Airtable writes. No Webhound. SerpApi research/staging only.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { runCensusAutopilotV22 } from "../lib/research-engine-v2/census-autopilot-v2-2/index.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

const waveSize = Number(process.env.CAV22_WAVE_SIZE || 500);
const searchCeiling = Number(process.env.CAV22_SEARCH_CEILING || 650);

const result = await runCensusAutopilotV22({
  root: ROOT,
  waveSize,
  searchCeiling,
  log: console.log,
});

console.log(
  JSON.stringify(
    {
      outDir: result.outDir,
      wave: result.scorecard.wave_processed,
      confirmed: result.scorecard.independently_confirmed,
      rooms_v3: result.scorecard.rooms_v3_success,
      serpapi_searches: result.scorecard.serpapi_searches,
      forecast: result.scorecard.new_serpapi_forecast,
      research: result.scorecard.research_verdict,
      rooms: result.scorecard.rooms_verdict,
      airtable: result.scorecard.airtable_verdict,
      serpapi: result.scorecard.serpapi_verdict,
    },
    null,
    2
  )
);
